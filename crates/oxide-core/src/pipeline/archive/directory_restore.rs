use std::fs;
use std::io::{BufWriter, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::anyhow;
use regex::RegexSet;

use crate::format::ArchiveManifest;
use crate::types::Result;
use crate::OxideError;

use super::super::directory;
use super::reorder_writer::OrderedChunkWriter;

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct DirectoryRestoreStats {
    pub(crate) directory_decode: Duration,
    pub(crate) output_write: Duration,
    pub(crate) output_bytes_total: u64,
    pub(crate) entry_count: u64,
}

#[derive(Debug)]
pub(crate) struct DirectoryExtractSelection {
    manifest: ArchiveManifest,
    selected_ranges: Vec<Range<u64>>,
}

impl DirectoryExtractSelection {
    pub(crate) fn from_filters<S: AsRef<str>, T: AsRef<str>>(
        manifest: &ArchiveManifest,
        filters: &[S],
        regex_filters: &[T],
    ) -> Result<Self> {
        let normalized_filters = normalize_filters(filters)?;
        let regex_set = compile_regex_filters(regex_filters)?;
        if normalized_filters.is_empty() && regex_set.is_none() {
            return Err(anyhow!("path filters cannot be empty").into());
        }

        let mut entries = Vec::new();
        let mut selected_ranges = Vec::new();
        let mut payload_offset = 0u64;

        for entry in manifest.entries() {
            let selected = normalized_filters
                .iter()
                .any(|filter| path_matches_filter(&entry.path, filter))
                || regex_set
                    .as_ref()
                    .is_some_and(|patterns| patterns.is_match(&entry.path));

            if selected {
                entries.push(entry.clone());
                if matches!(entry.kind, crate::ArchiveEntryKind::File) && entry.size > 0 {
                    selected_ranges.push(payload_offset..payload_offset.saturating_add(entry.size));
                }
            }

            if matches!(entry.kind, crate::ArchiveEntryKind::File) {
                payload_offset = payload_offset.saturating_add(entry.size);
            }
        }

        if entries.is_empty() {
            return Err(anyhow!("path filters did not match any archive entries").into());
        }

        Ok(Self {
            manifest: ArchiveManifest::new(entries),
            selected_ranges,
        })
    }

    fn into_parts(self) -> (ArchiveManifest, Vec<Range<u64>>) {
        (self.manifest, self.selected_ranges)
    }

    pub(crate) fn selected_ranges(&self) -> &[Range<u64>] {
        &self.selected_ranges
    }
}

#[derive(Debug)]
struct PendingFile {
    remaining: u64,
    writer: BufWriter<fs::File>,
}

#[derive(Debug)]
pub(crate) struct DirectoryRestoreWriter {
    root: PathBuf,
    entries: Vec<crate::ArchiveListingEntry>,
    next_entry: usize,
    pending_file: Option<PendingFile>,
    stats: DirectoryRestoreStats,
}

#[derive(Debug)]
pub(crate) struct FilteredDirectoryRestoreWriter {
    inner: DirectoryRestoreWriter,
    selected_ranges: Vec<Range<u64>>,
    next_range: usize,
    input_offset: u64,
}

impl DirectoryRestoreWriter {
    pub(crate) fn create(root: &Path, manifest: ArchiveManifest) -> Result<Self> {
        fs::create_dir_all(root)?;
        Ok(Self {
            root: root.to_path_buf(),
            entries: manifest.entries().to_vec(),
            next_entry: 0,
            pending_file: None,
            stats: DirectoryRestoreStats::default(),
        })
    }

    pub(crate) fn finish(&mut self) -> Result<DirectoryRestoreStats> {
        self.advance_entries()?;
        if let Some(pending) = self.pending_file.as_ref() {
            if pending.remaining > 0 {
                return Err(crate::OxideError::InvalidFormat(
                    "truncated file payload during directory restore",
                ));
            }
        }
        if self.pending_file.is_some() || self.next_entry != self.entries.len() {
            return Err(crate::OxideError::InvalidFormat(
                "directory restore ended before all entries completed",
            ));
        }
        Ok(self.stats)
    }

    fn advance_entries(&mut self) -> Result<()> {
        while self.pending_file.is_none() && self.next_entry < self.entries.len() {
            let entry = self.entries[self.next_entry].clone();
            self.next_entry += 1;
            self.stats.entry_count = self.stats.entry_count.saturating_add(1);

            match entry.kind {
                crate::ArchiveEntryKind::Directory => {
                    let output_started = Instant::now();
                    let out_path = directory::join_safe(&self.root, &entry.path)?;
                    fs::create_dir_all(out_path)?;
                    self.stats.output_write += output_started.elapsed();
                }
                crate::ArchiveEntryKind::File => {
                    let output_started = Instant::now();
                    let out_path = directory::join_safe(&self.root, &entry.path)?;
                    if let Some(parent) = out_path.parent() {
                        fs::create_dir_all(parent)?;
                    }
                    let file = fs::File::create(out_path)?;
                    self.stats.output_write += output_started.elapsed();

                    if entry.size == 0 {
                        continue;
                    }

                    self.pending_file = Some(PendingFile {
                        remaining: entry.size,
                        writer: BufWriter::new(file),
                    });
                }
            }
        }

        Ok(())
    }
}

impl FilteredDirectoryRestoreWriter {
    pub(crate) fn create(root: &Path, selection: DirectoryExtractSelection) -> Result<Self> {
        let (manifest, selected_ranges) = selection.into_parts();
        Ok(Self {
            inner: DirectoryRestoreWriter::create(root, manifest)?,
            selected_ranges,
            next_range: 0,
            input_offset: 0,
        })
    }

    pub(crate) fn finish(&mut self) -> Result<DirectoryRestoreStats> {
        self.inner.finish()
    }
}

impl OrderedChunkWriter for DirectoryRestoreWriter {
    fn write_chunk(&mut self, mut bytes: &[u8]) -> Result<()> {
        let decode_started = Instant::now();
        self.advance_entries()?;
        self.stats.directory_decode += decode_started.elapsed();

        while !bytes.is_empty() {
            let finished_file = {
                let pending = self.pending_file.as_mut().ok_or(OxideError::InvalidFormat(
                    "directory archive contains file data beyond manifest entries",
                ))?;

                let take = bytes.len().min(pending.remaining as usize);
                let output_started = Instant::now();
                pending.writer.write_all(&bytes[..take])?;
                self.stats.output_write += output_started.elapsed();
                self.stats.output_bytes_total =
                    self.stats.output_bytes_total.saturating_add(take as u64);
                pending.remaining -= take as u64;
                bytes = &bytes[take..];
                pending.remaining == 0
            };

            if finished_file {
                let output_started = Instant::now();
                if let Some(pending) = self.pending_file.as_mut() {
                    pending.writer.flush()?;
                }
                self.stats.output_write += output_started.elapsed();
                self.pending_file = None;
                let decode_started = Instant::now();
                self.advance_entries()?;
                self.stats.directory_decode += decode_started.elapsed();
            }
        }

        Ok(())
    }
}

impl OrderedChunkWriter for FilteredDirectoryRestoreWriter {
    fn write_chunk(&mut self, bytes: &[u8]) -> Result<()> {
        let chunk_start = self.input_offset;
        let chunk_end = chunk_start.saturating_add(bytes.len() as u64);

        while self.next_range < self.selected_ranges.len()
            && self.selected_ranges[self.next_range].end <= chunk_start
        {
            self.next_range += 1;
        }

        let mut range_index = self.next_range;
        while range_index < self.selected_ranges.len() {
            let range = &self.selected_ranges[range_index];
            if range.start >= chunk_end {
                break;
            }

            let write_start = range.start.max(chunk_start) - chunk_start;
            let write_end = range.end.min(chunk_end) - chunk_start;
            if write_start < write_end {
                self.inner
                    .write_chunk(&bytes[write_start as usize..write_end as usize])?;
            }

            if range.end <= chunk_end {
                range_index += 1;
            } else {
                break;
            }
        }

        self.next_range = range_index;
        self.input_offset = chunk_end;
        Ok(())
    }
}

fn normalize_filters<S: AsRef<str>>(filters: &[S]) -> Result<Vec<String>> {
    filters
        .iter()
        .map(|filter| normalize_filter(filter.as_ref()))
        .collect()
}

fn compile_regex_filters<S: AsRef<str>>(filters: &[S]) -> Result<Option<RegexSet>> {
    if filters.is_empty() {
        return Ok(None);
    }

    let patterns = filters
        .iter()
        .map(|filter| filter.as_ref().trim())
        .collect::<Vec<_>>();
    if patterns.iter().any(|pattern| pattern.is_empty()) {
        return Err(anyhow!("regex filters must be non-empty").into());
    }

    let set = RegexSet::new(&patterns)
        .map_err(|error| crate::OxideError::Other(anyhow!("invalid regex filter: {error}")))?;
    Ok(Some(set))
}

fn normalize_filter(raw: &str) -> Result<String> {
    let replaced = raw.trim().replace('\\', "/");
    if replaced.is_empty() {
        return Err(anyhow!("path filters must be non-empty relative paths").into());
    }

    let mut parts = Vec::new();
    for part in replaced.split('/') {
        match part {
            "" | "." => continue,
            ".." => {
                return Err(
                    anyhow!("path filters must not contain parent directory traversal").into(),
                );
            }
            _ => parts.push(part),
        }
    }

    if parts.is_empty() || raw.trim_start().starts_with('/') {
        return Err(anyhow!("path filters must be non-empty relative paths").into());
    }

    Ok(parts.join("/"))
}

fn path_matches_filter(path: &str, filter: &str) -> bool {
    path == filter
        || path
            .strip_prefix(filter)
            .is_some_and(|suffix| suffix.starts_with('/'))
}
