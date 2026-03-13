use std::fs;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use crate::OxideError;
use crate::format::ArchiveManifest;
use crate::types::Result;

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
