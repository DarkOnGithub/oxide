use std::fs;
use std::io::Read;
use std::path::{Component, Path, PathBuf};

use bytes::Bytes;
use jwalk::WalkDir;

use crate::format::FormatDetector;
use crate::types::{Batch, FileFormat, Result};

use super::types::ArchiveSourceKind;

pub(super) const DIRECTORY_BUNDLE_MAGIC: [u8; 4] = *b"OXDB";
pub(super) const DIRECTORY_BUNDLE_VERSION: u16 = 1;
pub(super) const SOURCE_KIND_DIRECTORY_FLAG: u32 = 1 << 0;

/// Represents an entry in a directory bundle.
#[derive(Debug)]
pub(super) enum DirectoryBundleEntry {
    /// A directory entry with its relative path.
    Directory { rel_path: String },
    /// A file entry with its relative path and complete data.
    File { rel_path: String, data: Vec<u8> },
}

/// Metadata for a discovered file in a directory tree.
#[derive(Debug, Clone)]
pub(super) struct DirectoryFileSpec {
    /// Path relative to the archive root.
    pub(super) rel_path: String,
    /// Absolute path on the host filesystem.
    pub(super) full_path: PathBuf,
    /// File size in bytes.
    pub(super) size: u64,
}

/// Result of a directory discovery operation.
#[derive(Debug, Clone)]
pub(super) struct DirectoryDiscovery {
    /// Root directory path.
    pub(super) root: PathBuf,
    /// List of discovered directories (relative paths).
    pub(super) directories: Vec<String>,
    /// List of discovered files.
    pub(super) files: Vec<DirectoryFileSpec>,
    /// Total estimated size of the directory bundle.
    pub(super) input_bytes_total: u64,
}

/// Utility for grouping file data into batches while respecting format boundaries.
#[derive(Debug, Clone)]
pub(super) struct DirectoryBatchSubmitter {
    source_path: PathBuf,
    block_size: usize,
    preserve_format_boundaries: bool,
    next_block_id: usize,
    pending: Vec<u8>,
    pending_format: Option<FileFormat>,
}

impl DirectoryBatchSubmitter {
    pub(super) fn new(
        source_path: PathBuf,
        block_size: usize,
        preserve_format_boundaries: bool,
    ) -> Self {
        Self {
            source_path,
            block_size: block_size.max(1),
            preserve_format_boundaries,
            next_block_id: 0,
            pending: Vec::with_capacity(block_size.max(1)),
            pending_format: None,
        }
    }

    pub(super) fn push_bytes_with_hint<F>(
        &mut self,
        mut bytes: &[u8],
        file_type_hint: FileFormat,
        mut submit: F,
    ) -> Result<()>
    where
        F: FnMut(Batch) -> Result<()>,
    {
        while !bytes.is_empty() {
            match self.pending_format {
                Some(current) if current != file_type_hint => {
                    if self.preserve_format_boundaries {
                        if !self.pending.is_empty() {
                            self.flush_pending(&mut submit)?;
                        }
                        self.pending_format = Some(file_type_hint);
                    } else {
                        self.pending_format = Some(FileFormat::Common);
                    }
                }
                Some(_) => {}
                None => {
                    self.pending_format = Some(file_type_hint);
                }
            }

            let room = self.block_size.saturating_sub(self.pending.len()).max(1);
            let take = room.min(bytes.len());
            self.pending.extend_from_slice(&bytes[..take]);
            bytes = &bytes[take..];

            if self.pending.len() == self.block_size {
                self.flush_pending(&mut submit)?;
            }
        }

        Ok(())
    }

    pub(super) fn finish<F>(&mut self, submit: F) -> Result<()>
    where
        F: FnMut(Batch) -> Result<()>,
    {
        if !self.pending.is_empty() {
            self.flush_pending(submit)?;
        }
        Ok(())
    }

    fn flush_pending<F>(&mut self, mut submit: F) -> Result<()>
    where
        F: FnMut(Batch) -> Result<()>,
    {
        let file_type_hint = self.pending_format.unwrap_or(FileFormat::Common);
        let chunk = std::mem::replace(&mut self.pending, Vec::with_capacity(self.block_size));
        let batch = Batch::with_hint(
            self.next_block_id,
            self.source_path.clone(),
            Bytes::from(chunk),
            file_type_hint,
        );
        submit(batch)?;
        self.next_block_id += 1;
        self.pending_format = None;
        Ok(())
    }
}

pub(super) fn decode_directory_entries(
    payload: &[u8],
    flags: u32,
) -> Result<Option<Vec<DirectoryBundleEntry>>> {
    if flags & SOURCE_KIND_DIRECTORY_FLAG != 0 {
        return decode_directory_bundle(payload).map(Some);
    }

    match decode_directory_bundle(payload) {
        Ok(entries) => Ok(Some(entries)),
        Err(_) => Ok(None),
    }
}

pub(super) fn write_directory_entries(
    root: &Path,
    entries: Vec<DirectoryBundleEntry>,
) -> Result<()> {
    fs::create_dir_all(root)?;

    for entry in entries {
        match entry {
            DirectoryBundleEntry::Directory { rel_path } => {
                let out_path = join_safe(root, &rel_path)?;
                fs::create_dir_all(out_path)?;
            }
            DirectoryBundleEntry::File { rel_path, data } => {
                let out_path = join_safe(root, &rel_path)?;
                if let Some(parent) = out_path.parent() {
                    fs::create_dir_all(parent)?;
                }
                fs::write(out_path, data)?;
            }
        }
    }

    Ok(())
}

pub(super) fn source_kind_flags(source_kind: ArchiveSourceKind) -> u32 {
    match source_kind {
        ArchiveSourceKind::File => 0,
        ArchiveSourceKind::Directory => SOURCE_KIND_DIRECTORY_FLAG,
    }
}

pub(super) fn source_kind_from_flags(flags: u32) -> ArchiveSourceKind {
    if flags & SOURCE_KIND_DIRECTORY_FLAG != 0 {
        ArchiveSourceKind::Directory
    } else {
        ArchiveSourceKind::File
    }
}

pub(super) fn discover_directory_tree(root: &Path) -> Result<DirectoryDiscovery> {
    if !root.is_dir() {
        return Err(crate::OxideError::InvalidFormat(
            "archive_directory expects a directory path",
        ));
    }

    let mut directory_paths = Vec::<PathBuf>::new();
    let mut file_paths = Vec::<PathBuf>::new();

    for entry in WalkDir::new(root) {
        let entry = entry.map_err(anyhow::Error::from)?;
        let path = entry.path();
        if path == root {
            continue;
        }

        let rel_path = path
            .strip_prefix(root)
            .map_err(|_| crate::OxideError::InvalidFormat("invalid relative path"))?
            .to_path_buf();

        if entry.file_type().is_dir() {
            directory_paths.push(rel_path);
        } else if entry.file_type().is_file() {
            file_paths.push(rel_path);
        } else {
            return Err(crate::OxideError::InvalidFormat(
                "directory archive supports regular files/directories only",
            ));
        }
    }

    directory_paths.sort();
    file_paths.sort();

    let mut input_bytes_total = 10u64;
    let mut directories = Vec::with_capacity(directory_paths.len());
    for directory_rel in directory_paths {
        let rel_path = relative_path_to_utf8(&directory_rel)?;
        input_bytes_total = accumulate_bundle_size(input_bytes_total, rel_path.len())?;
        directories.push(rel_path);
    }

    let mut files = Vec::with_capacity(file_paths.len());
    for file_rel in file_paths {
        let rel_path = relative_path_to_utf8(&file_rel)?;
        let full_path = root.join(&file_rel);
        let size = fs::metadata(&full_path)?.len();
        input_bytes_total = accumulate_bundle_size(input_bytes_total, rel_path.len())?;
        input_bytes_total = input_bytes_total
            .checked_add(8)
            .and_then(|total| total.checked_add(size))
            .ok_or(crate::OxideError::InvalidFormat(
                "directory bundle size overflow",
            ))?;
        files.push(DirectoryFileSpec {
            rel_path,
            full_path,
            size,
        });
    }

    Ok(DirectoryDiscovery {
        root: root.to_path_buf(),
        directories,
        files,
        input_bytes_total,
    })
}

fn accumulate_bundle_size(current: u64, path_len: usize) -> Result<u64> {
    current
        .checked_add(1)
        .and_then(|value| value.checked_add(4))
        .and_then(|value| value.checked_add(path_len as u64))
        .ok_or(crate::OxideError::InvalidFormat(
            "directory bundle size overflow",
        ))
}

pub(super) fn detect_file_formats(
    discovery: &DirectoryDiscovery,
    format_probe_limit: usize,
) -> Result<Vec<FileFormat>> {
    let probe_limit = format_probe_limit.max(1);
    let mut probe = vec![0u8; probe_limit];
    let mut formats = Vec::with_capacity(discovery.files.len());

    for file in &discovery.files {
        let mut reader = fs::File::open(&file.full_path)?;
        let read = reader.read(&mut probe)?;
        formats.push(FormatDetector::detect(&probe[..read]));
    }

    Ok(formats)
}

pub(super) fn estimate_directory_block_count(
    discovery: &DirectoryDiscovery,
    file_formats: &[FileFormat],
    block_size: usize,
    preserve_format_boundaries: bool,
) -> Result<u32> {
    if discovery.files.len() != file_formats.len() {
        return Err(crate::OxideError::InvalidFormat(
            "directory file format plan mismatch",
        ));
    }

    let mut planner = BlockCountPlanner::new(block_size, preserve_format_boundaries);

    // OXDB magic + version + entry_count.
    planner.push_len(10, FileFormat::Common);

    for rel_path in &discovery.directories {
        planner.push_len(
            1usize
                .checked_add(4)
                .and_then(|size| size.checked_add(rel_path.len()))
                .ok_or(crate::OxideError::InvalidFormat(
                    "directory path size overflow",
                ))?,
            FileFormat::Common,
        );
    }

    for (file, file_format) in discovery.files.iter().zip(file_formats.iter().copied()) {
        planner.push_len(
            1usize
                .checked_add(4)
                .and_then(|size| size.checked_add(file.rel_path.len()))
                .and_then(|size| size.checked_add(8))
                .ok_or(crate::OxideError::InvalidFormat(
                    "directory file header overflow",
                ))?,
            FileFormat::Common,
        );

        let file_size = usize::try_from(file.size)
            .map_err(|_| crate::OxideError::InvalidFormat("file size exceeds usize range"))?;
        planner.push_len(file_size, file_format);
    }

    u32::try_from(planner.finish())
        .map_err(|_| crate::OxideError::InvalidFormat("too many blocks for OXZ v1"))
}

#[derive(Debug, Clone)]
struct BlockCountPlanner {
    block_size: usize,
    preserve_format_boundaries: bool,
    blocks: usize,
    pending_len: usize,
    pending_format: Option<FileFormat>,
}

impl BlockCountPlanner {
    fn new(block_size: usize, preserve_format_boundaries: bool) -> Self {
        Self {
            block_size: block_size.max(1),
            preserve_format_boundaries,
            blocks: 0,
            pending_len: 0,
            pending_format: None,
        }
    }

    fn push_len(&mut self, mut len: usize, file_type_hint: FileFormat) {
        while len > 0 {
            match self.pending_format {
                Some(current) if current != file_type_hint => {
                    if self.preserve_format_boundaries {
                        self.flush_pending();
                        self.pending_format = Some(file_type_hint);
                    } else {
                        self.pending_format = Some(FileFormat::Common);
                    }
                }
                Some(_) => {}
                None => {
                    self.pending_format = Some(file_type_hint);
                }
            }

            let room = self.block_size.saturating_sub(self.pending_len).max(1);
            let take = room.min(len);
            self.pending_len += take;
            len -= take;

            if self.pending_len == self.block_size {
                self.flush_pending();
            }
        }
    }

    fn finish(mut self) -> usize {
        self.flush_pending();
        self.blocks
    }

    fn flush_pending(&mut self) {
        if self.pending_len > 0 {
            self.blocks += 1;
            self.pending_len = 0;
            self.pending_format = None;
        }
    }
}

fn relative_path_to_utf8(path: &Path) -> Result<String> {
    let raw = path.to_str().ok_or(crate::OxideError::InvalidFormat(
        "non-utf8 path not supported",
    ))?;
    Ok(raw.replace('\\', "/"))
}

pub(super) fn encode_path(out: &mut Vec<u8>, rel_path: &str) -> Result<()> {
    let bytes = rel_path.as_bytes();
    let len = u32::try_from(bytes.len())
        .map_err(|_| crate::OxideError::InvalidFormat("path length overflow"))?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(bytes);
    Ok(())
}

fn decode_directory_bundle(payload: &[u8]) -> Result<Vec<DirectoryBundleEntry>> {
    if payload.len() < 10 {
        return Err(crate::OxideError::InvalidFormat(
            "directory bundle is too short",
        ));
    }

    if payload[..4] != DIRECTORY_BUNDLE_MAGIC {
        return Err(crate::OxideError::InvalidFormat(
            "archive payload is not a directory bundle",
        ));
    }

    let version = u16::from_le_bytes([payload[4], payload[5]]);
    if version != DIRECTORY_BUNDLE_VERSION {
        return Err(crate::OxideError::InvalidFormat(
            "unsupported directory bundle version",
        ));
    }

    let entry_count = u32::from_le_bytes([payload[6], payload[7], payload[8], payload[9]]);
    let mut cursor = 10usize;
    let mut entries = Vec::with_capacity(entry_count as usize);

    for _ in 0..entry_count {
        if cursor >= payload.len() {
            return Err(crate::OxideError::InvalidFormat(
                "truncated directory bundle entry kind",
            ));
        }
        let kind = payload[cursor];
        cursor += 1;

        let rel_path = decode_path(payload, &mut cursor)?;
        match kind {
            0 => entries.push(DirectoryBundleEntry::Directory { rel_path }),
            1 => {
                if cursor + 8 > payload.len() {
                    return Err(crate::OxideError::InvalidFormat(
                        "truncated directory bundle file size",
                    ));
                }
                let size = u64::from_le_bytes([
                    payload[cursor],
                    payload[cursor + 1],
                    payload[cursor + 2],
                    payload[cursor + 3],
                    payload[cursor + 4],
                    payload[cursor + 5],
                    payload[cursor + 6],
                    payload[cursor + 7],
                ]);
                cursor += 8;

                let size_usize = usize::try_from(size)
                    .map_err(|_| crate::OxideError::InvalidFormat("file size overflow"))?;
                let end =
                    cursor
                        .checked_add(size_usize)
                        .ok_or(crate::OxideError::InvalidFormat(
                            "directory bundle offset overflow",
                        ))?;
                if end > payload.len() {
                    return Err(crate::OxideError::InvalidFormat(
                        "truncated directory bundle file data",
                    ));
                }

                let data = payload[cursor..end].to_vec();
                cursor = end;
                entries.push(DirectoryBundleEntry::File { rel_path, data });
            }
            _ => {
                return Err(crate::OxideError::InvalidFormat(
                    "invalid directory bundle entry kind",
                ));
            }
        }
    }

    if cursor != payload.len() {
        return Err(crate::OxideError::InvalidFormat(
            "directory bundle has trailing data",
        ));
    }

    Ok(entries)
}

fn decode_path(payload: &[u8], cursor: &mut usize) -> Result<String> {
    if *cursor + 4 > payload.len() {
        return Err(crate::OxideError::InvalidFormat(
            "truncated directory bundle path length",
        ));
    }
    let len = u32::from_le_bytes([
        payload[*cursor],
        payload[*cursor + 1],
        payload[*cursor + 2],
        payload[*cursor + 3],
    ]) as usize;
    *cursor += 4;

    let end = cursor
        .checked_add(len)
        .ok_or(crate::OxideError::InvalidFormat(
            "directory path offset overflow",
        ))?;
    if end > payload.len() {
        return Err(crate::OxideError::InvalidFormat(
            "truncated directory bundle path data",
        ));
    }

    let rel_path = std::str::from_utf8(&payload[*cursor..end])
        .map_err(|_| crate::OxideError::InvalidFormat("directory path is not utf8"))?
        .to_string();
    *cursor = end;
    Ok(rel_path)
}

fn join_safe(root: &Path, rel_path: &str) -> Result<PathBuf> {
    let rel = Path::new(rel_path);
    if rel.is_absolute() {
        return Err(crate::OxideError::InvalidFormat(
            "absolute paths are not allowed in directory bundle",
        ));
    }

    for component in rel.components() {
        match component {
            Component::Normal(_) => {}
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(crate::OxideError::InvalidFormat(
                    "unsafe path component in directory bundle",
                ));
            }
        }
    }

    Ok(root.join(rel))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn submitter_flushes_on_format_change_when_preserve_boundaries_enabled() {
        let mut submitter = DirectoryBatchSubmitter::new(PathBuf::from("root"), 8, true);
        let mut batches = Vec::new();

        submitter
            .push_bytes_with_hint(b"aaaaaa", FileFormat::Text, |batch| {
                batches.push(batch);
                Ok(())
            })
            .expect("text push should succeed");
        submitter
            .push_bytes_with_hint(b"bbbbbb", FileFormat::Binary, |batch| {
                batches.push(batch);
                Ok(())
            })
            .expect("binary push should succeed");
        submitter
            .finish(|batch| {
                batches.push(batch);
                Ok(())
            })
            .expect("finish should succeed");

        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].file_type_hint, FileFormat::Text);
        assert_eq!(batches[0].len(), 6);
        assert_eq!(batches[1].file_type_hint, FileFormat::Binary);
        assert_eq!(batches[1].len(), 6);
    }

    #[test]
    fn submitter_merges_formats_when_preserve_boundaries_disabled() {
        let mut submitter = DirectoryBatchSubmitter::new(PathBuf::from("root"), 8, false);
        let mut batches = Vec::new();

        submitter
            .push_bytes_with_hint(b"aaaaaa", FileFormat::Text, |batch| {
                batches.push(batch);
                Ok(())
            })
            .expect("text push should succeed");
        submitter
            .push_bytes_with_hint(b"bbbbbb", FileFormat::Binary, |batch| {
                batches.push(batch);
                Ok(())
            })
            .expect("binary push should succeed");
        submitter
            .finish(|batch| {
                batches.push(batch);
                Ok(())
            })
            .expect("finish should succeed");

        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].file_type_hint, FileFormat::Common);
        assert_eq!(batches[0].len(), 8);
        assert_eq!(batches[1].file_type_hint, FileFormat::Binary);
        assert_eq!(batches[1].len(), 4);
    }

    #[test]
    fn block_count_planner_respects_boundary_toggle() {
        let mut preserving = BlockCountPlanner::new(8, true);
        preserving.push_len(4, FileFormat::Text);
        preserving.push_len(4, FileFormat::Binary);
        preserving.push_len(4, FileFormat::Text);
        assert_eq!(preserving.finish(), 3);

        let mut merging = BlockCountPlanner::new(8, false);
        merging.push_len(4, FileFormat::Text);
        merging.push_len(4, FileFormat::Binary);
        merging.push_len(4, FileFormat::Text);
        assert_eq!(merging.finish(), 2);
    }
}
