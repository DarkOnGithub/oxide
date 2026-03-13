use std::fs;
use std::io::Read;
use std::path::{Component, Path, PathBuf};
use std::thread;

use bytes::Bytes;
use jwalk::WalkDir;

use crate::format::FormatDetector;
use crate::types::{Batch, FileFormat, Result};

use super::types::{ArchiveEntryKind, ArchiveListingEntry, ArchiveSourceKind};

pub(super) const SOURCE_KIND_DIRECTORY_FLAG: u32 = 1 << 0;

/// Metadata for a discovered file in a directory tree.
#[derive(Debug, Clone)]
pub(super) struct DirectoryFileSpec {
    pub(super) rel_path: String,
    pub(super) full_path: PathBuf,
    pub(super) size: u64,
}

/// Result of a directory discovery operation.
#[derive(Debug, Clone)]
pub(super) struct DirectoryDiscovery {
    pub(super) root: PathBuf,
    pub(super) directories: Vec<String>,
    pub(super) files: Vec<DirectoryFileSpec>,
    pub(super) input_bytes_total: u64,
}

/// Utility for grouping file data into batches while respecting format boundaries.
#[derive(Debug, Clone)]
pub struct DirectoryBatchSubmitter {
    source_path: PathBuf,
    block_size: usize,
    preserve_format_boundaries: bool,
    next_block_id: usize,
    pending: Vec<u8>,
    pending_format: Option<FileFormat>,
}

impl DirectoryBatchSubmitter {
    pub fn new(source_path: PathBuf, block_size: usize, preserve_format_boundaries: bool) -> Self {
        Self {
            source_path,
            block_size: block_size.max(1),
            preserve_format_boundaries,
            next_block_id: 0,
            pending: Vec::with_capacity(block_size.max(1)),
            pending_format: None,
        }
    }

    pub fn push_bytes_with_hint<F>(
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

    pub fn finish<F>(&mut self, submit: F) -> Result<()>
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
    let mut files = Vec::<DirectoryFileSpec>::new();
    let mut input_bytes_total = 0u64;

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
            let full_path = path;
            let rel_path = relative_path_to_utf8(&rel_path)?;
            let size = fs::metadata(&full_path)?.len();

            input_bytes_total =
                input_bytes_total
                    .checked_add(size)
                    .ok_or(crate::OxideError::InvalidFormat(
                        "directory input size overflow",
                    ))?;
            files.push(DirectoryFileSpec {
                rel_path,
                full_path,
                size,
            });
        } else {
            return Err(crate::OxideError::InvalidFormat(
                "directory archive supports regular files/directories only",
            ));
        }
    }

    directory_paths.sort();
    files.sort_by(|left, right| left.rel_path.cmp(&right.rel_path));

    let mut directories = Vec::with_capacity(directory_paths.len());
    for directory_rel in directory_paths {
        directories.push(relative_path_to_utf8(&directory_rel)?);
    }

    Ok(DirectoryDiscovery {
        root: root.to_path_buf(),
        directories,
        files,
        input_bytes_total,
    })
}

pub(super) fn manifest_from_discovery(
    discovery: &DirectoryDiscovery,
) -> crate::format::ArchiveManifest {
    let mut entries = Vec::with_capacity(discovery.directories.len() + discovery.files.len());
    entries.extend(
        discovery
            .directories
            .iter()
            .cloned()
            .map(|path| ArchiveListingEntry {
                path,
                kind: ArchiveEntryKind::Directory,
                size: 0,
            }),
    );
    entries.extend(discovery.files.iter().map(|file| ArchiveListingEntry {
        path: file.rel_path.clone(),
        kind: ArchiveEntryKind::File,
        size: file.size,
    }));
    crate::format::ArchiveManifest::new(entries)
}

pub(super) fn detect_file_formats(
    discovery: &DirectoryDiscovery,
    format_probe_limit: usize,
    threads: usize,
) -> Result<Vec<FileFormat>> {
    let probe_limit = format_probe_limit.max(1);
    let mut formats = vec![FileFormat::Common; discovery.files.len()];
    let worker_count = threads.max(1).min(discovery.files.len().max(1));

    if worker_count == 1 {
        let mut probe = vec![0u8; probe_limit];
        for (file, format) in discovery.files.iter().zip(formats.iter_mut()) {
            let mut reader = fs::File::open(&file.full_path)?;
            let read = reader.read(&mut probe)?;
            *format = FormatDetector::detect(&probe[..read]);
        }
        return Ok(formats);
    }

    let chunk_size = discovery.files.len().div_ceil(worker_count).max(1);
    thread::scope(|scope| -> Result<()> {
        let mut handles = Vec::new();
        for (files_chunk, formats_chunk) in discovery
            .files
            .chunks(chunk_size)
            .zip(formats.chunks_mut(chunk_size))
        {
            handles.push(scope.spawn(move || -> Result<()> {
                let mut probe = vec![0u8; probe_limit];
                for (file, format) in files_chunk.iter().zip(formats_chunk.iter_mut()) {
                    let mut reader = fs::File::open(&file.full_path)?;
                    let read = reader.read(&mut probe)?;
                    *format = FormatDetector::detect(&probe[..read]);
                }
                Ok(())
            }));
        }

        for handle in handles {
            match handle.join() {
                Ok(outcome) => outcome?,
                Err(payload) => {
                    let details = if let Some(message) = payload.downcast_ref::<&str>() {
                        (*message).to_string()
                    } else if let Some(message) = payload.downcast_ref::<String>() {
                        message.clone()
                    } else {
                        "unknown panic payload".to_string()
                    };
                    return Err(crate::OxideError::CompressionError(format!(
                        "directory format probe thread panicked: {details}"
                    )));
                }
            }
        }

        Ok(())
    })?;

    Ok(formats)
}

pub(super) fn estimate_directory_block_count(
    discovery: &DirectoryDiscovery,
    file_formats: &[FileFormat],
    block_size: usize,
    preserve_format_boundaries: bool,
) -> Result<u32> {
    if !preserve_format_boundaries {
        let block_size = block_size.max(1) as u64;
        let block_count = discovery.input_bytes_total.div_ceil(block_size);
        return u32::try_from(block_count)
            .map_err(|_| crate::OxideError::InvalidFormat("too many blocks for OXZ v2"));
    }

    if discovery.files.len() != file_formats.len() {
        return Err(crate::OxideError::InvalidFormat(
            "directory file format plan mismatch",
        ));
    }

    let mut planner = BlockCountPlanner::new(block_size, preserve_format_boundaries);
    for (file, file_format) in discovery.files.iter().zip(file_formats.iter().copied()) {
        let file_size = usize::try_from(file.size)
            .map_err(|_| crate::OxideError::InvalidFormat("file size exceeds usize range"))?;
        planner.push_len(file_size, file_format);
    }

    u32::try_from(planner.finish())
        .map_err(|_| crate::OxideError::InvalidFormat("too many blocks for OXZ v2"))
}

#[cfg(test)]
mod tests {
    use super::{detect_file_formats, discover_directory_tree, estimate_directory_block_count};
    use crate::types::FileFormat;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn discovery_collects_sizes_in_one_pass() {
        let temp = tempdir().expect("tempdir");
        let root = temp.path();
        let nested = root.join("nested");
        fs::create_dir(&nested).expect("create nested dir");
        fs::write(root.join("notes.txt"), b"hello\nworld\n").expect("write text");
        fs::write(nested.join("payload.bin"), [0, 159, 146, 150]).expect("write binary");

        let discovery = discover_directory_tree(root).expect("discover directory");

        assert_eq!(discovery.directories, vec!["nested"]);
        assert_eq!(discovery.files.len(), 2);
        assert_eq!(discovery.files[0].rel_path, "nested/payload.bin");
        assert_eq!(discovery.files[1].rel_path, "notes.txt");
        assert_eq!(discovery.input_bytes_total, 16);
    }

    #[test]
    fn format_probe_detects_expected_formats() {
        let temp = tempdir().expect("tempdir");
        let root = temp.path();
        let nested = root.join("nested");
        fs::create_dir(&nested).expect("create nested dir");
        fs::write(root.join("notes.txt"), b"hello\nworld\n").expect("write text");
        fs::write(nested.join("payload.bin"), [0, 159, 146, 150]).expect("write binary");

        let discovery = discover_directory_tree(root).expect("discover directory");
        let formats = detect_file_formats(&discovery, 64 * 1024, 2).expect("probe formats");

        assert_eq!(formats, vec![FileFormat::Common, FileFormat::Text]);
    }

    #[test]
    fn block_count_uses_formats_embedded_in_discovery() {
        let temp = tempdir().expect("tempdir");
        let root = temp.path();
        fs::write(root.join("alpha.txt"), b"alpha\n").expect("write alpha");
        fs::write(root.join("beta.bin"), [0, 159, 146, 150, 42]).expect("write beta");

        let discovery = discover_directory_tree(root).expect("discover directory");
        let formats = detect_file_formats(&discovery, 64 * 1024, 2).expect("probe formats");
        let block_count =
            estimate_directory_block_count(&discovery, &formats, 4, true).expect("plan");

        assert_eq!(block_count, 4);
    }

    #[test]
    fn block_count_without_boundaries_uses_total_bytes_only() {
        let temp = tempdir().expect("tempdir");
        let root = temp.path();
        fs::write(root.join("alpha.txt"), b"alpha\n").expect("write alpha");
        fs::write(root.join("beta.bin"), [0, 159, 146, 150, 42]).expect("write beta");

        let discovery = discover_directory_tree(root).expect("discover directory");
        let block_count = estimate_directory_block_count(&discovery, &[], 4, false).expect("plan");

        assert_eq!(block_count, 3);
    }
}

#[derive(Debug, Clone)]
pub struct BlockCountPlanner {
    block_size: usize,
    preserve_format_boundaries: bool,
    blocks: usize,
    pending_len: usize,
    pending_format: Option<FileFormat>,
}

impl BlockCountPlanner {
    pub fn new(block_size: usize, preserve_format_boundaries: bool) -> Self {
        Self {
            block_size: block_size.max(1),
            preserve_format_boundaries,
            blocks: 0,
            pending_len: 0,
            pending_format: None,
        }
    }

    pub fn push_len(&mut self, mut len: usize, file_type_hint: FileFormat) {
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

    pub fn finish(mut self) -> usize {
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

pub(super) fn join_safe(root: &Path, rel_path: &str) -> Result<PathBuf> {
    let rel = Path::new(rel_path);
    if rel.is_absolute() {
        return Err(crate::OxideError::InvalidFormat(
            "absolute paths are not allowed in archive metadata",
        ));
    }

    for component in rel.components() {
        match component {
            Component::Normal(_) => {}
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(crate::OxideError::InvalidFormat(
                    "unsafe path component in archive metadata",
                ));
            }
        }
    }

    Ok(root.join(rel))
}
