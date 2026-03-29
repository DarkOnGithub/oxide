use std::fs;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use jwalk::WalkDir;
use memmap2::Mmap;

use crate::compression::{CompressionRequest, apply_compression_request_with_scratch};
use crate::dictionary::classify_sample;
use crate::dictionary::normalized_extension_from_path;
use crate::format::should_force_raw_storage;
use crate::types::{Batch, BatchData, ChunkEncodingPlan, Result};

use super::types::{ArchiveListingEntry, ArchiveSourceKind};

pub(super) const SOURCE_KIND_DIRECTORY_FLAG: u32 = 1 << 0;

/// Metadata for a discovered file in a directory tree.
#[derive(Debug, Clone)]
pub(super) struct DirectorySpec {
    pub(super) rel_path: String,
    pub(super) mode: u32,
    pub(super) mtime: super::types::ArchiveTimestamp,
    pub(super) uid: u32,
    pub(super) gid: u32,
}

/// Metadata for a discovered file in a directory tree.
#[derive(Debug, Clone)]
pub(super) struct DirectoryFileSpec {
    pub(super) entry: DirectorySpec,
    pub(super) full_path: PathBuf,
    pub(super) size: u64,
}

#[derive(Debug, Clone)]
pub(super) struct DirectorySymlinkSpec {
    pub(super) entry: DirectorySpec,
    pub(super) target: String,
}

/// Result of a directory discovery operation.
#[derive(Debug, Clone)]
pub(super) struct DirectoryDiscovery {
    pub(super) root: PathBuf,
    pub(super) directories: Vec<DirectorySpec>,
    pub(super) symlinks: Vec<DirectorySymlinkSpec>,
    pub(super) files: Vec<DirectoryFileSpec>,
    pub(super) input_bytes_total: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct FileProbePlan {
    pub(super) force_raw_storage: bool,
    pub(super) max_block_size: usize,
}

/// Utility for grouping file data into batches while respecting raw-storage boundaries.
#[derive(Debug, Clone)]
pub struct DirectoryBatchSubmitter {
    default_source_path: PathBuf,
    block_size: usize,
    compression_plan: ChunkEncodingPlan,
    next_block_id: usize,
    pending: Vec<u8>,
    pending_key: Option<BatchBoundaryKey>,
    pending_source_path: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BatchBoundaryKey {
    force_raw_storage: bool,
    extension: Option<String>,
    max_block_size: usize,
}

impl DirectoryBatchSubmitter {
    pub fn new(source_path: PathBuf, block_size: usize) -> Self {
        Self::new_with_plan(source_path, block_size, ChunkEncodingPlan::default())
    }

    pub fn new_with_plan(
        source_path: PathBuf,
        block_size: usize,
        compression_plan: ChunkEncodingPlan,
    ) -> Self {
        Self {
            default_source_path: source_path,
            block_size: block_size.max(1),
            compression_plan,
            next_block_id: 0,
            pending: Vec::with_capacity(block_size.max(1)),
            pending_key: None,
            pending_source_path: None,
        }
    }

    pub fn push_bytes<P, F>(
        &mut self,
        source_path: P,
        bytes: &[u8],
        force_raw_storage: bool,
        submit: F,
    ) -> Result<()>
    where
        P: AsRef<Path>,
        F: FnMut(Batch) -> Result<()>,
    {
        self.push_bytes_with_limit(
            source_path,
            bytes,
            force_raw_storage,
            self.block_size,
            submit,
        )
    }

    pub fn push_bytes_with_limit<P, F>(
        &mut self,
        source_path: P,
        mut bytes: &[u8],
        force_raw_storage: bool,
        max_block_size: usize,
        mut submit: F,
    ) -> Result<()>
    where
        P: AsRef<Path>,
        F: FnMut(Batch) -> Result<()>,
    {
        let source_path = source_path.as_ref();
        while !bytes.is_empty() {
            self.prepare_pending_state(
                source_path,
                force_raw_storage,
                max_block_size,
                &mut submit,
            )?;

            let room = self
                .active_block_size()
                .saturating_sub(self.pending.len())
                .max(1);
            let take = room.min(bytes.len());
            self.pending.extend_from_slice(&bytes[..take]);
            bytes = &bytes[take..];

            if self.pending.len() == self.active_block_size() {
                self.flush_pending(&mut submit)?;
            }
        }

        Ok(())
    }

    pub fn push_mapped<P, F>(
        &mut self,
        source_path: P,
        map: Arc<Mmap>,
        start: usize,
        end: usize,
        force_raw_storage: bool,
        submit: F,
    ) -> Result<()>
    where
        P: AsRef<Path>,
        F: FnMut(Batch) -> Result<()>,
    {
        self.push_mapped_with_limit(
            source_path,
            map,
            start,
            end,
            force_raw_storage,
            self.block_size,
            submit,
        )
    }

    pub fn push_mapped_with_limit<P, F>(
        &mut self,
        source_path: P,
        map: Arc<Mmap>,
        start: usize,
        end: usize,
        force_raw_storage: bool,
        max_block_size: usize,
        mut submit: F,
    ) -> Result<()>
    where
        P: AsRef<Path>,
        F: FnMut(Batch) -> Result<()>,
    {
        if end < start || end > map.len() {
            return Err(crate::OxideError::InvalidFormat(
                "invalid mapped batch range",
            ));
        }

        let source_path = source_path.as_ref();
        let mut offset = start;
        while offset < end {
            self.prepare_pending_state(
                source_path,
                force_raw_storage,
                max_block_size,
                &mut submit,
            )?;

            if !self.pending.is_empty() {
                let room = self
                    .active_block_size()
                    .saturating_sub(self.pending.len())
                    .max(1);
                let take = room.min(end - offset);
                self.pending.extend_from_slice(&map[offset..offset + take]);
                offset += take;

                if self.pending.len() == self.active_block_size() {
                    self.flush_pending(&mut submit)?;
                }
                continue;
            }

            let remaining = end - offset;
            if remaining >= self.active_block_size() {
                let batch_end = offset + self.active_block_size();
                self.submit_batch(
                    source_path.to_path_buf(),
                    BatchData::Mapped {
                        map: Arc::clone(&map),
                        start: offset,
                        end: batch_end,
                    },
                    force_raw_storage,
                    &mut submit,
                )?;
                offset = batch_end;
                continue;
            }

            self.pending.extend_from_slice(&map[offset..end]);
            offset = end;
        }

        Ok(())
    }

    pub fn finish<F>(&mut self, submit: F) -> Result<()>
    where
        F: FnMut(Batch) -> Result<()>,
    {
        let mut submit = submit;
        if !self.pending.is_empty() {
            self.flush_pending(&mut submit)?;
        }
        Ok(())
    }

    fn prepare_pending_state<F>(
        &mut self,
        source_path: &Path,
        force_raw_storage: bool,
        max_block_size: usize,
        submit: &mut F,
    ) -> Result<()>
    where
        F: FnMut(Batch) -> Result<()>,
    {
        let next_key = BatchBoundaryKey {
            force_raw_storage,
            extension: normalized_extension_from_path(source_path),
            max_block_size: max_block_size.max(self.block_size),
        };

        match self.pending_key.as_ref() {
            Some(current) if current != &next_key => {
                if !self.pending.is_empty() {
                    self.flush_pending(submit)?;
                }
                self.pending_key = Some(next_key);
                self.pending_source_path = Some(source_path.to_path_buf());
            }
            Some(_) => {}
            None => {
                self.pending_key = Some(next_key);
                self.pending_source_path = Some(source_path.to_path_buf());
            }
        }

        Ok(())
    }

    fn flush_pending<F>(&mut self, submit: &mut F) -> Result<()>
    where
        F: FnMut(Batch) -> Result<()>,
    {
        let chunk = std::mem::replace(&mut self.pending, Vec::with_capacity(self.block_size));
        self.submit_batch(
            self.pending_source_path
                .clone()
                .unwrap_or_else(|| self.default_source_path.clone()),
            BatchData::Owned(Bytes::from(chunk)),
            self.pending_key
                .as_ref()
                .map(|key| key.force_raw_storage)
                .unwrap_or(false),
            submit,
        )?;
        self.pending_key = None;
        self.pending_source_path = None;
        Ok(())
    }

    fn active_block_size(&self) -> usize {
        self.pending_key
            .as_ref()
            .map(|key| key.max_block_size)
            .unwrap_or(self.block_size)
    }

    fn submit_batch<F>(
        &mut self,
        source_path: PathBuf,
        data: BatchData,
        force_raw_storage: bool,
        submit: &mut F,
    ) -> Result<()>
    where
        F: FnMut(Batch) -> Result<()>,
    {
        let batch = Batch {
            id: self.next_block_id,
            source_path,
            data,
            stream_id: 0,
            compression_plan: self.compression_plan,
            force_raw_storage,
        };
        submit(batch)?;
        self.next_block_id += 1;
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

    let mut directories = Vec::<DirectorySpec>::new();
    let mut symlinks = Vec::<DirectorySymlinkSpec>::new();
    let mut files = Vec::<DirectoryFileSpec>::new();
    let mut input_bytes_total = 0u64;

    for entry in WalkDir::new(root).skip_hidden(false).follow_links(false) {
        let entry = entry.map_err(anyhow::Error::from)?;
        let path = entry.path();
        if path == root {
            continue;
        }

        let rel_path = path
            .strip_prefix(root)
            .map_err(|_| crate::OxideError::InvalidFormat("invalid relative path"))?
            .to_path_buf();
        let metadata = fs::symlink_metadata(&path)?;

        if entry.file_type().is_dir() {
            directories.push(DirectorySpec {
                rel_path: relative_path_to_utf8(&rel_path)?,
                mode: metadata_mode(&metadata),
                mtime: metadata_mtime(&metadata)?,
                uid: metadata_uid(&metadata),
                gid: metadata_gid(&metadata),
            });
        } else if entry.file_type().is_symlink() {
            let target = fs::read_link(&path)?;
            let target = target.to_str().ok_or(crate::OxideError::InvalidFormat(
                "non-utf8 symlink target not supported",
            ))?;
            symlinks.push(DirectorySymlinkSpec {
                entry: DirectorySpec {
                    rel_path: relative_path_to_utf8(&rel_path)?,
                    mode: metadata_mode(&metadata),
                    mtime: metadata_mtime(&metadata)?,
                    uid: metadata_uid(&metadata),
                    gid: metadata_gid(&metadata),
                },
                target: target.to_string(),
            });
        } else if entry.file_type().is_file() {
            let full_path = path;
            let rel_path = relative_path_to_utf8(&rel_path)?;
            let size = metadata.len();

            input_bytes_total =
                input_bytes_total
                    .checked_add(size)
                    .ok_or(crate::OxideError::InvalidFormat(
                        "directory input size overflow",
                    ))?;
            files.push(DirectoryFileSpec {
                entry: DirectorySpec {
                    rel_path,
                    mode: metadata_mode(&metadata),
                    mtime: metadata_mtime(&metadata)?,
                    uid: metadata_uid(&metadata),
                    gid: metadata_gid(&metadata),
                },
                full_path,
                size,
            });
        } else {
            return Err(crate::OxideError::InvalidFormat(
                "directory archive supports regular files, directories, and symlinks only",
            ));
        }
    }

    directories.sort_by(|left, right| left.rel_path.cmp(&right.rel_path));
    symlinks.sort_by(|left, right| left.entry.rel_path.cmp(&right.entry.rel_path));
    files.sort_by(|left, right| left.entry.rel_path.cmp(&right.entry.rel_path));

    Ok(DirectoryDiscovery {
        root: root.to_path_buf(),
        directories,
        symlinks,
        files,
        input_bytes_total,
    })
}

pub(super) fn manifest_from_discovery(
    discovery: &DirectoryDiscovery,
) -> crate::types::Result<crate::format::ArchiveManifest> {
    let mut entries = Vec::with_capacity(
        discovery.directories.len() + discovery.symlinks.len() + discovery.files.len(),
    );
    entries.extend(discovery.directories.iter().map(|directory| {
        ArchiveListingEntry::directory(
            directory.rel_path.clone(),
            directory.mode,
            directory.mtime,
            directory.uid,
            directory.gid,
        )
    }));

    entries.extend(
        discovery
            .symlinks
            .iter()
            .map(|symlink| ArchiveListingEntry {
                path: symlink.entry.rel_path.clone(),
                kind: crate::ArchiveEntryKind::Symlink,
                target: Some(symlink.target.clone()),
                size: 0,
                mode: symlink.entry.mode,
                mtime: symlink.entry.mtime,
                uid: symlink.entry.uid,
                gid: symlink.entry.gid,
                content_offset: 0,
            }),
    );

    let mut content_offset = 0u64;
    for file in &discovery.files {
        entries.push(ArchiveListingEntry::file(
            file.entry.rel_path.clone(),
            file.size,
            file.entry.mode,
            file.entry.mtime,
            file.entry.uid,
            file.entry.gid,
            content_offset,
        ));
        content_offset = content_offset.saturating_add(file.size);
    }

    Ok(crate::format::ArchiveManifest::new(entries))
}

pub(super) fn detect_file_probe_plans(
    discovery: &DirectoryDiscovery,
    target_block_size: usize,
    compression_plan: ChunkEncodingPlan,
    _threads: usize,
) -> Result<Vec<FileProbePlan>> {
    let target_block_size = target_block_size.max(1);
    let adaptive_max_block_size = target_block_size.saturating_mul(2).max(target_block_size);
    let small_file_cutoff = target_block_size / 4;

    let mut plans = Vec::with_capacity(discovery.files.len());
    for file in &discovery.files {
        let force_raw_storage = should_force_raw_storage(&file.full_path);
        let max_block_size = if force_raw_storage {
            target_block_size
        } else {
            let file_size = usize::try_from(file.size)
                .map_err(|_| crate::OxideError::InvalidFormat("file size exceeds usize range"))?;
            if file_size == 0 || small_file_cutoff == 0 || file_size >= small_file_cutoff {
                target_block_size
            } else if is_small_file_highly_compressible(&file.full_path, compression_plan)? {
                adaptive_max_block_size
            } else {
                target_block_size
            }
        };

        plans.push(FileProbePlan {
            force_raw_storage,
            max_block_size,
        });
    }

    Ok(plans)
}

pub(super) fn estimate_directory_block_count(
    discovery: &DirectoryDiscovery,
    file_probe_plans: &[FileProbePlan],
    block_size: usize,
) -> Result<u32> {
    if discovery.files.len() != file_probe_plans.len() {
        return Err(crate::OxideError::InvalidFormat(
            "directory probe plan mismatch",
        ));
    }

    let mut planner = BlockCountPlanner::new(block_size);
    for (index, file) in discovery.files.iter().enumerate() {
        let file_size = usize::try_from(file.size)
            .map_err(|_| crate::OxideError::InvalidFormat("file size exceeds usize range"))?;
        planner.push_file_with_limit(
            &file.full_path,
            file_size,
            file_probe_plans[index].force_raw_storage,
            file_probe_plans[index].max_block_size,
        );
    }

    u32::try_from(planner.finish())
        .map_err(|_| crate::OxideError::InvalidFormat("too many blocks for OXZ v2"))
}

#[cfg(test)]
#[path = "../../tests/pipeline/directory.rs"]
mod tests;

fn metadata_mtime(metadata: &fs::Metadata) -> Result<super::types::ArchiveTimestamp> {
    Ok(super::types::ArchiveTimestamp::from_system_time(
        metadata.modified()?,
    ))
}

#[cfg(unix)]
fn metadata_mode(metadata: &fs::Metadata) -> u32 {
    use std::os::unix::fs::MetadataExt;

    metadata.mode()
}

#[cfg(not(unix))]
fn metadata_mode(metadata: &fs::Metadata) -> u32 {
    if metadata.permissions().readonly() {
        0o444
    } else {
        0o666
    }
}

#[cfg(unix)]
fn metadata_uid(metadata: &fs::Metadata) -> u32 {
    use std::os::unix::fs::MetadataExt;

    metadata.uid()
}

#[cfg(not(unix))]
fn metadata_uid(_: &fs::Metadata) -> u32 {
    0
}

#[cfg(unix)]
fn metadata_gid(metadata: &fs::Metadata) -> u32 {
    use std::os::unix::fs::MetadataExt;

    metadata.gid()
}

#[cfg(not(unix))]
fn metadata_gid(_: &fs::Metadata) -> u32 {
    0
}

#[derive(Debug, Clone)]
pub struct BlockCountPlanner {
    block_size: usize,
    blocks: usize,
    pending_len: usize,
    pending_key: Option<BatchBoundaryKey>,
}

impl BlockCountPlanner {
    pub fn new(block_size: usize) -> Self {
        Self {
            block_size: block_size.max(1),
            blocks: 0,
            pending_len: 0,
            pending_key: None,
        }
    }

    pub fn push_len(&mut self, len: usize, force_raw_storage: bool) {
        self.push_len_with_extension(None, len, force_raw_storage);
    }

    pub fn push_file(&mut self, path: &Path, len: usize, force_raw_storage: bool) {
        self.push_len_with_extension(normalized_extension_from_path(path), len, force_raw_storage);
    }

    pub fn push_file_with_limit(
        &mut self,
        path: &Path,
        len: usize,
        force_raw_storage: bool,
        max_block_size: usize,
    ) {
        self.push_len_with_key(
            BatchBoundaryKey {
                force_raw_storage,
                extension: normalized_extension_from_path(path),
                max_block_size: max_block_size.max(self.block_size),
            },
            len,
        );
    }

    fn push_len_with_extension(
        &mut self,
        extension: Option<String>,
        len: usize,
        force_raw_storage: bool,
    ) {
        self.push_len_with_key(
            BatchBoundaryKey {
                force_raw_storage,
                extension,
                max_block_size: self.block_size,
            },
            len,
        );
    }

    fn push_len_with_key(&mut self, next_key: BatchBoundaryKey, mut len: usize) {
        while len > 0 {
            match self.pending_key.as_ref() {
                Some(current) if current != &next_key => {
                    self.flush_pending();
                    self.pending_key = Some(next_key.clone());
                }
                Some(_) => {}
                None => {
                    self.pending_key = Some(next_key.clone());
                }
            }

            let room = next_key
                .max_block_size
                .saturating_sub(self.pending_len)
                .max(1);
            let take = room.min(len);
            self.pending_len += take;
            len -= take;

            if self.pending_len == next_key.max_block_size {
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
            self.pending_key = None;
        }
    }
}

fn is_small_file_highly_compressible(
    path: &Path,
    compression_plan: ChunkEncodingPlan,
) -> Result<bool> {
    let sample = fs::read(path)?;
    if sample.is_empty() {
        return Ok(false);
    }

    if !matches!(classify_sample(&sample), crate::DictionaryClass::Binary) {
        return Ok(true);
    }

    let mut scratch = crate::compression::CompressionScratchArena::new();
    let compressed = apply_compression_request_with_scratch(
        CompressionRequest {
            data: &sample,
            algo: compression_plan.algo,
            level: compression_plan.level,
            lzma_extreme: compression_plan.lzma_extreme,
            lzma_dictionary_size: compression_plan.lzma_dictionary_size,
            dictionary_id: 0,
            dictionary: None,
        },
        &mut scratch,
    )?;

    Ok(compressed.len().saturating_mul(100) <= sample.len().saturating_mul(90))
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
