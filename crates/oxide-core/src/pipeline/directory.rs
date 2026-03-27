use std::fs;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use jwalk::WalkDir;
use memmap2::Mmap;

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
}

/// Utility for grouping file data into batches while respecting raw-storage boundaries.
#[derive(Debug, Clone)]
pub struct DirectoryBatchSubmitter {
    source_path: PathBuf,
    block_size: usize,
    compression_plan: ChunkEncodingPlan,
    next_block_id: usize,
    pending: Vec<u8>,
    pending_force_raw_storage: Option<bool>,
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
            source_path,
            block_size: block_size.max(1),
            compression_plan,
            next_block_id: 0,
            pending: Vec::with_capacity(block_size.max(1)),
            pending_force_raw_storage: None,
        }
    }

    pub fn push_bytes<F>(
        &mut self,
        mut bytes: &[u8],
        force_raw_storage: bool,
        mut submit: F,
    ) -> Result<()>
    where
        F: FnMut(Batch) -> Result<()>,
    {
        while !bytes.is_empty() {
            self.prepare_pending_state(force_raw_storage, &mut submit)?;

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

    pub fn push_mapped<F>(
        &mut self,
        map: Arc<Mmap>,
        start: usize,
        end: usize,
        force_raw_storage: bool,
        mut submit: F,
    ) -> Result<()>
    where
        F: FnMut(Batch) -> Result<()>,
    {
        if end < start || end > map.len() {
            return Err(crate::OxideError::InvalidFormat(
                "invalid mapped batch range",
            ));
        }

        let mut offset = start;
        while offset < end {
            self.prepare_pending_state(force_raw_storage, &mut submit)?;

            if !self.pending.is_empty() {
                let room = self.block_size.saturating_sub(self.pending.len()).max(1);
                let take = room.min(end - offset);
                self.pending.extend_from_slice(&map[offset..offset + take]);
                offset += take;

                if self.pending.len() == self.block_size {
                    self.flush_pending(&mut submit)?;
                }
                continue;
            }

            let remaining = end - offset;
            if remaining >= self.block_size {
                let batch_end = offset + self.block_size;
                self.submit_batch(
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

    fn prepare_pending_state<F>(&mut self, force_raw_storage: bool, submit: &mut F) -> Result<()>
    where
        F: FnMut(Batch) -> Result<()>,
    {
        match self.pending_force_raw_storage {
            Some(current) if current != force_raw_storage => {
                if !self.pending.is_empty() {
                    self.flush_pending(submit)?;
                }
                self.pending_force_raw_storage = Some(force_raw_storage);
            }
            Some(_) => {}
            None => {
                self.pending_force_raw_storage = Some(force_raw_storage);
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
            BatchData::Owned(Bytes::from(chunk)),
            self.pending_force_raw_storage.unwrap_or(false),
            submit,
        )?;
        self.pending_force_raw_storage = None;
        Ok(())
    }

    fn submit_batch<F>(
        &mut self,
        data: BatchData,
        force_raw_storage: bool,
        submit: &mut F,
    ) -> Result<()>
    where
        F: FnMut(Batch) -> Result<()>,
    {
        let batch = Batch {
            id: self.next_block_id,
            source_path: self.source_path.clone(),
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
    _threads: usize,
) -> Result<Vec<FileProbePlan>> {
    Ok(discovery
        .files
        .iter()
        .map(|file| FileProbePlan {
            force_raw_storage: should_force_raw_storage(&file.full_path),
        })
        .collect())
}

pub(super) fn estimate_directory_block_count(
    discovery: &DirectoryDiscovery,
    file_force_raw_storage: &[bool],
    block_size: usize,
) -> Result<u32> {
    if discovery.files.len() != file_force_raw_storage.len() {
        return Err(crate::OxideError::InvalidFormat(
            "directory raw storage plan mismatch",
        ));
    }

    if file_force_raw_storage.iter().all(|force_raw| !*force_raw) {
        let block_size = block_size.max(1) as u64;
        let block_count = discovery.input_bytes_total.div_ceil(block_size);
        return u32::try_from(block_count)
            .map_err(|_| crate::OxideError::InvalidFormat("too many blocks for OXZ v2"));
    }

    let mut planner = BlockCountPlanner::new(block_size);
    for (index, file) in discovery.files.iter().enumerate() {
        let file_size = usize::try_from(file.size)
            .map_err(|_| crate::OxideError::InvalidFormat("file size exceeds usize range"))?;
        planner.push_len(file_size, file_force_raw_storage[index]);
    }

    u32::try_from(planner.finish())
        .map_err(|_| crate::OxideError::InvalidFormat("too many blocks for OXZ v2"))
}

#[cfg(test)]
mod tests {
    use super::{detect_file_probe_plans, discover_directory_tree, estimate_directory_block_count};
    use std::fs;
    use tempfile::tempdir;

    #[cfg(unix)]
    use std::os::unix::fs::symlink;

    #[test]
    fn discovery_collects_sizes_in_one_pass() {
        let temp = tempdir().expect("tempdir");
        let root = temp.path();
        let nested = root.join("nested");
        fs::create_dir(&nested).expect("create nested dir");
        fs::write(root.join("notes.txt"), b"hello\nworld\n").expect("write text");
        fs::write(nested.join("payload.bin"), [0, 159, 146, 150]).expect("write binary");

        let discovery = discover_directory_tree(root).expect("discover directory");

        assert_eq!(discovery.directories.len(), 1);
        assert_eq!(discovery.directories[0].rel_path, "nested");
        assert_eq!(discovery.symlinks.len(), 0);
        assert_eq!(discovery.files.len(), 2);
        assert_eq!(discovery.files[0].entry.rel_path, "nested/payload.bin");
        assert_eq!(discovery.files[1].entry.rel_path, "notes.txt");
        assert_eq!(discovery.input_bytes_total, 16);
    }

    #[test]
    fn discovery_includes_hidden_files_and_directories() {
        let temp = tempdir().expect("tempdir");
        let root = temp.path();
        let hidden_dir = root.join(".hidden");
        let nested_hidden_dir = hidden_dir.join("nested");

        fs::create_dir(&hidden_dir).expect("create hidden dir");
        fs::create_dir(&nested_hidden_dir).expect("create nested hidden dir");
        fs::write(root.join(".root-hidden.txt"), b"root").expect("write root hidden file");
        fs::write(hidden_dir.join("visible.txt"), b"hidden-dir").expect("write hidden dir file");
        fs::write(nested_hidden_dir.join(".nested-hidden.txt"), b"nested")
            .expect("write nested hidden file");

        let discovery = discover_directory_tree(root).expect("discover directory");

        assert_eq!(
            discovery
                .directories
                .iter()
                .map(|entry| entry.rel_path.as_str())
                .collect::<Vec<_>>(),
            vec![".hidden", ".hidden/nested"]
        );
        assert_eq!(
            discovery
                .files
                .iter()
                .map(|entry| entry.entry.rel_path.as_str())
                .collect::<Vec<_>>(),
            vec![
                ".hidden/nested/.nested-hidden.txt",
                ".hidden/visible.txt",
                ".root-hidden.txt",
            ]
        );
        assert_eq!(discovery.input_bytes_total, 4 + 10 + 6);
    }

    #[cfg(unix)]
    #[test]
    fn discovery_preserves_symlinks_without_following_them() {
        let temp = tempdir().expect("tempdir");
        let root = temp.path();
        let nested = root.join("nested");
        fs::create_dir(&nested).expect("create nested dir");
        symlink(&nested, root.join("nested-link")).expect("create symlink");

        let discovery = discover_directory_tree(root).expect("discover directory");

        assert_eq!(discovery.directories.len(), 1);
        assert_eq!(discovery.symlinks.len(), 1);
        assert_eq!(discovery.symlinks[0].entry.rel_path, "nested-link");
        assert_eq!(
            discovery.symlinks[0].target,
            nested.to_string_lossy().as_ref()
        );
        assert_eq!(discovery.files.len(), 0);
    }

    #[test]
    fn probe_plan_uses_extension_based_raw_storage() {
        let temp = tempdir().expect("tempdir");
        let root = temp.path();
        fs::write(root.join("photo.jpg"), [0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10])
            .expect("write jpeg");

        let discovery = discover_directory_tree(root).expect("discover directory");
        let plans = detect_file_probe_plans(&discovery, 1).expect("probe plans");

        assert_eq!(plans.len(), 1);
        assert!(plans[0].force_raw_storage);
    }

    #[test]
    fn block_count_without_raw_boundaries_uses_total_bytes_only() {
        let temp = tempdir().expect("tempdir");
        let root = temp.path();
        fs::write(root.join("alpha.txt"), b"alpha\n").expect("write alpha");
        fs::write(root.join("beta.bin"), [0, 159, 146, 150, 42]).expect("write beta");

        let discovery = discover_directory_tree(root).expect("discover directory");
        let raw_plan = vec![false; discovery.files.len()];
        let block_count = estimate_directory_block_count(&discovery, &raw_plan, 4).expect("plan");

        assert_eq!(block_count, 3);
    }

    #[test]
    fn block_count_accounts_for_raw_storage_policy_changes() {
        let temp = tempdir().expect("tempdir");
        let root = temp.path();
        fs::write(root.join("first.txt"), b"aaaaaa").expect("write first");
        fs::write(root.join("second.jpg"), b"bbbbbb").expect("write second");

        let discovery = discover_directory_tree(root).expect("discover directory");
        let raw_plan = vec![false, true];
        let block_count = estimate_directory_block_count(&discovery, &raw_plan, 8).expect("plan");

        assert_eq!(block_count, 2);
    }
}

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
    pending_force_raw_storage: Option<bool>,
}

impl BlockCountPlanner {
    pub fn new(block_size: usize) -> Self {
        Self {
            block_size: block_size.max(1),
            blocks: 0,
            pending_len: 0,
            pending_force_raw_storage: None,
        }
    }

    pub fn push_len(&mut self, mut len: usize, force_raw_storage: bool) {
        while len > 0 {
            match self.pending_force_raw_storage {
                Some(current) if current != force_raw_storage => {
                    self.flush_pending();
                    self.pending_force_raw_storage = Some(force_raw_storage);
                }
                Some(_) => {}
                None => {
                    self.pending_force_raw_storage = Some(force_raw_storage);
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
            self.pending_force_raw_storage = None;
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
