use std::collections::{HashMap, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, Sender, TrySendError, bounded};

use super::*;
use crate::format::{ArchiveManifest, ChunkDescriptor};
use crate::pipeline::archive::reorder_writer::{OwnedChunk, SharedChunk};

/// Per-shard task queue. Larger values reduce main-thread blocking when shards
/// briefly fall behind (telemetry often pegged smaller queues at full capacity).
const DIRECT_WRITE_QUEUE_CAPACITY: usize = 1024;
/// Upper bound on concurrently open output files per direct-write shard.
const DIRECT_FILE_CACHE_MAX_PER_SHARD: usize = 96;
/// Bounded parent-directory fd cache per shard (`openat`); must stay small for `ulimit`.
#[cfg(unix)]
const PARENT_DIR_LRU_CAP: usize = 8;

#[derive(Debug)]
pub(crate) struct DirectDirectoryRestoreWriter {
    block_extents: Vec<Vec<DirectBlockExtent>>,
    task_txs: Vec<Sender<DirectWriteTask>>,
    handles: Vec<thread::JoinHandle<Result<DirectShardOutcome>>>,
    pending_file_metadata: Vec<PendingMetadata>,
    pending_directory_metadata: Vec<PendingMetadata>,
    preserve_metadata: bool,
    stats: DirectoryRestoreStats,
}

#[derive(Debug, Clone)]
struct DirectFileEntry {
    id: usize,
    path: Arc<PathBuf>,
    entry: crate::ArchiveListingEntry,
}

#[derive(Debug, Clone)]
struct DirectBlockExtent {
    file_id: usize,
    shard: usize,
    path: Arc<PathBuf>,
    block_offset: usize,
    len: usize,
    file_offset: u64,
}

#[derive(Debug)]
enum DirectWriteTask {
    Write {
        file_id: usize,
        path: Arc<PathBuf>,
        chunk: SharedChunk,
        range: std::ops::Range<usize>,
        file_offset: u64,
    },
}

#[derive(Debug, Default)]
struct DirectShardOutcome {
    output_data: Duration,
}

impl DirectDirectoryRestoreWriter {
    pub(crate) fn create(
        root: &Path,
        manifest: ArchiveManifest,
        block_descriptors: &[ChunkDescriptor],
        write_shards: usize,
        preserve_metadata: bool,
    ) -> Result<Self> {
        let started = Instant::now();
        let entries = build_restore_entries(root, manifest)?;
        let mut stats = DirectoryRestoreStats::default();
        let mut pending_file_metadata = Vec::new();
        let mut pending_directory_metadata = Vec::new();
        let files = prepare_direct_entries(
            root,
            &entries,
            preserve_metadata,
            &mut pending_file_metadata,
            &mut pending_directory_metadata,
            &mut stats,
        )?;
        stats.record_output_prepare_directories(started.elapsed());

        let shard_count = write_shards.max(1);
        stats.ensure_write_shards(shard_count);
        let block_extents = build_block_extents(block_descriptors, &files, shard_count)?;
        let mut task_txs = Vec::with_capacity(shard_count);
        let mut handles = Vec::with_capacity(shard_count);

        let file_cache_max_open = direct_write_file_cache_max_open(shard_count);
        for shard in 0..shard_count {
            let (tx, rx) = bounded::<DirectWriteTask>(DIRECT_WRITE_QUEUE_CAPACITY);
            handles.push(thread::spawn(move || {
                run_direct_write_shard(rx, file_cache_max_open)
            }));
            task_txs.push(tx);
            stats.record_write_shard_queue_peak(shard, 0);
        }

        stats.entry_count = entries.len() as u64;
        Ok(Self {
            block_extents,
            task_txs,
            handles,
            pending_file_metadata,
            pending_directory_metadata,
            preserve_metadata,
            stats,
        })
    }

    pub(crate) fn write_decoded_block(&mut self, block_index: usize, block: OwnedChunk) -> Result<()> {
        let shared = block.into_shared();
        let extents = self
            .block_extents
            .get(block_index)
            .ok_or(crate::OxideError::InvalidFormat(
                "decoded directory block index out of range",
            ))?;
        for extent in extents {
            let tx = self.task_txs.get(extent.shard).ok_or_else(|| {
                crate::OxideError::CompressionError("direct write shard unavailable".to_string())
            })?;
            let task = DirectWriteTask::Write {
                file_id: extent.file_id,
                path: Arc::clone(&extent.path),
                chunk: shared.clone(),
                range: extent.block_offset..extent.block_offset + extent.len,
                file_offset: extent.file_offset,
            };
            match tx.try_send(task) {
                Ok(()) => {
                    self.stats
                        .record_write_shard_queue_peak(extent.shard, tx.len());
                }
                Err(TrySendError::Full(task)) => {
                    let blocked_started = Instant::now();
                    tx.send(task).map_err(|_| {
                        crate::OxideError::CompressionError(
                            "direct write shard queue closed before completion".to_string(),
                        )
                    })?;
                    self.stats
                        .record_write_shard_blocked(extent.shard, blocked_started.elapsed());
                    self.stats
                        .record_write_shard_queue_peak(extent.shard, tx.len());
                }
                Err(TrySendError::Disconnected(_)) => {
                    return Err(crate::OxideError::CompressionError(
                        "direct write shard disconnected before completion".to_string(),
                    ));
                }
            }
        }
        self.stats.output_bytes_total = self
            .stats
            .output_bytes_total
            .saturating_add(shared.len() as u64);
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<DirectoryRestoreStats> {
        let task_txs = std::mem::take(&mut self.task_txs);
        drop(task_txs);
        for (shard, handle) in self.handles.drain(..).enumerate() {
            let outcome = handle.join().map_err(|payload| {
                let details = if let Some(message) = payload.downcast_ref::<&str>() {
                    (*message).to_string()
                } else if let Some(message) = payload.downcast_ref::<String>() {
                    message.clone()
                } else {
                    "unknown panic payload".to_string()
                };
                crate::OxideError::CompressionError(format!(
                    "direct write shard thread panicked: {details}"
                ))
            })??;
            self.stats.record_output_data(outcome.output_data);
            self.stats
                .record_write_shard_output_data(shard, outcome.output_data);
        }

        self.finalize_file_metadata()?;
        self.finalize_directories()?;
        Ok(self.stats)
    }

    fn finalize_file_metadata(&mut self) -> Result<()> {
        if self.pending_file_metadata.is_empty() || !self.preserve_metadata {
            return Ok(());
        }

        let started = Instant::now();
        apply_pending_metadata_in_parallel(&self.pending_file_metadata)?;
        self.stats.record_output_metadata_file(started.elapsed());
        Ok(())
    }

    fn finalize_directories(&mut self) -> Result<()> {
        if self.pending_directory_metadata.is_empty() || !self.preserve_metadata {
            return Ok(());
        }

        self.pending_directory_metadata
            .sort_unstable_by_key(|directory| Reverse(directory.depth));
        let started = Instant::now();
        let mut range_start = 0usize;
        while range_start < self.pending_directory_metadata.len() {
            let depth = self.pending_directory_metadata[range_start].depth;
            let mut range_end = range_start + 1;
            while range_end < self.pending_directory_metadata.len()
                && self.pending_directory_metadata[range_end].depth == depth
            {
                range_end += 1;
            }
            apply_pending_metadata_in_parallel(
                &self.pending_directory_metadata[range_start..range_end],
            )?;
            range_start = range_end;
        }
        self.stats
            .record_output_metadata_directory(started.elapsed());
        Ok(())
    }
}

fn prepare_direct_entries(
    root: &Path,
    entries: &[RestoreEntry],
    preserve_metadata: bool,
    pending_file_metadata: &mut Vec<PendingMetadata>,
    pending_directory_metadata: &mut Vec<PendingMetadata>,
    stats: &mut DirectoryRestoreStats,
) -> Result<Vec<DirectFileEntry>> {
    let mut created_directories = HashSet::new();
    ensure_direct_directory(root, root, &mut created_directories, stats)?;

    for restore_entry in entries {
        match restore_entry.entry.kind {
            crate::ArchiveEntryKind::Directory => {
                ensure_direct_directory(root, &restore_entry.path, &mut created_directories, stats)?;
                pending_directory_metadata
                    .push(PendingMetadata::new(restore_entry.path.clone(), restore_entry.entry.clone()));
            }
            crate::ArchiveEntryKind::File | crate::ArchiveEntryKind::Symlink => {
                if let Some(parent) = restore_entry
                    .path
                    .parent()
                    .filter(|path| !path.as_os_str().is_empty())
                {
                    ensure_direct_directory(root, parent, &mut created_directories, stats)?;
                }
            }
        }
    }

    let mut files = Vec::new();
    for restore_entry in entries {
        match restore_entry.entry.kind {
            crate::ArchiveEntryKind::File => {
                if restore_entry.entry.size == 0 {
                    let create_started = Instant::now();
                    let file = fs::File::create(&restore_entry.path)?;
                    drop(file);
                    stats.record_output_create_file(create_started.elapsed());
                }
                pending_file_metadata
                    .push(PendingMetadata::new(restore_entry.path.clone(), restore_entry.entry.clone()));
                files.push(DirectFileEntry {
                    id: files.len(),
                    path: Arc::new(restore_entry.path.clone()),
                    entry: restore_entry.entry.clone(),
                });
            }
            crate::ArchiveEntryKind::Symlink => {
                let target = restore_entry.entry.target.as_deref().ok_or(
                    crate::OxideError::InvalidFormat("symlink manifest entry missing target"),
                )?;
                let create_started = Instant::now();
                metadata::create_symlink(&restore_entry.path, target)?;
                stats.record_output_create_file(create_started.elapsed());
                if preserve_metadata {
                    pending_file_metadata.push(PendingMetadata::new(
                        restore_entry.path.clone(),
                        restore_entry.entry.clone(),
                    ));
                }
            }
            crate::ArchiveEntryKind::Directory => {}
        }
    }

    Ok(files)
}

fn build_restore_entries(root: &Path, manifest: ArchiveManifest) -> Result<Vec<RestoreEntry>> {
    manifest
        .entries()
        .iter()
        .map(|entry| {
            Ok(RestoreEntry {
                path: directory::join_safe(root, &entry.path)?,
                entry: entry.clone(),
            })
        })
        .collect()
}

fn ensure_direct_directory(
    root: &Path,
    path: &Path,
    created_directories: &mut HashSet<PathBuf>,
    stats: &mut DirectoryRestoreStats,
) -> Result<()> {
    if path.as_os_str().is_empty() || created_directories.contains(path) {
        return Ok(());
    }

    let started = Instant::now();
    fs::create_dir_all(path)?;
    stats.record_output_create_directory(started.elapsed());

    let mut current = Some(path);
    while let Some(directory) = current {
        if !directory.starts_with(root) {
            break;
        }
        if !created_directories.insert(directory.to_path_buf()) {
            break;
        }
        current = directory.parent();
    }

    Ok(())
}

fn build_block_extents(
    block_descriptors: &[ChunkDescriptor],
    files: &[DirectFileEntry],
    shard_count: usize,
) -> Result<Vec<Vec<DirectBlockExtent>>> {
    let mut by_offset = files.iter().collect::<Vec<_>>();
    by_offset.sort_by_key(|file| file.entry.content_offset);

    let mut block_extents = Vec::with_capacity(block_descriptors.len());
    let mut file_index = 0usize;
    let mut block_start = 0u64;
    for descriptor in block_descriptors {
        let block_len = descriptor.raw_len as u64;
        let block_end = block_start
            .checked_add(block_len)
            .ok_or(crate::OxideError::InvalidFormat("decoded block range overflow"))?;
        let mut extents = Vec::new();

        while file_index < by_offset.len()
            && by_offset[file_index]
                .entry
                .content_offset
                .saturating_add(by_offset[file_index].entry.size)
                <= block_start
        {
            file_index += 1;
        }

        let mut scan = file_index;
        while scan < by_offset.len() {
            let file = by_offset[scan];
            let file_start = file.entry.content_offset;
            let file_end = file_start
                .checked_add(file.entry.size)
                .ok_or(crate::OxideError::InvalidFormat("file content range overflow"))?;
            if file_start >= block_end {
                break;
            }

            let overlap_start = block_start.max(file_start);
            let overlap_end = block_end.min(file_end);
            if overlap_start < overlap_end {
                let len = usize::try_from(overlap_end - overlap_start).map_err(|_| {
                    crate::OxideError::InvalidFormat("direct write extent length exceeds usize range")
                })?;
                let block_offset = usize::try_from(overlap_start - block_start).map_err(|_| {
                    crate::OxideError::InvalidFormat("direct write block offset exceeds usize range")
                })?;
                extents.push(DirectBlockExtent {
                    file_id: file.id,
                    shard: file.id % shard_count.max(1),
                    path: Arc::clone(&file.path),
                    block_offset,
                    len,
                    file_offset: overlap_start - file_start,
                });
            }

            if file_end <= block_end {
                scan += 1;
            } else {
                break;
            }
        }

        block_extents.push(extents);
        block_start = block_end;
    }

    let expected_total = by_offset
        .iter()
        .map(|file| file.entry.size)
        .try_fold(0u64, |acc, size| {
            acc.checked_add(size)
                .ok_or(crate::OxideError::InvalidFormat("directory content size overflow"))
        })?;
    if block_start != expected_total {
        return Err(crate::OxideError::InvalidFormat(
            "directory block payload size does not match manifest file sizes",
        ));
    }

    Ok(block_extents)
}

/// Caps aggregate `File` handles held across all direct-write shards so extract stays
/// within typical `RLIMIT_NOFILE` soft limits (each shard keeps an LRU of open outputs).
fn direct_write_file_cache_max_open(write_shard_count: usize) -> usize {
    let shards = write_shard_count.max(1);
    #[cfg(unix)]
    let reserve = shards.saturating_mul(PARENT_DIR_LRU_CAP);
    #[cfg(not(unix))]
    let reserve = 0usize;
    let budget = fd_budget_for_direct_write_file_cache().saturating_sub(reserve);
    let per = budget / shards;
    per.clamp(1, DIRECT_FILE_CACHE_MAX_PER_SHARD)
}

#[cfg(unix)]
fn fd_budget_for_direct_write_file_cache() -> usize {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // Headroom for decode/read threads, the archive fd, stdio, mmap, etc.
    const RESERVED: usize = 128;
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } != 0 {
        return 512usize;
    }
    let soft = limit.rlim_cur;
    if soft == libc::RLIM_INFINITY {
        return 4096usize;
    }
    let Ok(soft) = usize::try_from(soft) else {
        return 512usize;
    };
    soft.saturating_sub(RESERVED).max(32)
}

#[cfg(not(unix))]
fn fd_budget_for_direct_write_file_cache() -> usize {
    512
}

fn run_direct_write_shard(
    task_rx: Receiver<DirectWriteTask>,
    file_cache_max_open: usize,
) -> Result<DirectShardOutcome> {
    let mut files = DirectFileCache::new(file_cache_max_open);
    let mut outcome = DirectShardOutcome::default();

    while let Ok(task) = task_rx.recv() {
        match task {
            DirectWriteTask::Write {
                file_id,
                path,
                chunk,
                range,
                file_offset,
            } => {
                let file = files.get(file_id, &path)?;
                let started = Instant::now();
                write_all_at(file, &chunk.as_ref()[range], file_offset)?;
                outcome.output_data += started.elapsed();
            }
        }
    }

    Ok(outcome)
}

struct DirectFileCache {
    max_open: usize,
    files: HashMap<usize, fs::File>,
    order: VecDeque<usize>,
    #[cfg(unix)]
    parent_dirs: ParentDirLru,
}

impl DirectFileCache {
    fn new(max_open: usize) -> Self {
        Self {
            max_open: max_open.max(1),
            files: HashMap::new(),
            order: VecDeque::new(),
            #[cfg(unix)]
            parent_dirs: ParentDirLru::new(PARENT_DIR_LRU_CAP),
        }
    }

    fn get(&mut self, file_id: usize, path: &Path) -> Result<&mut fs::File> {
        if self.files.contains_key(&file_id) {
            self.touch(file_id);
            return self.files.get_mut(&file_id).ok_or_else(|| {
                crate::OxideError::CompressionError("direct file cache lost open file".to_string())
            });
        }

        while self.files.len() >= self.max_open {
            let Some(evicted) = self.order.pop_front() else {
                break;
            };
            self.files.remove(&evicted);
        }

        #[cfg(unix)]
        let file = open_direct_file(path, &mut self.parent_dirs)?;
        #[cfg(not(unix))]
        let file = open_direct_file(path)?;
        self.files.insert(file_id, file);
        self.order.push_back(file_id);
        self.files.get_mut(&file_id).ok_or_else(|| {
            crate::OxideError::CompressionError("direct file cache failed to open file".to_string())
        })
    }

    fn touch(&mut self, file_id: usize) {
        if let Some(index) = self.order.iter().position(|&id| id == file_id) {
            self.order.remove(index);
        }
        self.order.push_back(file_id);
    }
}

/// Small LRU of directory fds for `openat` (evicted entries are closed on drop).
#[cfg(unix)]
#[derive(Debug)]
struct ParentDirLru {
    cap: usize,
    entries: VecDeque<(PathBuf, fs::File)>,
}

#[cfg(unix)]
impl ParentDirLru {
    fn new(cap: usize) -> Self {
        Self {
            cap: cap.max(1),
            entries: VecDeque::new(),
        }
    }

    fn open_child_file(&mut self, path: &Path) -> Result<fs::File> {
        use std::ffi::CString;
        use std::os::fd::{AsRawFd, FromRawFd};
        use std::os::unix::ffi::OsStrExt;

        let parent = path.parent().ok_or(crate::OxideError::InvalidFormat(
            "direct restore file path has no parent",
        ))?;
        let file_name = path.file_name().ok_or(crate::OxideError::InvalidFormat(
            "direct restore file path has no file name",
        ))?;
        let parent_buf = parent.to_path_buf();
        let file_name = CString::new(file_name.as_bytes()).map_err(|_| {
            crate::OxideError::InvalidFormat("direct restore file name contains nul byte")
        })?;

        if let Some(i) = self.entries.iter().position(|(p, _)| p == &parent_buf) {
            if i != self.entries.len().saturating_sub(1) {
                let entry = self.entries.remove(i).expect("index valid");
                self.entries.push_back(entry);
            }
            let parent_file = &self.entries.back().expect("non-empty").1;
            let fd = unsafe {
                libc::openat(
                    parent_file.as_raw_fd(),
                    file_name.as_ptr(),
                    libc::O_CREAT | libc::O_WRONLY | libc::O_CLOEXEC,
                    0o666,
                )
            };
            if fd < 0 {
                return Err(std::io::Error::last_os_error().into());
            }
            return Ok(unsafe { fs::File::from_raw_fd(fd) });
        }

        while self.entries.len() >= self.cap {
            self.entries.pop_front();
        }

        let parent_file = fs::File::open(parent)?;
        let fd = unsafe {
            libc::openat(
                parent_file.as_raw_fd(),
                file_name.as_ptr(),
                libc::O_CREAT | libc::O_WRONLY | libc::O_CLOEXEC,
                0o666,
            )
        };
        if fd < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        self.entries.push_back((parent_buf, parent_file));
        Ok(unsafe { fs::File::from_raw_fd(fd) })
    }
}

#[cfg(unix)]
fn open_direct_file(path: &Path, parents: &mut ParentDirLru) -> Result<fs::File> {
    parents.open_child_file(path)
}

#[cfg(not(unix))]
fn open_direct_file(path: &Path) -> Result<fs::File> {
    Ok(std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(path)?)
}

#[cfg(unix)]
fn write_all_at(file: &fs::File, mut bytes: &[u8], mut offset: u64) -> std::io::Result<()> {
    use std::io::{Error, ErrorKind};
    use std::os::unix::fs::FileExt;

    while !bytes.is_empty() {
        let written = file.write_at(bytes, offset)?;
        if written == 0 {
            return Err(Error::new(
                ErrorKind::WriteZero,
                "failed to write direct directory block",
            ));
        }
        bytes = &bytes[written..];
        offset = offset.saturating_add(written as u64);
    }
    Ok(())
}

#[cfg(not(unix))]
fn write_all_at(file: &mut fs::File, bytes: &[u8], offset: u64) -> std::io::Result<()> {
    use std::io::{Seek, SeekFrom, Write};

    file.seek(SeekFrom::Start(offset))?;
    file.write_all(bytes)
}
