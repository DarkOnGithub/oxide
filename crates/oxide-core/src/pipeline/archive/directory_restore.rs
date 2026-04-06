use std::cmp::Reverse;
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::fmt;
use std::fs;
use std::io::{BufWriter, Write};
use std::num::NonZeroUsize;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::anyhow;
use crossbeam_channel::{Receiver, Sender, TryRecvError, TrySendError, bounded, never};
use regex::RegexSet;

use crate::OxideError;
use crate::format::ArchiveManifest;
use crate::types::Result;

use super::super::directory;
use super::reorder_writer::OrderedChunkWriter;

pub(crate) const OUTPUT_BUFFER_CAPACITY: usize = 1024 * 1024;
const MEDIUM_OUTPUT_BUFFER_CAPACITY: usize = 256 * 1024;
const SMALL_OUTPUT_BUFFER_CAPACITY: usize = 64 * 1024;
const PARALLEL_METADATA_MIN_ITEMS: usize = 8;
const MAX_METADATA_WORKERS: usize = 8;
const MAX_PREPARED_ENTRY_WORKERS: usize = 8;
const PREPARED_ENTRY_QUEUE_CAPACITY: usize = 256;
const BASE_PREOPENED_FILE_WINDOW_MULTIPLIER: usize = 16;
const PREOPENED_FILE_WINDOW_BONUS_STEP: usize = 4;
const MIN_PREOPENED_FILE_WINDOW_CAPACITY: usize = 64;
const MAX_PREOPENED_FILE_WINDOW_CAPACITY: usize = 512;
const MIN_READY_ENTRY_WINDOW_CAPACITY: usize = 32;
const MAX_READY_ENTRY_WINDOW_CAPACITY: usize = 128;
const DIRECT_FILE_WRITE_MAX_BYTES: u64 = 32 * 1024;
const SMALL_FILE_BUFFER_MAX_BYTES: u64 = 256 * 1024;
const MEDIUM_FILE_BUFFER_MAX_BYTES: u64 = 1024 * 1024;

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct DirectoryRestoreStats {
    pub(crate) directory_decode: Duration,
    pub(crate) ordered_write_time: Duration,
    pub(crate) prepared_file_open: Duration,
    pub(crate) prepared_file_permit_wait: Duration,
    pub(crate) prepared_file_wait: Duration,
    pub(crate) output_prepare_directories: Duration,
    pub(crate) output_write: Duration,
    pub(crate) output_create: Duration,
    pub(crate) output_create_directories: Duration,
    pub(crate) output_create_files: Duration,
    pub(crate) output_data: Duration,
    pub(crate) output_flush: Duration,
    pub(crate) output_metadata: Duration,
    pub(crate) output_metadata_files: Duration,
    pub(crate) output_metadata_directories: Duration,
    pub(crate) output_bytes_total: u64,
    pub(crate) entry_count: u64,
}

impl DirectoryRestoreStats {
    fn record_prepared_file_open(&mut self, elapsed: Duration) {
        self.prepared_file_open += elapsed;
    }

    fn record_prepared_file_permit_wait(&mut self, elapsed: Duration) {
        self.prepared_file_permit_wait += elapsed;
    }

    fn record_prepared_file_wait(&mut self, elapsed: Duration) {
        self.prepared_file_wait += elapsed;
    }

    fn record_output_prepare_directories(&mut self, elapsed: Duration) {
        self.output_prepare_directories += elapsed;
    }

    fn record_output_create_directory(&mut self, elapsed: Duration) {
        self.output_write += elapsed;
        self.output_create += elapsed;
        self.output_create_directories += elapsed;
    }

    fn record_output_create_file(&mut self, elapsed: Duration) {
        self.output_write += elapsed;
        self.output_create += elapsed;
        self.output_create_files += elapsed;
    }

    fn record_output_data(&mut self, elapsed: Duration) {
        self.output_write += elapsed;
        self.output_data += elapsed;
    }

    fn record_output_flush(&mut self, elapsed: Duration) {
        self.output_write += elapsed;
        self.output_flush += elapsed;
    }

    fn record_output_metadata_file(&mut self, elapsed: Duration) {
        self.output_write += elapsed;
        self.output_metadata += elapsed;
        self.output_metadata_files += elapsed;
    }

    fn record_output_metadata_directory(&mut self, elapsed: Duration) {
        self.output_write += elapsed;
        self.output_metadata += elapsed;
        self.output_metadata_directories += elapsed;
    }
}

#[derive(Debug)]
struct RestoreEntry {
    path: PathBuf,
    entry: crate::ArchiveListingEntry,
}

#[derive(Debug)]
struct PreparedRestoreWorkItem {
    index: usize,
    restore_entry: RestoreEntry,
    reserved_open_permit: Option<ReservedOpenFilePermit>,
    permit_wait_started: Option<Instant>,
}

#[derive(Debug)]
struct ReservedOpenFilePermit {
    permit: OpenFilePermit,
    wait_elapsed: Duration,
}

#[derive(Debug)]
enum PreparedRestoreEntry {
    Directory {
        path: PathBuf,
        entry: crate::ArchiveListingEntry,
    },
    File {
        path: PathBuf,
        entry: crate::ArchiveListingEntry,
    },
    FileReady {
        path: PathBuf,
        entry: crate::ArchiveListingEntry,
        writer: PendingFileWriter,
        permit: OpenFilePermit,
        permit_wait_elapsed: Duration,
        open_elapsed: Duration,
    },
    Symlink {
        path: PathBuf,
        entry: crate::ArchiveListingEntry,
        create_elapsed: Duration,
    },
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
                    selected_ranges.push(entry.content_range());
                }
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

    pub(crate) fn selected_ranges(&self) -> &[Range<u64>] {
        &self.selected_ranges
    }

    pub(crate) fn into_manifest(self) -> ArchiveManifest {
        self.manifest
    }
}

#[derive(Debug)]
struct PendingFile {
    remaining: u64,
    path: PathBuf,
    entry: crate::ArchiveListingEntry,
    writer: PendingFileWriter,
    permit: OpenFilePermit,
}

struct OpenFilePermit {
    release_tx: Sender<()>,
}

impl fmt::Debug for OpenFilePermit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpenFilePermit").finish_non_exhaustive()
    }
}

impl Drop for OpenFilePermit {
    fn drop(&mut self) {
        let _ = self.release_tx.send(());
    }
}

#[derive(Debug)]
enum PendingFileWriter {
    Direct(fs::File),
    Buffered(BufWriter<fs::File>),
}

impl PendingFileWriter {
    fn new(file: fs::File, entry_size: u64) -> Self {
        match output_buffer_capacity(entry_size) {
            Some(capacity) => Self::Buffered(BufWriter::with_capacity(capacity, file)),
            None => Self::Direct(file),
        }
    }

    fn write_all(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        match self {
            Self::Direct(file) => file.write_all(bytes),
            Self::Buffered(writer) => writer.write_all(bytes),
        }
    }

    fn into_inner(self) -> std::io::Result<fs::File> {
        match self {
            Self::Direct(file) => Ok(file),
            Self::Buffered(writer) => writer.into_inner().map_err(|error| error.into_error()),
        }
    }
}

#[derive(Debug)]
struct PendingMetadata {
    path: PathBuf,
    entry: crate::ArchiveListingEntry,
    depth: usize,
}

#[derive(Debug)]
pub(crate) struct DirectoryRestoreWriter {
    root: PathBuf,
    entry_count_total: usize,
    next_entry: usize,
    pending_file: Option<PendingFile>,
    ready_entries: VecDeque<PreparedRestoreEntry>,
    created_directories: HashSet<PathBuf>,
    pending_file_metadata: Vec<PendingMetadata>,
    pending_directory_metadata: Vec<PendingMetadata>,
    ready_entry_window_capacity: usize,
    prepared_entry_rx: Receiver<Result<PreparedRestoreEntry>>,
    prepared_entry_handle: Option<thread::JoinHandle<Result<()>>>,
    stats: DirectoryRestoreStats,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PreparedEntryWindowConfig {
    preopened_file_window_capacity: usize,
    ready_entry_window_capacity: usize,
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
        let entries = build_restore_entries(root, manifest)?;
        let window_config = prepared_entry_window_config(&entries);
        let mut writer = Self {
            root: root.to_path_buf(),
            entry_count_total: entries.len(),
            next_entry: 0,
            pending_file: None,
            ready_entries: VecDeque::with_capacity(window_config.ready_entry_window_capacity),
            created_directories: HashSet::new(),
            pending_file_metadata: Vec::new(),
            pending_directory_metadata: Vec::new(),
            ready_entry_window_capacity: window_config.ready_entry_window_capacity,
            prepared_entry_rx: never(),
            prepared_entry_handle: None,
            stats: DirectoryRestoreStats::default(),
        };
        writer.ensure_directory_exists(root)?;
        writer.prepare_output_directories(&entries)?;
        let (prepared_entry_rx, prepared_entry_handle) =
            spawn_prepared_restore_entries(entries, window_config);
        writer.prepared_entry_rx = prepared_entry_rx;
        writer.prepared_entry_handle = Some(prepared_entry_handle);
        Ok(writer)
    }

    pub(crate) fn finish(&mut self) -> Result<DirectoryRestoreStats> {
        self.advance_entries()?;
        if let Some(pending) = self.pending_file.as_ref()
            && pending.remaining > 0
        {
            return Err(crate::OxideError::InvalidFormat(
                "truncated file payload during directory restore",
            ));
        }
        if self.pending_file.is_some() || self.next_entry != self.entry_count_total {
            return Err(crate::OxideError::InvalidFormat(
                "directory restore ended before all entries completed",
            ));
        }
        self.finalize_file_metadata()?;
        self.finalize_directories()?;
        self.join_prepared_entries()?;
        Ok(self.stats)
    }

    fn ensure_directory_exists(&mut self, path: &Path) -> Result<()> {
        if path.as_os_str().is_empty() || self.created_directories.contains(path) {
            return Ok(());
        }

        let started = Instant::now();
        fs::create_dir_all(path)?;
        self.stats.record_output_create_directory(started.elapsed());

        let mut current = Some(path);
        while let Some(directory) = current {
            if !directory.starts_with(&self.root) {
                break;
            }
            if !self.created_directories.insert(directory.to_path_buf()) {
                break;
            }
            current = directory.parent();
        }

        Ok(())
    }

    fn defer_file_metadata(&mut self, path: PathBuf, entry: crate::ArchiveListingEntry) {
        self.pending_file_metadata.push(PendingMetadata {
            depth: path_depth(&path),
            path,
            entry,
        });
    }

    fn defer_directory_metadata(&mut self, path: PathBuf, entry: crate::ArchiveListingEntry) {
        self.pending_directory_metadata.push(PendingMetadata {
            depth: path_depth(&path),
            path,
            entry,
        });
    }

    fn prepare_output_directories(&mut self, entries: &[RestoreEntry]) -> Result<()> {
        let started = Instant::now();
        let directories = entries
            .iter()
            .filter_map(|restore_entry| match restore_entry.entry.kind {
                crate::ArchiveEntryKind::Directory => Some(restore_entry.path.clone()),
                crate::ArchiveEntryKind::File | crate::ArchiveEntryKind::Symlink => restore_entry
                    .path
                    .parent()
                    .filter(|path| !path.as_os_str().is_empty())
                    .map(Path::to_path_buf),
            })
            .collect::<Vec<_>>();

        for directory in directories {
            self.ensure_directory_exists(&directory)?;
        }

        self.stats
            .record_output_prepare_directories(started.elapsed());
        Ok(())
    }

    fn advance_entries(&mut self) -> Result<()> {
        self.fill_ready_entries()?;

        while self.pending_file.is_none() && self.next_entry < self.entry_count_total {
            let prepared_entry = self.next_ready_entry()?;
            self.next_entry += 1;
            self.stats.entry_count = self.stats.entry_count.saturating_add(1);

            match prepared_entry {
                PreparedRestoreEntry::Directory { path, entry } => {
                    self.defer_directory_metadata(path, entry);
                }
                PreparedRestoreEntry::File { .. } => {
                    return Err(crate::OxideError::CompressionError(
                        "prepared file entry reached writer before in-order file open".to_string(),
                    ));
                }
                PreparedRestoreEntry::FileReady {
                    path,
                    entry,
                    writer,
                    permit,
                    permit_wait_elapsed,
                    open_elapsed,
                } => {
                    self.stats
                        .record_prepared_file_permit_wait(permit_wait_elapsed);
                    self.stats.record_prepared_file_open(open_elapsed);
                    if entry.size == 0 {
                        let started = Instant::now();
                        let _file = writer.into_inner()?;
                        self.stats.record_output_flush(started.elapsed());
                        self.defer_file_metadata(path, entry);
                        continue;
                    }

                    self.pending_file = Some(PendingFile {
                        remaining: entry.size,
                        path,
                        entry,
                        writer,
                        permit,
                    });
                }
                PreparedRestoreEntry::Symlink {
                    path,
                    entry,
                    create_elapsed,
                } => {
                    self.stats.record_output_create_file(create_elapsed);
                    self.defer_file_metadata(path, entry);
                }
            }

            self.fill_ready_entries()?;
        }

        Ok(())
    }

    fn fill_ready_entries(&mut self) -> Result<()> {
        while self.ready_entries.len() < self.ready_entry_window_capacity
            && self.next_entry + self.ready_entries.len() < self.entry_count_total
        {
            match self.prepared_entry_rx.try_recv() {
                Ok(prepared) => self.ready_entries.push_back(prepared?),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    return Err(crate::OxideError::CompressionError(
                        "prepared entry queue closed before completion".to_string(),
                    ));
                }
            }
        }

        Ok(())
    }

    fn next_ready_entry(&mut self) -> Result<PreparedRestoreEntry> {
        if let Some(prepared_entry) = self.ready_entries.pop_front() {
            return Ok(prepared_entry);
        }

        let wait_started = Instant::now();
        let prepared_entry = self.prepared_entry_rx.recv().map_err(|_| {
            crate::OxideError::CompressionError(
                "prepared entry queue closed before completion".to_string(),
            )
        })?;
        self.stats.record_prepared_file_wait(wait_started.elapsed());
        prepared_entry
    }

    fn join_prepared_entries(&mut self) -> Result<()> {
        let Some(handle) = self.prepared_entry_handle.take() else {
            return Ok(());
        };

        handle.join().map_err(|payload| {
            let details = if let Some(message) = payload.downcast_ref::<&str>() {
                (*message).to_string()
            } else if let Some(message) = payload.downcast_ref::<String>() {
                message.clone()
            } else {
                "unknown panic payload".to_string()
            };
            crate::OxideError::CompressionError(format!(
                "prepared entry worker thread panicked: {details}"
            ))
        })?
    }

    fn finalize_pending_file(&mut self) -> Result<()> {
        let pending = self.pending_file.take().ok_or(OxideError::InvalidFormat(
            "directory restore missing pending file after write completion",
        ))?;
        let PendingFile {
            path,
            entry,
            writer,
            permit,
            ..
        } = pending;
        let started = Instant::now();
        let _file = writer.into_inner()?;
        self.stats.record_output_flush(started.elapsed());
        drop(permit);
        self.defer_file_metadata(path, entry);
        Ok(())
    }

    fn finalize_file_metadata(&mut self) -> Result<()> {
        let pending = self.pending_file_metadata.drain(..).collect::<Vec<_>>();
        if pending.is_empty() {
            return Ok(());
        }

        let started = Instant::now();
        apply_pending_metadata_in_parallel(&pending)?;
        self.stats.record_output_metadata_file(started.elapsed());
        Ok(())
    }

    fn finalize_directories(&mut self) -> Result<()> {
        if self.pending_directory_metadata.is_empty() {
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

        self.pending_directory_metadata.clear();
        self.stats
            .record_output_metadata_directory(started.elapsed());
        Ok(())
    }
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

fn path_depth(path: &Path) -> usize {
    path.components().count()
}

fn output_buffer_capacity(entry_size: u64) -> Option<usize> {
    if entry_size <= DIRECT_FILE_WRITE_MAX_BYTES {
        return None;
    }

    if entry_size <= SMALL_FILE_BUFFER_MAX_BYTES {
        return Some(
            (entry_size.min(usize::MAX as u64) as usize).min(SMALL_OUTPUT_BUFFER_CAPACITY),
        );
    }

    if entry_size <= MEDIUM_FILE_BUFFER_MAX_BYTES {
        return Some(MEDIUM_OUTPUT_BUFFER_CAPACITY);
    }

    Some(OUTPUT_BUFFER_CAPACITY)
}

fn available_prepared_entry_workers(item_count: usize) -> usize {
    thread::available_parallelism()
        .unwrap_or(NonZeroUsize::MIN)
        .get()
        .min(MAX_PREPARED_ENTRY_WORKERS)
        .min(item_count)
        .max(1)
}

fn prepared_entry_window_config(entries: &[RestoreEntry]) -> PreparedEntryWindowConfig {
    let entry_count = entries.len().max(1);
    let file_count = entries
        .iter()
        .filter(|restore_entry| matches!(restore_entry.entry.kind, crate::ArchiveEntryKind::File))
        .count()
        .max(1);
    let parallelism = thread::available_parallelism()
        .unwrap_or(NonZeroUsize::MIN)
        .get();

    prepared_entry_window_config_for_counts(entry_count, file_count, parallelism)
}

fn prepared_entry_window_config_for_counts(
    entry_count: usize,
    file_count: usize,
    parallelism: usize,
) -> PreparedEntryWindowConfig {
    let entry_count = entry_count.max(1);
    let file_count = file_count.max(1);
    let parallelism = parallelism.max(1);

    let ready_entry_window_capacity = parallelism
        .saturating_mul(8)
        .clamp(
            MIN_READY_ENTRY_WINDOW_CAPACITY,
            MAX_READY_ENTRY_WINDOW_CAPACITY,
        )
        .min(entry_count);
    let preopened_file_window_capacity =
        adaptive_preopened_file_window_capacity(entry_count, file_count, parallelism);

    PreparedEntryWindowConfig {
        preopened_file_window_capacity,
        ready_entry_window_capacity,
    }
}

fn adaptive_preopened_file_window_capacity(
    entry_count: usize,
    file_count: usize,
    parallelism: usize,
) -> usize {
    let entry_count = entry_count.max(1);
    let file_count = file_count.max(1);
    let parallelism = parallelism.max(1);

    let base_capacity = parallelism.saturating_mul(BASE_PREOPENED_FILE_WINDOW_MULTIPLIER);
    let file_density_per_thousand = file_count.saturating_mul(1000).div_ceil(entry_count);
    let files_per_worker = file_count.div_ceil(parallelism);

    // Only boost the file-open window when the workload is both file-heavy and
    // deep enough per worker to keep additional pre-opened files useful. That
    // avoids inflating FD pressure for sparse or short manifests while still
    // widening the window on large file-dense restores.
    let density_bonus_scale = match file_density_per_thousand {
        850.. => 4,
        600.. => 3,
        350.. => 2,
        150.. => 1,
        _ => 0,
    };
    let backlog_bonus_scale = match files_per_worker {
        128.. => 4,
        64.. => 3,
        32.. => 2,
        16.. => 1,
        _ => 0,
    };
    let bonus_scale = density_bonus_scale.min(backlog_bonus_scale);
    let adaptive_bonus = parallelism
        .saturating_mul(PREOPENED_FILE_WINDOW_BONUS_STEP)
        .saturating_mul(bonus_scale);

    base_capacity
        .saturating_add(adaptive_bonus)
        .clamp(
            MIN_PREOPENED_FILE_WINDOW_CAPACITY,
            MAX_PREOPENED_FILE_WINDOW_CAPACITY,
        )
        .min(file_count)
}

fn spawn_prepared_restore_entries(
    entries: Vec<RestoreEntry>,
    window_config: PreparedEntryWindowConfig,
) -> (
    Receiver<Result<PreparedRestoreEntry>>,
    thread::JoinHandle<Result<()>>,
) {
    let (tx, rx) = bounded::<Result<PreparedRestoreEntry>>(PREPARED_ENTRY_QUEUE_CAPACITY);
    let handle = thread::spawn(move || -> Result<()> {
        let (open_file_permit_tx, open_file_permit_rx) =
            bounded::<()>(window_config.preopened_file_window_capacity);
        for _ in 0..window_config.preopened_file_window_capacity {
            open_file_permit_tx.send(()).map_err(|_| {
                crate::OxideError::CompressionError(
                    "pre-opened file permit queue closed during initialization".to_string(),
                )
            })?;
        }

        let total_entries = entries.len();
        let worker_count = available_prepared_entry_workers(total_entries);
        if worker_count <= 1 {
            for restore_entry in entries {
                let prepared = prepare_restore_entry_with_open(
                    restore_entry,
                    &open_file_permit_rx,
                    &open_file_permit_tx,
                );
                let stop_after_send = prepared.is_err();
                if tx.send(prepared).is_err() {
                    return Ok(());
                }
                if stop_after_send {
                    return Ok(());
                }
            }

            return Ok(());
        }

        let cancelled = Arc::new(AtomicBool::new(false));
        let (work_tx, work_rx) = bounded::<PreparedRestoreWorkItem>(PREPARED_ENTRY_QUEUE_CAPACITY);
        let (result_tx, result_rx) =
            bounded::<(usize, Result<PreparedRestoreEntry>)>(PREPARED_ENTRY_QUEUE_CAPACITY);

        let mut worker_handles = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            let work_rx = work_rx.clone();
            let result_tx = result_tx.clone();
            let cancelled = Arc::clone(&cancelled);
            let open_file_permit_rx = open_file_permit_rx.clone();
            let open_file_permit_tx = open_file_permit_tx.clone();
            worker_handles.push(thread::spawn(move || -> Result<()> {
                while !cancelled.load(Ordering::Relaxed) {
                    let Ok(work_item) = work_rx.recv() else {
                        return Ok(());
                    };
                    if cancelled.load(Ordering::Relaxed) {
                        return Ok(());
                    }

                    let index = work_item.index;
                    let prepared = prepare_restore_work_item_with_open(
                        work_item,
                        &open_file_permit_rx,
                        &open_file_permit_tx,
                    );
                    if prepared.is_err() {
                        cancelled.store(true, Ordering::Relaxed);
                    }
                    if result_tx.send((index, prepared)).is_err() {
                        return Ok(());
                    }
                }

                Ok(())
            }));
        }
        drop(result_tx);

        let mut pending_work = entries
            .into_iter()
            .enumerate()
            .map(|(index, restore_entry)| PreparedRestoreWorkItem {
                index,
                restore_entry,
                reserved_open_permit: None,
                permit_wait_started: None,
            })
            .collect::<VecDeque<_>>();

        let mut next_index = 0usize;
        let mut pending: BTreeMap<usize, Result<PreparedRestoreEntry>> = BTreeMap::new();
        let mut in_flight = 0usize;
        let mut sent_error = false;
        while next_index < total_entries && !sent_error {
            while let Some(prepared) = pending.remove(&next_index) {
                sent_error = prepared.is_err();
                if tx.send(prepared).is_err() {
                    cancelled.store(true, Ordering::Relaxed);
                    sent_error = true;
                    break;
                }
                next_index += 1;
                if sent_error {
                    break;
                }
            }

            if next_index >= total_entries || sent_error {
                break;
            }

            while let Some(work_item) = pending_work.pop_front() {
                if cancelled.load(Ordering::Relaxed) {
                    break;
                }

                let mut work_item = work_item;
                if matches!(
                    work_item.restore_entry.entry.kind,
                    crate::ArchiveEntryKind::File
                ) && work_item.reserved_open_permit.is_none()
                {
                    match try_reserve_open_file_permit_for_work_item(
                        &mut work_item,
                        &open_file_permit_rx,
                        &open_file_permit_tx,
                    ) {
                        Ok(true) => {}
                        Ok(false) => {
                            if in_flight == 0 {
                                if let Err(error) = reserve_open_file_permit_for_work_item(
                                    &mut work_item,
                                    &open_file_permit_rx,
                                    &open_file_permit_tx,
                                ) {
                                    cancelled.store(true, Ordering::Relaxed);
                                    sent_error = true;
                                    if tx.send(Err(error)).is_err() {
                                        return Ok(());
                                    }
                                    break;
                                }
                            } else {
                                pending_work.push_front(work_item);
                                break;
                            }
                        }
                        Err(error) => {
                            cancelled.store(true, Ordering::Relaxed);
                            sent_error = true;
                            if tx.send(Err(error)).is_err() {
                                return Ok(());
                            }
                            break;
                        }
                    }
                }

                match work_tx.try_send(work_item) {
                    Ok(()) => {
                        in_flight += 1;
                    }
                    Err(TrySendError::Full(work_item)) => {
                        pending_work.push_front(work_item);
                        break;
                    }
                    Err(TrySendError::Disconnected(_work_item)) => {
                        cancelled.store(true, Ordering::Relaxed);
                        sent_error = true;
                        break;
                    }
                }
            }

            while let Some(prepared) = pending.remove(&next_index) {
                sent_error = prepared.is_err();
                if tx.send(prepared).is_err() {
                    cancelled.store(true, Ordering::Relaxed);
                    sent_error = true;
                    break;
                }
                next_index += 1;
                if sent_error {
                    break;
                }
            }

            if next_index >= total_entries || sent_error {
                break;
            }

            let (index, prepared) = result_rx.recv().map_err(|_| {
                crate::OxideError::CompressionError(
                    "prepared entry workers exited before completion".to_string(),
                )
            })?;
            in_flight = in_flight.saturating_sub(1);
            pending.insert(index, prepared);
        }

        drop(work_tx);
        cancelled.store(true, Ordering::Relaxed);
        drop(pending_work);
        drop(pending);
        drop(result_rx);

        for handle in worker_handles {
            handle.join().map_err(|payload| {
                let details = if let Some(message) = payload.downcast_ref::<&str>() {
                    (*message).to_string()
                } else if let Some(message) = payload.downcast_ref::<String>() {
                    message.clone()
                } else {
                    "unknown panic payload".to_string()
                };
                crate::OxideError::CompressionError(format!(
                    "prepared entry worker thread panicked: {details}"
                ))
            })??;
        }

        Ok(())
    });

    (rx, handle)
}

fn prepare_restore_entry(restore_entry: RestoreEntry) -> Result<PreparedRestoreEntry> {
    let RestoreEntry { path, entry } = restore_entry;
    match entry.kind {
        crate::ArchiveEntryKind::Directory => Ok(PreparedRestoreEntry::Directory { path, entry }),
        crate::ArchiveEntryKind::File => Ok(PreparedRestoreEntry::File { path, entry }),
        crate::ArchiveEntryKind::Symlink => {
            let target = entry.target.as_deref().ok_or(OxideError::InvalidFormat(
                "symlink manifest entry missing target",
            ))?;
            let create_started = Instant::now();
            create_symlink(&path, target)?;
            Ok(PreparedRestoreEntry::Symlink {
                path,
                entry,
                create_elapsed: create_started.elapsed(),
            })
        }
    }
}

fn prepare_restore_entry_with_open(
    restore_entry: RestoreEntry,
    open_file_permit_rx: &Receiver<()>,
    open_file_permit_tx: &Sender<()>,
) -> Result<PreparedRestoreEntry> {
    prepare_prepared_entry_with_open(
        prepare_restore_entry(restore_entry),
        None,
        open_file_permit_rx,
        open_file_permit_tx,
    )
}

fn prepare_restore_work_item_with_open(
    work_item: PreparedRestoreWorkItem,
    open_file_permit_rx: &Receiver<()>,
    open_file_permit_tx: &Sender<()>,
) -> Result<PreparedRestoreEntry> {
    let PreparedRestoreWorkItem {
        restore_entry,
        reserved_open_permit,
        ..
    } = work_item;
    prepare_prepared_entry_with_open(
        prepare_restore_entry(restore_entry),
        reserved_open_permit,
        open_file_permit_rx,
        open_file_permit_tx,
    )
}

fn prepare_prepared_entry_with_open(
    prepared: Result<PreparedRestoreEntry>,
    reserved_open_permit: Option<ReservedOpenFilePermit>,
    open_file_permit_rx: &Receiver<()>,
    open_file_permit_tx: &Sender<()>,
) -> Result<PreparedRestoreEntry> {
    let prepared = prepared?;
    match prepared {
        PreparedRestoreEntry::File { path, entry } => {
            let (permit, permit_wait_elapsed) = match reserved_open_permit {
                Some(reserved) => (reserved.permit, reserved.wait_elapsed),
                None => {
                    let permit_wait_started = Instant::now();
                    let permit =
                        acquire_open_file_permit(open_file_permit_rx, open_file_permit_tx)?;
                    (permit, permit_wait_started.elapsed())
                }
            };
            let open_started = Instant::now();
            let file = fs::File::create(&path)?;
            let writer = PendingFileWriter::new(file, entry.size);
            Ok(PreparedRestoreEntry::FileReady {
                path,
                entry,
                writer,
                permit,
                permit_wait_elapsed,
                open_elapsed: open_started.elapsed(),
            })
        }
        other => Ok(other),
    }
}

fn try_reserve_open_file_permit_for_work_item(
    work_item: &mut PreparedRestoreWorkItem,
    open_file_permit_rx: &Receiver<()>,
    open_file_permit_tx: &Sender<()>,
) -> Result<bool> {
    debug_assert!(matches!(
        work_item.restore_entry.entry.kind,
        crate::ArchiveEntryKind::File
    ));

    let wait_started = work_item
        .permit_wait_started
        .get_or_insert_with(Instant::now);
    match open_file_permit_rx.try_recv() {
        Ok(()) => {
            work_item.reserved_open_permit = Some(ReservedOpenFilePermit {
                permit: OpenFilePermit {
                    release_tx: open_file_permit_tx.clone(),
                },
                wait_elapsed: wait_started.elapsed(),
            });
            work_item.permit_wait_started = None;
            Ok(true)
        }
        Err(TryRecvError::Empty) => Ok(false),
        Err(TryRecvError::Disconnected) => Err(crate::OxideError::CompressionError(
            "pre-opened file permit queue closed before completion".to_string(),
        )),
    }
}

fn reserve_open_file_permit_for_work_item(
    work_item: &mut PreparedRestoreWorkItem,
    open_file_permit_rx: &Receiver<()>,
    open_file_permit_tx: &Sender<()>,
) -> Result<()> {
    debug_assert!(matches!(
        work_item.restore_entry.entry.kind,
        crate::ArchiveEntryKind::File
    ));

    if work_item.reserved_open_permit.is_some() {
        return Ok(());
    }

    let wait_started = work_item
        .permit_wait_started
        .get_or_insert_with(Instant::now);
    let permit = acquire_open_file_permit(open_file_permit_rx, open_file_permit_tx)?;
    work_item.reserved_open_permit = Some(ReservedOpenFilePermit {
        permit,
        wait_elapsed: wait_started.elapsed(),
    });
    work_item.permit_wait_started = None;
    Ok(())
}

fn acquire_open_file_permit(
    open_file_permit_rx: &Receiver<()>,
    open_file_permit_tx: &Sender<()>,
) -> Result<OpenFilePermit> {
    open_file_permit_rx.recv().map_err(|_| {
        crate::OxideError::CompressionError(
            "pre-opened file permit queue closed before completion".to_string(),
        )
    })?;
    Ok(OpenFilePermit {
        release_tx: open_file_permit_tx.clone(),
    })
}

fn apply_pending_metadata_in_parallel(entries: &[PendingMetadata]) -> Result<()> {
    if entries.len() < PARALLEL_METADATA_MIN_ITEMS {
        for entry in entries {
            apply_entry_metadata(&entry.path, &entry.entry)?;
        }
        return Ok(());
    }

    let worker_count = available_metadata_workers(entries.len());
    if worker_count <= 1 {
        for entry in entries {
            apply_entry_metadata(&entry.path, &entry.entry)?;
        }
        return Ok(());
    }

    let chunk_size = entries.len().div_ceil(worker_count);
    thread::scope(|scope| -> Result<()> {
        let mut handles = Vec::new();
        for chunk in entries.chunks(chunk_size) {
            handles.push(scope.spawn(move || -> Result<()> {
                for entry in chunk {
                    apply_entry_metadata(&entry.path, &entry.entry)?;
                }
                Ok(())
            }));
        }

        for handle in handles {
            match handle.join() {
                Ok(result) => result?,
                Err(payload) => {
                    let details = if let Some(message) = payload.downcast_ref::<&str>() {
                        (*message).to_string()
                    } else if let Some(message) = payload.downcast_ref::<String>() {
                        message.clone()
                    } else {
                        "unknown panic payload".to_string()
                    };
                    return Err(crate::OxideError::CompressionError(format!(
                        "metadata worker thread panicked: {details}"
                    )));
                }
            }
        }

        Ok(())
    })
}

fn available_metadata_workers(item_count: usize) -> usize {
    thread::available_parallelism()
        .unwrap_or(NonZeroUsize::MIN)
        .get()
        .min(MAX_METADATA_WORKERS)
        .min(item_count)
        .max(1)
}

impl FilteredDirectoryRestoreWriter {
    pub(crate) fn create(
        root: &Path,
        manifest: ArchiveManifest,
        selected_ranges: Vec<Range<u64>>,
    ) -> Result<Self> {
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
        let ordered_write_started = Instant::now();
        let decode_started = Instant::now();
        self.advance_entries()?;
        self.stats.directory_decode += decode_started.elapsed();

        while !bytes.is_empty() {
            let (take, finished_file) = {
                let pending = self.pending_file.as_mut().ok_or(OxideError::InvalidFormat(
                    "directory archive contains file data beyond manifest entries",
                ))?;

                let take = bytes.len().min(pending.remaining as usize);
                pending.remaining -= take as u64;
                let started = Instant::now();
                pending.writer.write_all(&bytes[..take])?;
                self.stats.record_output_data(started.elapsed());
                (take, pending.remaining == 0)
            };

            self.stats.output_bytes_total =
                self.stats.output_bytes_total.saturating_add(take as u64);
            bytes = &bytes[take..];
            self.fill_ready_entries()?;

            if finished_file {
                self.finalize_pending_file()?;
                let decode_started = Instant::now();
                self.advance_entries()?;
                self.stats.directory_decode += decode_started.elapsed();
            }
        }

        self.stats.ordered_write_time += ordered_write_started.elapsed();

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

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::{ArchiveListingEntry, ArchiveTimestamp};

    fn file_work_item(index: usize, path: PathBuf) -> PreparedRestoreWorkItem {
        PreparedRestoreWorkItem {
            index,
            restore_entry: RestoreEntry {
                path,
                entry: ArchiveListingEntry::file(
                    format!("file-{index}.bin"),
                    8,
                    0o644,
                    ArchiveTimestamp::default(),
                    0,
                    0,
                    0,
                ),
            },
            reserved_open_permit: None,
            permit_wait_started: None,
        }
    }

    #[test]
    fn prepared_entry_windows_scale_with_parallelism() {
        let config = prepared_entry_window_config_for_counts(10_000, 10_000, 12);

        assert_eq!(
            config,
            PreparedEntryWindowConfig {
                preopened_file_window_capacity: 384,
                ready_entry_window_capacity: 96,
            }
        );
    }

    #[test]
    fn prepared_entry_windows_adapt_for_file_heavy_workloads() {
        let config = prepared_entry_window_config_for_counts(10_000, 5_000, 16);

        assert_eq!(
            config,
            PreparedEntryWindowConfig {
                preopened_file_window_capacity: 384,
                ready_entry_window_capacity: 128,
            }
        );
    }

    #[test]
    fn prepared_entry_windows_do_not_boost_sparse_file_workloads() {
        let config = prepared_entry_window_config_for_counts(10_000, 1_000, 16);

        assert_eq!(
            config,
            PreparedEntryWindowConfig {
                preopened_file_window_capacity: 256,
                ready_entry_window_capacity: 128,
            }
        );
    }

    #[test]
    fn prepared_entry_windows_only_boost_when_density_and_backlog_align() {
        let config = prepared_entry_window_config_for_counts(1_000, 240, 16);

        assert_eq!(
            config,
            PreparedEntryWindowConfig {
                preopened_file_window_capacity: 240,
                ready_entry_window_capacity: 128,
            }
        );
    }

    #[test]
    fn prepared_entry_windows_respect_safe_upper_clamp() {
        let config = prepared_entry_window_config_for_counts(10_000, 10_000, 64);

        assert_eq!(
            config,
            PreparedEntryWindowConfig {
                preopened_file_window_capacity: MAX_PREOPENED_FILE_WINDOW_CAPACITY,
                ready_entry_window_capacity: MAX_READY_ENTRY_WINDOW_CAPACITY,
            }
        );
    }

    #[test]
    fn prepared_entry_windows_are_capped_by_workload() {
        let config = prepared_entry_window_config_for_counts(12, 5, 32);

        assert_eq!(
            config,
            PreparedEntryWindowConfig {
                preopened_file_window_capacity: 5,
                ready_entry_window_capacity: 12,
            }
        );
    }

    #[test]
    fn prepared_entry_windows_respect_minimums_for_large_workloads() {
        let config = prepared_entry_window_config_for_counts(10_000, 10_000, 1);

        assert_eq!(
            config,
            PreparedEntryWindowConfig {
                preopened_file_window_capacity: MIN_PREOPENED_FILE_WINDOW_CAPACITY,
                ready_entry_window_capacity: MIN_READY_ENTRY_WINDOW_CAPACITY,
            }
        );
    }

    #[test]
    fn prepare_restore_entry_with_open_returns_file_ready() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("data.bin");
        let restore_entry = RestoreEntry {
            path: path.clone(),
            entry: ArchiveListingEntry::file(
                "data.bin".to_string(),
                8,
                0o644,
                ArchiveTimestamp::default(),
                0,
                0,
                0,
            ),
        };
        let (permit_tx, permit_rx) = bounded(1);
        permit_tx.send(()).expect("seed permit");

        let prepared = prepare_restore_entry_with_open(restore_entry, &permit_rx, &permit_tx)
            .expect("file should prepare");

        match prepared {
            PreparedRestoreEntry::FileReady {
                path: prepared_path,
                writer,
                permit,
                ..
            } => {
                assert_eq!(prepared_path, path);
                assert!(path.exists());
                let _file = writer.into_inner().expect("writer should flush");
                drop(permit);
            }
            other => panic!("expected FileReady, got {other:?}"),
        }
    }

    #[test]
    fn work_items_reserve_open_permits_in_order() {
        let temp = tempfile::tempdir().expect("tempdir");
        let (permit_tx, permit_rx) = bounded(1);
        permit_tx.send(()).expect("seed permit");

        let mut first = file_work_item(0, temp.path().join("first.bin"));
        let mut second = file_work_item(1, temp.path().join("second.bin"));

        assert!(
            try_reserve_open_file_permit_for_work_item(&mut first, &permit_rx, &permit_tx)
                .expect("first reserve")
        );
        assert!(first.reserved_open_permit.is_some());
        assert!(
            !try_reserve_open_file_permit_for_work_item(&mut second, &permit_rx, &permit_tx)
                .expect("second should wait")
        );

        drop(first.reserved_open_permit.take());

        assert!(
            try_reserve_open_file_permit_for_work_item(&mut second, &permit_rx, &permit_tx)
                .expect("second reserve after release")
        );
        assert!(second.reserved_open_permit.is_some());
    }
}

fn path_matches_filter(path: &str, filter: &str) -> bool {
    path == filter
        || path
            .strip_prefix(filter)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

pub(crate) fn apply_entry_metadata(path: &Path, entry: &crate::ArchiveListingEntry) -> Result<()> {
    match entry.kind {
        crate::ArchiveEntryKind::Symlink => {
            apply_owner_nofollow(path, entry.uid, entry.gid)?;
            apply_mode_symlink(path, entry.mode)?;
            apply_mtime_nofollow(path, entry.mtime)?;
        }
        _ => {
            apply_owner(path, entry.uid, entry.gid)?;
            apply_mode(path, entry.mode)?;
            apply_mtime(path, entry.mtime)?;
        }
    }
    Ok(())
}

fn apply_mode(path: &Path, mode: u32) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        fs::set_permissions(path, fs::Permissions::from_mode(mode))?;
    }

    #[cfg(not(unix))]
    {
        let mut permissions = fs::metadata(path)?.permissions();
        permissions.set_readonly(mode & 0o200 == 0);
        fs::set_permissions(path, permissions)?;
    }

    Ok(())
}

#[cfg(unix)]
fn apply_owner(path: &Path, uid: u32, gid: u32) -> Result<()> {
    use std::ffi::CString;
    use std::io;
    use std::os::unix::ffi::OsStrExt;

    if should_apply_owner(uid, gid) == Some(false) {
        return Ok(());
    }

    let path = CString::new(path.as_os_str().as_bytes())
        .map_err(|_| OxideError::InvalidFormat("path contains interior nul byte"))?;
    let status = unsafe { libc::chown(path.as_ptr(), uid, gid) };
    if status == 0 {
        return Ok(());
    }

    let error = io::Error::last_os_error();
    match error.raw_os_error() {
        Some(libc::EPERM | libc::EACCES | libc::ENOTSUP | libc::EINVAL) => Ok(()),
        _ => Err(error.into()),
    }
}

#[cfg(not(unix))]
fn apply_owner(_: &Path, _: u32, _: u32) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn apply_owner_nofollow(path: &Path, uid: u32, gid: u32) -> Result<()> {
    use std::ffi::CString;
    use std::io;
    use std::os::unix::ffi::OsStrExt;

    if should_apply_owner(uid, gid) == Some(false) {
        return Ok(());
    }

    let path = CString::new(path.as_os_str().as_bytes())
        .map_err(|_| OxideError::InvalidFormat("path contains interior nul byte"))?;
    let status = unsafe { libc::lchown(path.as_ptr(), uid, gid) };
    if status == 0 {
        return Ok(());
    }

    let error = io::Error::last_os_error();
    match error.raw_os_error() {
        Some(libc::EPERM | libc::EACCES | libc::ENOTSUP | libc::EINVAL) => Ok(()),
        _ => Err(error.into()),
    }
}

#[cfg(not(unix))]
fn apply_owner_nofollow(_: &Path, _: u32, _: u32) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn should_apply_owner(uid: u32, gid: u32) -> Option<bool> {
    let effective_uid = unsafe { libc::geteuid() };
    let effective_gid = unsafe { libc::getegid() };

    if uid == effective_uid && gid == effective_gid {
        return Some(false);
    }

    if effective_uid != 0 && uid != effective_uid {
        return Some(false);
    }

    Some(true)
}

#[cfg(unix)]
fn apply_mtime(path: &Path, mtime: crate::ArchiveTimestamp) -> Result<()> {
    use std::ffi::CString;
    use std::io;
    use std::os::unix::ffi::OsStrExt;

    let path = CString::new(path.as_os_str().as_bytes())
        .map_err(|_| OxideError::InvalidFormat("path contains interior nul byte"))?;
    let times = [
        libc::timespec {
            tv_sec: 0,
            tv_nsec: libc::UTIME_OMIT,
        },
        libc::timespec {
            tv_sec: mtime.seconds as libc::time_t,
            tv_nsec: mtime.nanoseconds as libc::c_long,
        },
    ];
    let status = unsafe { libc::utimensat(libc::AT_FDCWD, path.as_ptr(), times.as_ptr(), 0) };
    if status == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error().into())
    }
}

#[cfg(not(unix))]
fn apply_mtime(_: &Path, _: crate::ArchiveTimestamp) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn apply_mtime_nofollow(path: &Path, mtime: crate::ArchiveTimestamp) -> Result<()> {
    use std::ffi::CString;
    use std::io;
    use std::os::unix::ffi::OsStrExt;

    let path = CString::new(path.as_os_str().as_bytes())
        .map_err(|_| OxideError::InvalidFormat("path contains interior nul byte"))?;
    let times = [
        libc::timespec {
            tv_sec: 0,
            tv_nsec: libc::UTIME_OMIT,
        },
        libc::timespec {
            tv_sec: mtime.seconds as libc::time_t,
            tv_nsec: mtime.nanoseconds as libc::c_long,
        },
    ];
    let status = unsafe {
        libc::utimensat(
            libc::AT_FDCWD,
            path.as_ptr(),
            times.as_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    };
    if status == 0 {
        Ok(())
    } else {
        let error = io::Error::last_os_error();
        match error.raw_os_error() {
            Some(libc::EPERM | libc::EACCES | libc::ENOTSUP | libc::EINVAL) => Ok(()),
            _ => Err(error.into()),
        }
    }
}

#[cfg(not(unix))]
fn apply_mtime_nofollow(_: &Path, _: crate::ArchiveTimestamp) -> Result<()> {
    Ok(())
}

fn apply_mode_symlink(_: &Path, _: u32) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn create_symlink(path: &Path, target: &str) -> Result<()> {
    std::os::unix::fs::symlink(target, path)?;
    Ok(())
}

#[cfg(windows)]
fn create_symlink(path: &Path, target: &str) -> Result<()> {
    use std::fs;

    let resolved_target = path.parent().unwrap_or_else(|| Path::new("")).join(target);
    if fs::metadata(&resolved_target)
        .map(|metadata| metadata.is_dir())
        .unwrap_or(false)
    {
        std::os::windows::fs::symlink_dir(target, path)?;
    } else {
        std::os::windows::fs::symlink_file(target, path)?;
    }
    Ok(())
}
