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
use crossbeam_channel::{Receiver, Sender, TryRecvError, TrySendError, bounded, never, unbounded};
use regex::RegexSet;

use crate::OxideError;
use crate::format::ArchiveManifest;
use crate::types::Result;

use super::super::super::directory;
use super::super::reorder_writer::{OrderedChunkWriter, OwnedChunk, SharedChunk};

mod metadata;
mod planner;
mod selection;
mod write_shards;
mod writer;

pub(super) use self::metadata::apply_entry_metadata;
use self::metadata::{PendingMetadata, apply_pending_metadata_in_parallel};
use self::planner::spawn_restore_planner;
#[cfg(test)]
use self::planner::{prepare_restore_entry_with_open, try_reserve_open_file_permit_for_work_item};
pub(super) use self::selection::DirectoryExtractSelection;

pub(super) const OUTPUT_BUFFER_CAPACITY: usize = 1024 * 1024;
const MEDIUM_OUTPUT_BUFFER_CAPACITY: usize = 256 * 1024;
const SMALL_OUTPUT_BUFFER_CAPACITY: usize = 64 * 1024;
const PARALLEL_METADATA_MIN_ITEMS: usize = 8;
const MAX_METADATA_WORKERS: usize = 8;
const MAX_PREPARED_ENTRY_WORKERS: usize = 8;
const PREPARED_ENTRY_QUEUE_CAPACITY: usize = 256;
const BASE_PREOPENED_FILE_WINDOW_MULTIPLIER: usize = 12;
const PREOPENED_FILE_WINDOW_BONUS_STEP: usize = 3;
const MIN_PREOPENED_FILE_WINDOW_CAPACITY: usize = 32;
const MAX_PREOPENED_FILE_WINDOW_CAPACITY: usize = 384;
const MIN_READY_ENTRY_WINDOW_CAPACITY: usize = 32;
const MAX_READY_ENTRY_WINDOW_CAPACITY: usize = 96;
const WRITE_SHARD_QUEUE_CAPACITY: usize = 64;
const DIRECT_FILE_WRITE_MAX_BYTES: u64 = 32 * 1024;
const SMALL_FILE_BUFFER_MAX_BYTES: u64 = 256 * 1024;
const MEDIUM_FILE_BUFFER_MAX_BYTES: u64 = 1024 * 1024;

#[derive(Debug, Clone, Default)]
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
    pub(crate) file_transition_wait: Duration,
    pub(crate) write_shard_count: usize,
    pub(crate) write_shard_queue_peak: Vec<usize>,
    pub(crate) write_shard_blocked: Vec<Duration>,
    pub(crate) write_shard_output_data: Vec<Duration>,
    pub(crate) ready_file_frontier: usize,
    pub(crate) planner_ready_queue_peak: usize,
    pub(crate) active_files_peak: usize,
    pub(crate) output_bytes_total: u64,
    pub(crate) entry_count: u64,
}

impl DirectoryRestoreStats {
    fn merge(&mut self, other: Self) {
        self.directory_decode += other.directory_decode;
        self.ordered_write_time += other.ordered_write_time;
        self.prepared_file_open += other.prepared_file_open;
        self.prepared_file_permit_wait += other.prepared_file_permit_wait;
        self.prepared_file_wait += other.prepared_file_wait;
        self.output_prepare_directories += other.output_prepare_directories;
        self.output_write += other.output_write;
        self.output_create += other.output_create;
        self.output_create_directories += other.output_create_directories;
        self.output_create_files += other.output_create_files;
        self.output_data += other.output_data;
        self.output_flush += other.output_flush;
        self.output_metadata += other.output_metadata;
        self.output_metadata_files += other.output_metadata_files;
        self.output_metadata_directories += other.output_metadata_directories;
        self.file_transition_wait += other.file_transition_wait;
        self.ensure_write_shards(other.write_shard_count);
        for (shard, peak) in other.write_shard_queue_peak.iter().copied().enumerate() {
            self.record_write_shard_queue_peak(shard, peak);
        }
        for (shard, elapsed) in other.write_shard_blocked.iter().copied().enumerate() {
            self.record_write_shard_blocked(shard, elapsed);
        }
        for (shard, elapsed) in other.write_shard_output_data.iter().copied().enumerate() {
            self.record_write_shard_output_data(shard, elapsed);
        }
        self.ready_file_frontier = self.ready_file_frontier.max(other.ready_file_frontier);
        self.planner_ready_queue_peak = self
            .planner_ready_queue_peak
            .max(other.planner_ready_queue_peak);
        self.active_files_peak = self.active_files_peak.max(other.active_files_peak);
        self.output_bytes_total = self
            .output_bytes_total
            .saturating_add(other.output_bytes_total);
        self.entry_count = self.entry_count.saturating_add(other.entry_count);
    }

    fn ensure_write_shards(&mut self, shard_count: usize) {
        self.write_shard_count = self.write_shard_count.max(shard_count);
        if self.write_shard_queue_peak.len() < self.write_shard_count {
            self.write_shard_queue_peak
                .resize(self.write_shard_count, 0);
        }
        if self.write_shard_blocked.len() < self.write_shard_count {
            self.write_shard_blocked
                .resize(self.write_shard_count, Duration::ZERO);
        }
        if self.write_shard_output_data.len() < self.write_shard_count {
            self.write_shard_output_data
                .resize(self.write_shard_count, Duration::ZERO);
        }
    }

    fn record_prepared_file_open(&mut self, elapsed: Duration) {
        self.prepared_file_open += elapsed;
    }

    fn record_prepared_file_permit_wait(&mut self, elapsed: Duration) {
        self.prepared_file_permit_wait += elapsed;
    }

    fn record_prepared_file_wait(&mut self, elapsed: Duration) {
        self.prepared_file_wait += elapsed;
    }

    fn record_file_transition_wait(&mut self, elapsed: Duration) {
        self.file_transition_wait += elapsed;
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

    fn record_write_shard_queue_peak(&mut self, shard: usize, peak: usize) {
        self.ensure_write_shards(shard + 1);
        if let Some(slot) = self.write_shard_queue_peak.get_mut(shard) {
            *slot = (*slot).max(peak);
        }
    }

    fn record_write_shard_blocked(&mut self, shard: usize, elapsed: Duration) {
        self.ensure_write_shards(shard + 1);
        if let Some(slot) = self.write_shard_blocked.get_mut(shard) {
            *slot += elapsed;
        }
    }

    fn record_write_shard_output_data(&mut self, shard: usize, elapsed: Duration) {
        self.ensure_write_shards(shard + 1);
        if let Some(slot) = self.write_shard_output_data.get_mut(shard) {
            *slot += elapsed;
        }
    }

    fn record_ready_file_frontier(&mut self, frontier: usize) {
        self.ready_file_frontier = self.ready_file_frontier.max(frontier);
    }

    fn record_planner_ready_queue_peak(&mut self, peak: usize) {
        self.planner_ready_queue_peak = self.planner_ready_queue_peak.max(peak);
    }

    fn record_active_files_peak(&mut self, peak: usize) {
        self.active_files_peak = self.active_files_peak.max(peak);
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
    active_file_counted: bool,
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
struct ReadyFileWrite {
    path: PathBuf,
    entry: crate::ArchiveListingEntry,
    writer: PendingFileWriter,
    permit: OpenFilePermit,
}

#[derive(Debug)]
enum RestorePlannerMessage {
    ReadyFile(ReadyFileWrite),
    Finished(RestorePlannerOutcome),
}

#[derive(Debug, Default)]
struct RestorePlannerOutcome {
    data_file_count_total: usize,
    pending_file_metadata: Vec<PendingMetadata>,
    pending_directory_metadata: Vec<PendingMetadata>,
    stats: DirectoryRestoreStats,
}

#[derive(Debug, Default)]
struct PreparedRestoreWorkerOutcome {
    active_files_peak: usize,
}

#[derive(Debug)]
enum PendingFileState {
    Local {
        writer: PendingFileWriter,
        permit: OpenFilePermit,
    },
    Sharded {
        file_id: usize,
        shard: usize,
    },
}

#[derive(Debug)]
struct PendingFile {
    remaining: u64,
    path: PathBuf,
    entry: crate::ArchiveListingEntry,
    state: PendingFileState,
}

#[derive(Debug)]
struct PendingShardCompletion {
    path: PathBuf,
    entry: crate::ArchiveListingEntry,
    shard: usize,
}

#[derive(Debug)]
enum WriteShardTask {
    OpenFile {
        file_id: usize,
        writer: PendingFileWriter,
        permit: OpenFilePermit,
    },
    WriteData {
        file_id: usize,
        chunk: SharedChunk,
        range: Range<usize>,
        finish: bool,
    },
    Abort,
}

#[derive(Debug)]
struct WriteShardCompletion {
    file_id: usize,
    flush_elapsed: Duration,
}

#[derive(Debug, Default)]
struct WriteShardOutcome {
    shard: usize,
    output_data: Duration,
}

#[derive(Debug)]
struct ShardPendingFile {
    file_id: usize,
    writer: PendingFileWriter,
    permit: OpenFilePermit,
}

#[derive(Debug)]
struct WriteShardWorker {
    shard_count: usize,
    next_file_id: usize,
    next_shard_hint: usize,
    shard_load_bytes: Vec<u64>,
    completion_rx: Receiver<Result<WriteShardCompletion>>,
    task_txs: Vec<Sender<WriteShardTask>>,
    handles: Vec<thread::JoinHandle<Result<WriteShardOutcome>>>,
    pending: BTreeMap<usize, PendingShardCompletion>,
    aborted: bool,
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
pub(crate) struct DirectoryRestoreWriter {
    root: PathBuf,
    data_file_count_total: usize,
    completed_data_files: usize,
    pending_file: Option<PendingFile>,
    write_shards: Option<WriteShardWorker>,
    created_directories: HashSet<PathBuf>,
    pending_file_metadata: Vec<PendingMetadata>,
    pending_directory_metadata: Vec<PendingMetadata>,
    ready_files: VecDeque<ReadyFileWrite>,
    planner_finished: bool,
    planner_rx: Receiver<Result<RestorePlannerMessage>>,
    planner_handle: Option<thread::JoinHandle<Result<()>>>,
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
        .saturating_mul(6)
        .clamp(
            MIN_READY_ENTRY_WINDOW_CAPACITY,
            MAX_READY_ENTRY_WINDOW_CAPACITY,
        )
        .min(entry_count);
    let preopened_file_window_capacity =
        adaptive_preopened_file_window_capacity(entry_count, file_count, parallelism)
            .min(ready_entry_window_capacity.saturating_mul(4).max(1));

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
