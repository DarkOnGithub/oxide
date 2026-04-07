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

use super::super::directory;
use super::reorder_writer::OrderedChunkWriter;

pub(crate) const OUTPUT_BUFFER_CAPACITY: usize = 1024 * 1024;
const MEDIUM_OUTPUT_BUFFER_CAPACITY: usize = 256 * 1024;
const SMALL_OUTPUT_BUFFER_CAPACITY: usize = 64 * 1024;
const PARALLEL_METADATA_MIN_ITEMS: usize = 8;
const MAX_METADATA_WORKERS: usize = 8;
const MAX_PREPARED_ENTRY_WORKERS: usize = 8;
const PREPARED_ENTRY_QUEUE_CAPACITY: usize = 256;
const BASE_PREOPENED_FILE_WINDOW_MULTIPLIER: usize = 8;
const PREOPENED_FILE_WINDOW_BONUS_STEP: usize = 2;
const MIN_PREOPENED_FILE_WINDOW_CAPACITY: usize = 32;
const MAX_PREOPENED_FILE_WINDOW_CAPACITY: usize = 256;
const MIN_READY_ENTRY_WINDOW_CAPACITY: usize = 32;
const MAX_READY_ENTRY_WINDOW_CAPACITY: usize = 64;
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
        bytes: std::sync::Arc<[u8]>,
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
struct PendingMetadata {
    path: PathBuf,
    entry: crate::ArchiveListingEntry,
    depth: usize,
}

impl PendingMetadata {
    fn new(path: PathBuf, entry: crate::ArchiveListingEntry) -> Self {
        Self {
            depth: path_depth(&path),
            path,
            entry,
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

impl WriteShardWorker {
    fn spawn(shard_count: usize) -> Self {
        let (completion_tx, completion_rx) = unbounded::<Result<WriteShardCompletion>>();
        let mut task_txs = Vec::with_capacity(shard_count);
        let mut handles = Vec::with_capacity(shard_count);

        for shard in 0..shard_count {
            let (task_tx, task_rx) = bounded::<WriteShardTask>(WRITE_SHARD_QUEUE_CAPACITY);
            let completion_tx = completion_tx.clone();
            let handle = thread::spawn(move || run_write_shard(shard, task_rx, completion_tx));
            task_txs.push(task_tx);
            handles.push(handle);
        }

        Self {
            shard_count,
            next_file_id: 0,
            next_shard_hint: 0,
            shard_load_bytes: vec![0; shard_count],
            completion_rx,
            task_txs,
            handles,
            pending: BTreeMap::new(),
            aborted: false,
        }
    }

    fn reserve_file(&mut self, file_size: u64) -> (usize, usize) {
        let file_id = self.next_file_id;
        let mut shard = 0usize;
        let mut best = (u64::MAX, usize::MAX, usize::MAX);
        for offset in 0..self.shard_count.max(1) {
            let candidate = (self.next_shard_hint + offset) % self.shard_count.max(1);
            let candidate_load = self.shard_load_bytes[candidate];
            let candidate_queue = self.task_txs[candidate].len();
            let candidate_key = (candidate_load, candidate_queue, offset);
            if candidate_key < best {
                best = candidate_key;
                shard = candidate;
            }
        }
        self.next_file_id = self.next_file_id.saturating_add(1);
        self.next_shard_hint = (shard + 1) % self.shard_count.max(1);
        self.shard_load_bytes[shard] = self.shard_load_bytes[shard].saturating_add(file_size);
        (file_id, shard)
    }

    fn release_file(&mut self, shard: usize, file_size: u64) {
        if let Some(load) = self.shard_load_bytes.get_mut(shard) {
            *load = load.saturating_sub(file_size);
        }
    }

    fn record_queue_peak(&self, stats: &mut DirectoryRestoreStats, shard: usize) {
        if let Some(tx) = self.task_txs.get(shard) {
            stats.record_write_shard_queue_peak(shard, tx.len());
        }
    }

    fn send_task(
        &mut self,
        shard: usize,
        task: WriteShardTask,
        stats: &mut DirectoryRestoreStats,
        pending_file_metadata: &mut Vec<PendingMetadata>,
        completed_data_files: &mut usize,
    ) -> Result<()> {
        self.try_receive_completions(stats, pending_file_metadata, completed_data_files)?;
        let Some(tx) = self.task_txs.get(shard) else {
            return Err(crate::OxideError::CompressionError(format!(
                "write shard {shard} is not available"
            )));
        };

        match tx.try_send(task) {
            Ok(()) => {
                self.record_queue_peak(stats, shard);
                Ok(())
            }
            Err(TrySendError::Full(task)) => {
                let blocked_started = Instant::now();
                tx.send(task).map_err(|_| {
                    crate::OxideError::CompressionError(format!(
                        "write shard {shard} queue closed before completion"
                    ))
                })?;
                stats.record_write_shard_blocked(shard, blocked_started.elapsed());
                self.record_queue_peak(stats, shard);
                Ok(())
            }
            Err(TrySendError::Disconnected(_)) => Err(crate::OxideError::CompressionError(
                format!("write shard {shard} disconnected before completion"),
            )),
        }
    }

    fn register_pending_completion(
        &mut self,
        file_id: usize,
        shard: usize,
        path: PathBuf,
        entry: crate::ArchiveListingEntry,
    ) {
        self.pending
            .insert(file_id, PendingShardCompletion { path, entry, shard });
    }

    fn forget_pending_completion(&mut self, file_id: usize) {
        let _ = self.pending.remove(&file_id);
    }

    fn try_receive_completions(
        &mut self,
        stats: &mut DirectoryRestoreStats,
        pending_file_metadata: &mut Vec<PendingMetadata>,
        completed_data_files: &mut usize,
    ) -> Result<()> {
        loop {
            match self.completion_rx.try_recv() {
                Ok(completion) => self.handle_completion(
                    completion,
                    stats,
                    pending_file_metadata,
                    completed_data_files,
                )?,
                Err(TryRecvError::Empty) => return Ok(()),
                Err(TryRecvError::Disconnected) => {
                    return Err(crate::OxideError::CompressionError(
                        "write shard completion queue closed before completion".to_string(),
                    ));
                }
            }
        }
    }

    fn finish(
        &mut self,
        stats: &mut DirectoryRestoreStats,
        pending_file_metadata: &mut Vec<PendingMetadata>,
        completed_data_files: &mut usize,
    ) -> Result<()> {
        while !self.pending.is_empty() {
            let completion = self.completion_rx.recv().map_err(|_| {
                crate::OxideError::CompressionError(
                    "write shard completion queue closed before all files finished".to_string(),
                )
            })?;
            self.handle_completion(
                completion,
                stats,
                pending_file_metadata,
                completed_data_files,
            )?;
        }

        self.join(stats)
    }

    fn abort(&mut self) {
        if self.aborted {
            return;
        }
        self.aborted = true;
        for tx in &self.task_txs {
            let _ = tx.try_send(WriteShardTask::Abort);
        }
    }

    fn shutdown(&mut self) {
        self.abort();
        let mut stats = DirectoryRestoreStats::default();
        let _ = self.join_outputs_only(&mut stats);
    }

    fn handle_completion(
        &mut self,
        completion: Result<WriteShardCompletion>,
        stats: &mut DirectoryRestoreStats,
        pending_file_metadata: &mut Vec<PendingMetadata>,
        completed_data_files: &mut usize,
    ) -> Result<()> {
        let completion = completion?;
        let pending = self.pending.remove(&completion.file_id).ok_or_else(|| {
            crate::OxideError::CompressionError("write shard completed unknown file".to_string())
        })?;
        self.shard_load_bytes[pending.shard] =
            self.shard_load_bytes[pending.shard].saturating_sub(pending.entry.size);
        stats.record_output_flush(completion.flush_elapsed);
        pending_file_metadata.push(PendingMetadata::new(pending.path, pending.entry));
        *completed_data_files = completed_data_files.saturating_add(1);
        Ok(())
    }

    fn join(&mut self, stats: &mut DirectoryRestoreStats) -> Result<()> {
        self.join_outputs_only(stats)
    }

    fn join_outputs_only(&mut self, stats: &mut DirectoryRestoreStats) -> Result<()> {
        for tx in self.task_txs.drain(..) {
            drop(tx);
        }

        for handle in self.handles.drain(..) {
            let outcome = handle.join().map_err(|payload| {
                let details = if let Some(message) = payload.downcast_ref::<&str>() {
                    (*message).to_string()
                } else if let Some(message) = payload.downcast_ref::<String>() {
                    message.clone()
                } else {
                    "unknown panic payload".to_string()
                };
                crate::OxideError::CompressionError(format!(
                    "write shard thread panicked: {details}"
                ))
            })??;
            stats.record_output_data(outcome.output_data);
            stats.record_write_shard_output_data(outcome.shard, outcome.output_data);
        }
        Ok(())
    }
}

fn run_write_shard(
    shard: usize,
    task_rx: Receiver<WriteShardTask>,
    completion_tx: Sender<Result<WriteShardCompletion>>,
) -> Result<WriteShardOutcome> {
    let result = run_write_shard_inner(shard, task_rx, &completion_tx);
    if let Err(error) = &result {
        let _ = completion_tx.send(Err(crate::OxideError::CompressionError(error.to_string())));
    }
    result
}

fn run_write_shard_inner(
    shard: usize,
    task_rx: Receiver<WriteShardTask>,
    completion_tx: &Sender<Result<WriteShardCompletion>>,
) -> Result<WriteShardOutcome> {
    let mut outcome = WriteShardOutcome {
        shard,
        ..WriteShardOutcome::default()
    };
    let mut pending: Option<ShardPendingFile> = None;

    while let Ok(task) = task_rx.recv() {
        match task {
            WriteShardTask::OpenFile {
                file_id,
                writer,
                permit,
            } => {
                if pending.is_some() {
                    return Err(crate::OxideError::CompressionError(format!(
                        "write shard {shard} received a new file before finishing the previous one"
                    )));
                }
                pending = Some(ShardPendingFile {
                    file_id,
                    writer,
                    permit,
                });
            }
            WriteShardTask::WriteData {
                file_id,
                bytes,
                finish,
            } => {
                let current = pending.as_mut().ok_or_else(|| {
                    crate::OxideError::CompressionError(format!(
                        "write shard {shard} received data without an open file"
                    ))
                })?;
                if current.file_id != file_id {
                    return Err(crate::OxideError::CompressionError(format!(
                        "write shard {shard} received out-of-order file data"
                    )));
                }

                let write_started = Instant::now();
                current.writer.write_all(&bytes)?;
                outcome.output_data += write_started.elapsed();

                if finish {
                    let pending_file = pending.take().ok_or_else(|| {
                        crate::OxideError::CompressionError(
                            "write shard lost pending file during finalization".to_string(),
                        )
                    })?;
                    let ShardPendingFile {
                        file_id,
                        writer,
                        permit,
                    } = pending_file;
                    let flush_started = Instant::now();
                    let _file = writer.into_inner()?;
                    let flush_elapsed = flush_started.elapsed();
                    drop(permit);
                    if completion_tx
                        .send(Ok(WriteShardCompletion {
                            file_id,
                            flush_elapsed,
                        }))
                        .is_err()
                    {
                        return Ok(outcome);
                    }
                }
            }
            WriteShardTask::Abort => return Ok(outcome),
        }
    }

    if pending.is_some() {
        return Err(crate::OxideError::CompressionError(format!(
            "write shard {shard} closed with a pending file"
        )));
    }

    Ok(outcome)
}

impl DirectoryRestoreWriter {
    pub(crate) fn create(root: &Path, manifest: ArchiveManifest) -> Result<Self> {
        Self::create_with_shards(root, manifest, 1)
    }

    pub(crate) fn create_with_performance(
        root: &Path,
        manifest: ArchiveManifest,
        performance: &crate::pipeline::types::PipelinePerformanceOptions,
    ) -> Result<Self> {
        Self::create_with_shards(root, manifest, performance.extract_write_shards.max(1))
    }

    fn create_with_shards(
        root: &Path,
        manifest: ArchiveManifest,
        write_shards: usize,
    ) -> Result<Self> {
        let entries = build_restore_entries(root, manifest)?;
        let window_config = prepared_entry_window_config(&entries);
        let write_shard_count = write_shards.max(1);
        let mut writer = Self {
            root: root.to_path_buf(),
            data_file_count_total: 0,
            completed_data_files: 0,
            pending_file: None,
            write_shards: (write_shard_count > 1)
                .then(|| WriteShardWorker::spawn(write_shard_count)),
            created_directories: HashSet::new(),
            pending_file_metadata: Vec::new(),
            pending_directory_metadata: Vec::new(),
            planner_finished: false,
            planner_rx: never(),
            planner_handle: None,
            stats: DirectoryRestoreStats::default(),
        };
        writer.stats.ensure_write_shards(write_shard_count);
        writer.ensure_directory_exists(root)?;
        writer.prepare_output_directories(&entries)?;
        let (planner_rx, planner_handle) = spawn_restore_planner(entries, window_config);
        writer.planner_rx = planner_rx;
        writer.planner_handle = Some(planner_handle);
        Ok(writer)
    }

    pub(crate) fn finish(&mut self) -> Result<DirectoryRestoreStats> {
        if let Some(pending) = self.pending_file.as_ref()
            && pending.remaining > 0
        {
            return Err(crate::OxideError::InvalidFormat(
                "truncated file payload during directory restore",
            ));
        }

        self.wait_for_planner_completion()?;
        self.finish_write_shards()?;

        if self.pending_file.is_some() || self.completed_data_files != self.data_file_count_total {
            return Err(crate::OxideError::InvalidFormat(
                "directory restore ended before all entries completed",
            ));
        }
        self.finalize_file_metadata()?;
        self.finalize_directories()?;
        self.join_planner()?;
        Ok(self.stats.clone())
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
        self.pending_file_metadata
            .push(PendingMetadata::new(path, entry));
    }

    fn defer_directory_metadata(&mut self, path: PathBuf, entry: crate::ArchiveListingEntry) {
        self.pending_directory_metadata
            .push(PendingMetadata::new(path, entry));
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

    fn ensure_pending_file_for_write(&mut self) -> Result<bool> {
        self.try_receive_write_shard_completions()?;
        if self.pending_file.is_some() {
            return Ok(true);
        }

        self.pending_file = self.next_pending_file(true)?;
        Ok(self.pending_file.is_some())
    }

    fn next_pending_file(
        &mut self,
        track_file_transition_wait: bool,
    ) -> Result<Option<PendingFile>> {
        loop {
            match self.planner_rx.try_recv() {
                Ok(message) => {
                    self.stats
                        .record_planner_ready_queue_peak(self.planner_rx.len());
                    if let Some(pending_file) = self.consume_planner_message(message?)? {
                        return Ok(Some(pending_file));
                    }
                    if self.planner_finished {
                        return Ok(None);
                    }
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => return self.handle_planner_disconnect(),
            }
        }

        if self.planner_finished {
            return Ok(None);
        }

        let wait_started = Instant::now();
        loop {
            match self.planner_rx.recv() {
                Ok(message) => {
                    let wait_elapsed = wait_started.elapsed();
                    self.stats.record_prepared_file_wait(wait_elapsed);
                    if track_file_transition_wait {
                        self.stats.record_file_transition_wait(wait_elapsed);
                    }
                    self.stats
                        .record_planner_ready_queue_peak(self.planner_rx.len());
                    if let Some(pending_file) = self.consume_planner_message(message?)? {
                        return Ok(Some(pending_file));
                    }
                    return Ok(None);
                }
                Err(_) => {
                    let wait_elapsed = wait_started.elapsed();
                    self.stats.record_prepared_file_wait(wait_elapsed);
                    if track_file_transition_wait {
                        self.stats.record_file_transition_wait(wait_elapsed);
                    }
                    return self.handle_planner_disconnect();
                }
            }
        }
    }

    fn consume_planner_message(
        &mut self,
        message: RestorePlannerMessage,
    ) -> Result<Option<PendingFile>> {
        match message {
            RestorePlannerMessage::ReadyFile(ready_file) => {
                self.stats
                    .record_ready_file_frontier(self.planner_rx.len().saturating_add(1));
                if let Some(write_shards) = self.write_shards.as_mut() {
                    let file_size = ready_file.entry.size;
                    let (file_id, shard) = write_shards.reserve_file(file_size);
                    if let Err(error) = write_shards.send_task(
                        shard,
                        WriteShardTask::OpenFile {
                            file_id,
                            writer: ready_file.writer,
                            permit: ready_file.permit,
                        },
                        &mut self.stats,
                        &mut self.pending_file_metadata,
                        &mut self.completed_data_files,
                    ) {
                        write_shards.release_file(shard, file_size);
                        return Err(error);
                    }
                    Ok(Some(PendingFile {
                        remaining: ready_file.entry.size,
                        path: ready_file.path,
                        entry: ready_file.entry,
                        state: PendingFileState::Sharded { file_id, shard },
                    }))
                } else {
                    Ok(Some(PendingFile {
                        remaining: ready_file.entry.size,
                        path: ready_file.path,
                        entry: ready_file.entry,
                        state: PendingFileState::Local {
                            writer: ready_file.writer,
                            permit: ready_file.permit,
                        },
                    }))
                }
            }
            RestorePlannerMessage::Finished(outcome) => {
                self.apply_planner_outcome(outcome);
                Ok(None)
            }
        }
    }

    fn apply_planner_outcome(&mut self, outcome: RestorePlannerOutcome) {
        self.planner_finished = true;
        self.data_file_count_total = outcome.data_file_count_total;
        self.pending_file_metadata
            .extend(outcome.pending_file_metadata);
        self.pending_directory_metadata
            .extend(outcome.pending_directory_metadata);
        self.stats.merge(outcome.stats);
    }

    fn wait_for_planner_completion(&mut self) -> Result<()> {
        while !self.planner_finished {
            self.try_receive_write_shard_completions()?;
            let Some(ready_file) = self.next_pending_file(false)? else {
                continue;
            };
            drop(ready_file);
        }

        Ok(())
    }

    fn try_receive_write_shard_completions(&mut self) -> Result<()> {
        if let Some(write_shards) = self.write_shards.as_mut() {
            write_shards.try_receive_completions(
                &mut self.stats,
                &mut self.pending_file_metadata,
                &mut self.completed_data_files,
            )?;
        }
        Ok(())
    }

    fn finish_write_shards(&mut self) -> Result<()> {
        if let Some(write_shards) = self.write_shards.as_mut() {
            write_shards.finish(
                &mut self.stats,
                &mut self.pending_file_metadata,
                &mut self.completed_data_files,
            )?;
        }
        Ok(())
    }

    fn handle_planner_disconnect(&mut self) -> Result<Option<PendingFile>> {
        self.join_planner()?;
        if self.planner_finished {
            Ok(None)
        } else {
            Err(crate::OxideError::CompressionError(
                "restore planner queue closed before completion".to_string(),
            ))
        }
    }

    fn join_planner(&mut self) -> Result<()> {
        let Some(handle) = self.planner_handle.take() else {
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
                "restore planner thread panicked: {details}"
            ))
        })?
    }

    fn finalize_pending_file(&mut self) -> Result<()> {
        let pending = self.pending_file.take().ok_or(OxideError::InvalidFormat(
            "directory restore missing pending file after write completion",
        ))?;
        let PendingFile {
            path, entry, state, ..
        } = pending;
        match state {
            PendingFileState::Local { writer, permit } => {
                let started = Instant::now();
                let _file = writer.into_inner()?;
                self.stats.record_output_flush(started.elapsed());
                drop(permit);
                self.defer_file_metadata(path, entry);
                self.completed_data_files = self.completed_data_files.saturating_add(1);
            }
            PendingFileState::Sharded { file_id, .. } => {
                let _ = (file_id, path, entry);
            }
        }
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

impl Drop for DirectoryRestoreWriter {
    fn drop(&mut self) {
        if let Some(write_shards) = self.write_shards.as_mut() {
            write_shards.shutdown();
        }
        let _ = self.join_planner();
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
        .saturating_mul(4)
        .clamp(
            MIN_READY_ENTRY_WINDOW_CAPACITY,
            MAX_READY_ENTRY_WINDOW_CAPACITY,
        )
        .min(entry_count);
    let preopened_file_window_capacity =
        adaptive_preopened_file_window_capacity(entry_count, file_count, parallelism)
            .min(ready_entry_window_capacity.saturating_mul(2).max(1));

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

fn spawn_restore_planner(
    entries: Vec<RestoreEntry>,
    window_config: PreparedEntryWindowConfig,
) -> (
    Receiver<Result<RestorePlannerMessage>>,
    thread::JoinHandle<Result<()>>,
) {
    // Phase 3 split: the planner owns manifest-order coordination and emits
    // only data-bearing file work to the executor. Non-data entries and
    // deferred metadata stay off the write hot path and are merged at finish.
    let (tx, rx) =
        bounded::<Result<RestorePlannerMessage>>(window_config.ready_entry_window_capacity);
    let handle = thread::spawn(move || -> Result<()> {
        let (prepared_entry_rx, prepared_entry_handle) =
            spawn_prepared_restore_entries(entries, window_config);
        let mut outcome = RestorePlannerOutcome::default();

        let planner_result = (|| -> Result<bool> {
            while let Ok(prepared) = prepared_entry_rx.recv() {
                let prepared = prepared?;
                outcome.stats.entry_count = outcome.stats.entry_count.saturating_add(1);

                match prepared {
                    PreparedRestoreEntry::Directory { path, entry } => {
                        outcome
                            .pending_directory_metadata
                            .push(PendingMetadata::new(path, entry));
                    }
                    PreparedRestoreEntry::File { .. } => {
                        let _ = tx.send(Err(crate::OxideError::CompressionError(
                            "prepared file entry reached planner before in-order file open"
                                .to_string(),
                        )));
                        return Ok(false);
                    }
                    PreparedRestoreEntry::FileReady {
                        path,
                        entry,
                        writer,
                        permit,
                        permit_wait_elapsed,
                        open_elapsed,
                    } => {
                        outcome
                            .stats
                            .record_prepared_file_permit_wait(permit_wait_elapsed);
                        outcome.stats.record_prepared_file_open(open_elapsed);

                        if entry.size == 0 {
                            let started = Instant::now();
                            let _file = writer.into_inner()?;
                            outcome.stats.record_output_flush(started.elapsed());
                            outcome
                                .pending_file_metadata
                                .push(PendingMetadata::new(path, entry));
                            drop(permit);
                            continue;
                        }

                        outcome.data_file_count_total =
                            outcome.data_file_count_total.saturating_add(1);
                        if tx
                            .send(Ok(RestorePlannerMessage::ReadyFile(ReadyFileWrite {
                                path,
                                entry,
                                writer,
                                permit,
                            })))
                            .is_err()
                        {
                            return Ok(false);
                        }
                        outcome.stats.record_planner_ready_queue_peak(tx.len());
                    }
                    PreparedRestoreEntry::Symlink {
                        path,
                        entry,
                        create_elapsed,
                    } => {
                        outcome.stats.record_output_create_file(create_elapsed);
                        outcome
                            .pending_file_metadata
                            .push(PendingMetadata::new(path, entry));
                    }
                }
            }

            Ok(true)
        })();

        let join_result = join_prepared_restore_entries_handle(prepared_entry_handle);
        match (planner_result, join_result) {
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
            (Ok(true), Ok(prepared_entry_outcome)) => {
                outcome
                    .stats
                    .record_active_files_peak(prepared_entry_outcome.active_files_peak);
                let _ = tx.send(Ok(RestorePlannerMessage::Finished(outcome)));
                Ok(())
            }
            (Ok(false), Ok(_)) => Ok(()),
        }
    });

    (rx, handle)
}

fn spawn_prepared_restore_entries(
    entries: Vec<RestoreEntry>,
    window_config: PreparedEntryWindowConfig,
) -> (
    Receiver<Result<PreparedRestoreEntry>>,
    thread::JoinHandle<Result<PreparedRestoreWorkerOutcome>>,
) {
    let (tx, rx) = bounded::<Result<PreparedRestoreEntry>>(PREPARED_ENTRY_QUEUE_CAPACITY);
    let handle = thread::spawn(move || -> Result<PreparedRestoreWorkerOutcome> {
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
            let mut outcome = PreparedRestoreWorkerOutcome::default();
            for restore_entry in entries {
                if matches!(restore_entry.entry.kind, crate::ArchiveEntryKind::File) {
                    outcome.active_files_peak = outcome.active_files_peak.max(1);
                }
                let prepared = prepare_restore_entry_with_open(
                    restore_entry,
                    &open_file_permit_rx,
                    &open_file_permit_tx,
                );
                let stop_after_send = prepared.is_err();
                if tx.send(prepared).is_err() {
                    return Ok(outcome);
                }
                if stop_after_send {
                    return Ok(outcome);
                }
            }

            return Ok(outcome);
        }

        let cancelled = Arc::new(AtomicBool::new(false));
        let (work_tx, work_rx) = bounded::<PreparedRestoreWorkItem>(PREPARED_ENTRY_QUEUE_CAPACITY);
        let (result_tx, result_rx) =
            bounded::<(usize, Result<PreparedRestoreEntry>)>(PREPARED_ENTRY_QUEUE_CAPACITY);
        let file_work_items = entries
            .iter()
            .map(|restore_entry| matches!(restore_entry.entry.kind, crate::ArchiveEntryKind::File))
            .collect::<Vec<_>>();

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
                active_file_counted: false,
            })
            .collect::<VecDeque<_>>();

        let mut next_index = 0usize;
        let mut pending: BTreeMap<usize, Result<PreparedRestoreEntry>> = BTreeMap::new();
        let mut in_flight = 0usize;
        let mut active_files = 0usize;
        let mut active_files_peak = 0usize;
        let mut sent_error = false;
        while next_index < total_entries && !sent_error {
            while let Some(prepared) = pending.remove(&next_index) {
                if file_work_items.get(next_index).copied().unwrap_or(false) {
                    active_files = active_files.saturating_sub(1);
                }
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
                        Ok(true) => {
                            mark_active_file_counted(
                                &mut work_item,
                                &mut active_files,
                                &mut active_files_peak,
                            );
                        }
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
                                        return Ok(PreparedRestoreWorkerOutcome {
                                            active_files_peak,
                                        });
                                    }
                                    break;
                                }
                                mark_active_file_counted(
                                    &mut work_item,
                                    &mut active_files,
                                    &mut active_files_peak,
                                );
                            } else {
                                pending_work.push_front(work_item);
                                break;
                            }
                        }
                        Err(error) => {
                            cancelled.store(true, Ordering::Relaxed);
                            sent_error = true;
                            if tx.send(Err(error)).is_err() {
                                return Ok(PreparedRestoreWorkerOutcome { active_files_peak });
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
                if file_work_items.get(next_index).copied().unwrap_or(false) {
                    active_files = active_files.saturating_sub(1);
                }
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

        Ok(PreparedRestoreWorkerOutcome { active_files_peak })
    });

    (rx, handle)
}

fn join_prepared_restore_entries_handle(
    handle: thread::JoinHandle<Result<PreparedRestoreWorkerOutcome>>,
) -> Result<PreparedRestoreWorkerOutcome> {
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

fn mark_active_file_counted(
    work_item: &mut PreparedRestoreWorkItem,
    active_files: &mut usize,
    active_files_peak: &mut usize,
) {
    if work_item.active_file_counted {
        return;
    }

    work_item.active_file_counted = true;
    *active_files += 1;
    *active_files_peak = (*active_files_peak).max(*active_files);
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
        Self::create_with_performance(
            root,
            manifest,
            selected_ranges,
            &crate::pipeline::types::PipelinePerformanceOptions::default(),
        )
    }

    pub(crate) fn create_with_performance(
        root: &Path,
        manifest: ArchiveManifest,
        selected_ranges: Vec<Range<u64>>,
        performance: &crate::pipeline::types::PipelinePerformanceOptions,
    ) -> Result<Self> {
        Ok(Self {
            inner: DirectoryRestoreWriter::create_with_performance(root, manifest, performance)?,
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
        self.try_receive_write_shard_completions()?;
        if !bytes.is_empty() && self.pending_file.is_none() {
            let decode_started = Instant::now();
            if !self.ensure_pending_file_for_write()? {
                return Err(OxideError::InvalidFormat(
                    "directory archive contains file data beyond manifest entries",
                ));
            }
            self.stats.directory_decode += decode_started.elapsed();
        }

        while !bytes.is_empty() {
            let (take, finished_file) = {
                let pending = self.pending_file.as_mut().ok_or(OxideError::InvalidFormat(
                    "directory archive contains file data beyond manifest entries",
                ))?;

                let take = bytes.len().min(pending.remaining as usize);
                pending.remaining -= take as u64;
                match &mut pending.state {
                    PendingFileState::Local { writer, .. } => {
                        let started = Instant::now();
                        writer.write_all(&bytes[..take])?;
                        let write_elapsed = started.elapsed();
                        self.stats.record_output_data(write_elapsed);
                        self.stats.record_write_shard_output_data(0, write_elapsed);
                    }
                    PendingFileState::Sharded { file_id, shard } => {
                        let write_shards = self.write_shards.as_mut().ok_or_else(|| {
                            crate::OxideError::CompressionError(
                                "directory restore missing write shards during data dispatch"
                                    .to_string(),
                            )
                        })?;
                        let payload: std::sync::Arc<[u8]> = std::sync::Arc::from(&bytes[..take]);
                        let finish_file = pending.remaining == 0;
                        if finish_file {
                            write_shards.register_pending_completion(
                                *file_id,
                                *shard,
                                pending.path.clone(),
                                pending.entry.clone(),
                            );
                        }
                        if let Err(error) = write_shards.send_task(
                            *shard,
                            WriteShardTask::WriteData {
                                file_id: *file_id,
                                bytes: payload,
                                finish: finish_file,
                            },
                            &mut self.stats,
                            &mut self.pending_file_metadata,
                            &mut self.completed_data_files,
                        ) {
                            if finish_file {
                                write_shards.forget_pending_completion(*file_id);
                            }
                            write_shards.release_file(*shard, pending.entry.size);
                            return Err(error);
                        }
                    }
                }
                (take, pending.remaining == 0)
            };

            self.stats.output_bytes_total =
                self.stats.output_bytes_total.saturating_add(take as u64);
            bytes = &bytes[take..];

            if finished_file {
                self.finalize_pending_file()?;
                if !bytes.is_empty() {
                    let decode_started = Instant::now();
                    if !self.ensure_pending_file_for_write()? {
                        return Err(OxideError::InvalidFormat(
                            "directory archive contains file data beyond manifest entries",
                        ));
                    }
                    self.stats.directory_decode += decode_started.elapsed();
                }
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
    use std::path::{Path, PathBuf};

    use super::*;
    use crate::{ArchiveListingEntry, ArchiveTimestamp, OxideError};

    fn file_entry(path: impl Into<String>, size: u64, content_offset: u64) -> ArchiveListingEntry {
        ArchiveListingEntry::file(
            path.into(),
            size,
            0o644,
            ArchiveTimestamp::default(),
            0,
            0,
            content_offset,
        )
    }

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
            active_file_counted: false,
        }
    }

    #[test]
    fn prepared_entry_windows_scale_with_parallelism() {
        let config = prepared_entry_window_config_for_counts(10_000, 10_000, 12);

        assert_eq!(
            config,
            PreparedEntryWindowConfig {
                preopened_file_window_capacity: 96,
                ready_entry_window_capacity: 48,
            }
        );
    }

    #[test]
    fn prepared_entry_windows_adapt_for_file_heavy_workloads() {
        let config = prepared_entry_window_config_for_counts(10_000, 5_000, 16);

        assert_eq!(
            config,
            PreparedEntryWindowConfig {
                preopened_file_window_capacity: 128,
                ready_entry_window_capacity: 64,
            }
        );
    }

    #[test]
    fn prepared_entry_windows_do_not_boost_sparse_file_workloads() {
        let config = prepared_entry_window_config_for_counts(10_000, 1_000, 16);

        assert_eq!(
            config,
            PreparedEntryWindowConfig {
                preopened_file_window_capacity: 128,
                ready_entry_window_capacity: 64,
            }
        );
    }

    #[test]
    fn prepared_entry_windows_only_boost_when_density_and_backlog_align() {
        let config = prepared_entry_window_config_for_counts(1_000, 240, 16);

        assert_eq!(
            config,
            PreparedEntryWindowConfig {
                preopened_file_window_capacity: 128,
                ready_entry_window_capacity: 64,
            }
        );
    }

    #[test]
    fn prepared_entry_windows_respect_safe_upper_clamp() {
        let config = prepared_entry_window_config_for_counts(10_000, 10_000, 64);

        assert_eq!(
            config,
            PreparedEntryWindowConfig {
                preopened_file_window_capacity: 128,
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
                preopened_file_window_capacity: 32,
                ready_entry_window_capacity: MIN_READY_ENTRY_WINDOW_CAPACITY,
            }
        );
    }

    #[test]
    fn restore_multiple_files_across_chunk_boundaries() {
        let temp = tempfile::tempdir().expect("tempdir");
        let manifest = ArchiveManifest::new(vec![
            file_entry("nested/first.txt", 4, 0),
            file_entry("nested/second.txt", 4, 4),
        ]);

        let mut writer =
            DirectoryRestoreWriter::create(temp.path(), manifest).expect("create writer");
        writer.write_chunk(b"te").expect("write first chunk");
        writer.write_chunk(b"stda").expect("write second chunk");
        writer.write_chunk(b"ta").expect("write third chunk");
        writer.finish().expect("finish restore");

        assert_eq!(
            std::fs::read(temp.path().join("nested/first.txt")).expect("read first"),
            b"test"
        );
        assert_eq!(
            std::fs::read(temp.path().join("nested/second.txt")).expect("read second"),
            b"data"
        );
    }

    #[test]
    fn restore_finishes_manifests_with_only_empty_entries() {
        let temp = tempfile::tempdir().expect("tempdir");
        let manifest = ArchiveManifest::new(vec![
            ArchiveListingEntry::directory(
                "nested".to_string(),
                0o755,
                ArchiveTimestamp::default(),
                0,
                0,
            ),
            file_entry("nested/empty.txt", 0, 0),
        ]);

        let mut writer =
            DirectoryRestoreWriter::create(temp.path(), manifest).expect("create writer");
        let stats = writer.finish().expect("finish restore");

        assert!(temp.path().join("nested").is_dir());
        assert_eq!(
            std::fs::metadata(temp.path().join("nested/empty.txt"))
                .expect("empty file metadata")
                .len(),
            0
        );
        assert_eq!(stats.entry_count, 2);
    }

    #[cfg(unix)]
    #[test]
    fn restore_finishes_manifests_with_symlink_entries_only() {
        let temp = tempfile::tempdir().expect("tempdir");
        let manifest = ArchiveManifest::new(vec![ArchiveListingEntry {
            path: "link".to_string(),
            kind: crate::ArchiveEntryKind::Symlink,
            target: Some("target.txt".to_string()),
            size: 0,
            mode: 0o777,
            mtime: ArchiveTimestamp::default(),
            uid: 0,
            gid: 0,
            content_offset: 0,
        }]);

        let mut writer =
            DirectoryRestoreWriter::create(temp.path(), manifest).expect("create writer");
        let stats = writer.finish().expect("finish restore");

        assert_eq!(
            std::fs::read_link(temp.path().join("link")).expect("read link"),
            Path::new("target.txt")
        );
        assert_eq!(stats.entry_count, 1);
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

    #[test]
    fn restore_finish_rejects_truncated_payload_mid_file() {
        let temp = tempfile::tempdir().expect("tempdir");
        let manifest = ArchiveManifest::new(vec![file_entry("nested/file.txt", 4, 0)]);

        let mut writer =
            DirectoryRestoreWriter::create(temp.path(), manifest).expect("create writer");
        writer.write_chunk(b"te").expect("write partial payload");

        let error = writer
            .finish()
            .expect_err("finish should reject truncation");
        assert!(matches!(
            error,
            OxideError::InvalidFormat("truncated file payload during directory restore")
        ));
    }

    #[test]
    fn restore_finish_rejects_missing_later_file_payload() {
        let temp = tempfile::tempdir().expect("tempdir");
        let manifest = ArchiveManifest::new(vec![
            file_entry("nested/first.txt", 4, 0),
            file_entry("nested/second.txt", 4, 4),
        ]);

        let mut writer =
            DirectoryRestoreWriter::create(temp.path(), manifest).expect("create writer");
        writer.write_chunk(b"test").expect("write first file");

        let error = writer
            .finish()
            .expect_err("finish should reject missing later payload");
        assert!(matches!(
            error,
            OxideError::InvalidFormat("directory restore ended before all entries completed")
        ));
    }

    #[test]
    fn restore_write_chunk_rejects_payload_beyond_manifest() {
        let temp = tempfile::tempdir().expect("tempdir");
        let manifest = ArchiveManifest::new(vec![file_entry("nested/file.txt", 4, 0)]);

        let mut writer =
            DirectoryRestoreWriter::create(temp.path(), manifest).expect("create writer");
        let error = writer
            .write_chunk(b"test!")
            .expect_err("write should reject extra payload");

        assert!(matches!(
            error,
            OxideError::InvalidFormat(
                "directory archive contains file data beyond manifest entries"
            )
        ));
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
