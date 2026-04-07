use super::*;

#[derive(Debug)]
pub(super) struct DecodeStreamOutcome {
    pub(super) flags: u32,
    pub(super) decoded_bytes_total: u64,
    pub(super) archive_bytes_total: u64,
    pub(super) blocks_total: u32,
    pub(super) workers: Vec<WorkerRuntimeSnapshot>,
    pub(super) stage_timings: ExtractStageTimings,
    pub(super) pipeline_stats: ExtractPipelineStats,
}

#[derive(Debug)]
pub(super) enum DecodedBlock {
    Owned(Vec<u8>),
    Pooled(PooledBuffer),
}

impl DecodedBlock {
    fn len(&self) -> usize {
        self.as_ref().len()
    }
}

impl AsRef<[u8]> for DecodedBlock {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Owned(bytes) => bytes.as_slice(),
            Self::Pooled(bytes) => bytes.as_slice(),
        }
    }
}

impl From<DecodedBlock> for OwnedChunk {
    fn from(value: DecodedBlock) -> Self {
        match value {
            DecodedBlock::Owned(bytes) => Self::Owned(bytes),
            DecodedBlock::Pooled(bytes) => Self::Pooled(bytes),
        }
    }
}

pub(super) enum OrderedWriteTask {
    Block { index: usize, bytes: DecodedBlock },
    Abort,
}

#[derive(Debug, Default, Clone, Copy)]
pub(super) struct OrderedWriteForwardStats {
    pub(super) ordered_write_queue_peak: usize,
    pub(super) blocked_elapsed: Duration,
}

pub(super) struct OrderedWriterOutcome<W> {
    pub(super) writer: W,
    pub(super) stats: ReorderWriterStats,
}

pub(super) enum ProcessedDecodeResult {
    Block { index: usize, bytes: DecodedBlock },
    NewError,
    IgnoredAfterError,
}

pub(super) struct DecodeResultContext<'a> {
    pub(super) stage_timings: &'a mut ExtractStageTimings,
    pub(super) total_blocks: usize,
    pub(super) received_indices: &'a mut [bool],
    pub(super) runtime_state: &'a DecodeRuntimeState,
    pub(super) decoded_bytes_completed: &'a mut u64,
    pub(super) received: &'a mut usize,
    pub(super) first_error: &'a mut Option<crate::OxideError>,
}

pub(super) fn receive_decode_result(
    result_rx: &Receiver<(usize, Duration, Result<DecodedBlock>)>,
    ctx: &mut DecodeResultContext<'_>,
) -> Result<ProcessedDecodeResult> {
    let wait_started = Instant::now();
    let result = result_rx.recv().map_err(|_| {
        crate::OxideError::CompressionError(
            "decode result channel closed before completion".to_string(),
        )
    })?;
    ctx.stage_timings.decode_wait += wait_started.elapsed();
    ctx.stage_timings.archive_read += result.1;
    process_decode_result(
        result,
        ctx.total_blocks,
        ctx.received_indices,
        ctx.runtime_state,
        ctx.decoded_bytes_completed,
        ctx.received,
        ctx.first_error,
    )
}

pub(super) fn process_decode_result(
    (index, _read_elapsed, block): (usize, Duration, Result<DecodedBlock>),
    total_blocks: usize,
    received_indices: &mut [bool],
    runtime_state: &DecodeRuntimeState,
    decoded_bytes_completed: &mut u64,
    received: &mut usize,
    first_error: &mut Option<crate::OxideError>,
) -> Result<ProcessedDecodeResult> {
    if index >= total_blocks || index >= received_indices.len() {
        return Err(crate::OxideError::InvalidFormat(
            "decode result index out of bounds",
        ));
    }
    if received_indices[index] {
        return Err(crate::OxideError::InvalidFormat(
            "duplicate decode result index",
        ));
    }
    received_indices[index] = true;
    *received += 1;
    runtime_state.record_completion();

    match block {
        Ok(bytes) => {
            *decoded_bytes_completed = decoded_bytes_completed.saturating_add(bytes.len() as u64);
            if first_error.is_none() {
                Ok(ProcessedDecodeResult::Block { index, bytes })
            } else {
                Ok(ProcessedDecodeResult::IgnoredAfterError)
            }
        }
        Err(error) => {
            let is_new_error = first_error.is_none();
            first_error.get_or_insert(error);
            if is_new_error {
                Ok(ProcessedDecodeResult::NewError)
            } else {
                Ok(ProcessedDecodeResult::IgnoredAfterError)
            }
        }
    }
}

pub(super) fn forward_processed_decode_result(
    outcome: ProcessedDecodeResult,
    ordered_write_tx: &mut Option<crossbeam_channel::Sender<OrderedWriteTask>>,
) -> Result<()> {
    match outcome {
        ProcessedDecodeResult::Block { index, bytes } => {
            if let Some(tx) = ordered_write_tx.as_ref() {
                let task = OrderedWriteTask::Block { index, bytes };
                match tx.try_send(task) {
                    Ok(()) => {}
                    Err(TrySendError::Full(task)) => {
                        tx.send(task).map_err(|_| {
                            *ordered_write_tx = None;
                            crate::OxideError::CompressionError(
                                "ordered write forward queue closed before completion".to_string(),
                            )
                        })?;
                    }
                    Err(TrySendError::Disconnected(_)) => {
                        *ordered_write_tx = None;
                        return Err(crate::OxideError::CompressionError(
                            "ordered write forward queue closed before completion".to_string(),
                        ));
                    }
                }
            }
        }
        ProcessedDecodeResult::NewError => {
            abort_ordered_writer(ordered_write_tx);
        }
        ProcessedDecodeResult::IgnoredAfterError => {}
    }
    Ok(())
}

pub(super) fn spawn_ordered_write_forwarder(
    ordered_write_forward_rx: Receiver<OrderedWriteTask>,
    ordered_write_tx: Sender<OrderedWriteTask>,
) -> thread::JoinHandle<Result<OrderedWriteForwardStats>> {
    thread::spawn(move || -> Result<OrderedWriteForwardStats> {
        let mut stats = OrderedWriteForwardStats::default();

        while let Ok(task) = ordered_write_forward_rx.recv() {
            match task {
                OrderedWriteTask::Block { index, bytes } => {
                    let task = OrderedWriteTask::Block { index, bytes };
                    match ordered_write_tx.try_send(task) {
                        Ok(()) => {
                            stats.ordered_write_queue_peak =
                                stats.ordered_write_queue_peak.max(ordered_write_tx.len());
                        }
                        Err(TrySendError::Full(task)) => {
                            let blocked_started = Instant::now();
                            ordered_write_tx.send(task).map_err(|_| {
                                crate::OxideError::CompressionError(
                                    "ordered write queue closed before completion".to_string(),
                                )
                            })?;
                            stats.blocked_elapsed += blocked_started.elapsed();
                            stats.ordered_write_queue_peak =
                                stats.ordered_write_queue_peak.max(ordered_write_tx.len());
                        }
                        Err(TrySendError::Disconnected(_)) => {
                            return Err(crate::OxideError::CompressionError(
                                "ordered write queue closed before completion".to_string(),
                            ));
                        }
                    }
                }
                OrderedWriteTask::Abort => {
                    let _ = ordered_write_tx.send(OrderedWriteTask::Abort);
                    return Ok(stats);
                }
            }
        }

        Ok(stats)
    })
}

pub(super) fn abort_ordered_writer(
    ordered_write_tx: &mut Option<crossbeam_channel::Sender<OrderedWriteTask>>,
) {
    if let Some(tx) = ordered_write_tx.take() {
        let _ = tx.send(OrderedWriteTask::Abort);
    }
}

#[cfg(unix)]
pub(super) fn read_exact_file_at(
    file: &fs::File,
    mut offset: u64,
    buffer: &mut [u8],
) -> std::io::Result<()> {
    use std::io::{Error, ErrorKind};
    use std::os::unix::fs::FileExt;

    let mut filled = 0usize;
    while filled < buffer.len() {
        let read = file.read_at(&mut buffer[filled..], offset)?;
        if read == 0 {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "unexpected EOF while reading archive block",
            ));
        }
        filled += read;
        offset = offset.saturating_add(read as u64);
    }
    Ok(())
}

#[cfg(windows)]
pub(super) fn read_exact_file_at(
    file: &fs::File,
    mut offset: u64,
    buffer: &mut [u8],
) -> std::io::Result<()> {
    use std::io::{Error, ErrorKind};
    use std::os::windows::fs::FileExt;

    let mut filled = 0usize;
    while filled < buffer.len() {
        let read = file.seek_read(&mut buffer[filled..], offset)?;
        if read == 0 {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "unexpected EOF while reading archive block",
            ));
        }
        filled += read;
        offset = offset.saturating_add(read as u64);
    }
    Ok(())
}

#[cfg(not(any(unix, windows)))]
pub(super) fn read_exact_file_at(
    _file: &fs::File,
    _offset: u64,
    _buffer: &mut [u8],
) -> std::io::Result<()> {
    Err(std::io::Error::other(
        "parallel offset reads are not supported on this platform",
    ))
}

pub(super) fn join_decode_workers(
    handles: Vec<thread::JoinHandle<DecodeWorkerOutcome>>,
) -> Result<Vec<WorkerRuntimeSnapshot>> {
    let mut workers = Vec::with_capacity(handles.len());
    for handle in handles {
        let outcome = handle.join().map_err(|payload| {
            let details = if let Some(message) = payload.downcast_ref::<&str>() {
                (*message).to_string()
            } else if let Some(message) = payload.downcast_ref::<String>() {
                message.clone()
            } else {
                "unknown panic payload".to_string()
            };
            crate::OxideError::CompressionError(format!("decode worker thread panicked: {details}"))
        })?;
        let busy = outcome.busy.min(outcome.uptime);
        let idle = outcome.uptime.saturating_sub(busy);
        let utilization = if outcome.uptime == Duration::ZERO {
            0.0
        } else {
            busy.as_secs_f64() / outcome.uptime.as_secs_f64()
        };
        workers.push(WorkerRuntimeSnapshot {
            worker_id: outcome.worker_id,
            tasks_completed: outcome.tasks_completed,
            uptime: outcome.uptime,
            busy,
            idle,
            utilization,
        });
    }
    workers.sort_by_key(|worker| worker.worker_id);
    Ok(workers)
}

pub(super) fn join_io_readers(handles: Vec<thread::JoinHandle<Result<()>>>) -> Result<()> {
    for handle in handles {
        handle.join().map_err(|payload| {
            let details = if let Some(message) = payload.downcast_ref::<&str>() {
                (*message).to_string()
            } else if let Some(message) = payload.downcast_ref::<String>() {
                message.clone()
            } else {
                "unknown panic payload".to_string()
            };
            crate::OxideError::CompressionError(format!("I/O reader thread panicked: {details}"))
        })??;
    }
    Ok(())
}

pub(super) fn spawn_ordered_writer<W>(
    writer: W,
    ordered_write_rx: Receiver<OrderedWriteTask>,
    reorder_pending_limit: usize,
    expected_blocks: usize,
) -> thread::JoinHandle<Result<OrderedWriterOutcome<W>>>
where
    W: OrderedChunkWriter + Send + 'static,
{
    thread::spawn(move || {
        let mut reorder = BoundedReorderWriter::with_limit(writer, reorder_pending_limit);

        while let Ok(task) = ordered_write_rx.recv() {
            match task {
                OrderedWriteTask::Block { index, bytes } => {
                    reorder.push(index, bytes)?;
                }
                OrderedWriteTask::Abort => {
                    let (writer, stats) = reorder.into_parts();
                    return Ok(OrderedWriterOutcome { writer, stats });
                }
            }
        }

        let (writer, stats) = reorder.finish(expected_blocks)?;
        Ok(OrderedWriterOutcome { writer, stats })
    })
}

pub(super) fn join_ordered_writer<W>(
    handle: thread::JoinHandle<Result<OrderedWriterOutcome<W>>>,
) -> Result<OrderedWriterOutcome<W>> {
    handle.join().map_err(|payload| {
        let details = if let Some(message) = payload.downcast_ref::<&str>() {
            (*message).to_string()
        } else if let Some(message) = payload.downcast_ref::<String>() {
            message.clone()
        } else {
            "unknown panic payload".to_string()
        };
        crate::OxideError::CompressionError(format!("ordered writer thread panicked: {details}"))
    })?
}

pub(super) fn join_ordered_write_forwarder(
    handle: thread::JoinHandle<Result<OrderedWriteForwardStats>>,
) -> Result<OrderedWriteForwardStats> {
    handle.join().map_err(|payload| {
        let details = if let Some(message) = payload.downcast_ref::<&str>() {
            (*message).to_string()
        } else if let Some(message) = payload.downcast_ref::<String>() {
            message.clone()
        } else {
            "unknown panic payload".to_string()
        };
        crate::OxideError::CompressionError(format!(
            "ordered write forwarder thread panicked: {details}"
        ))
    })?
}

pub fn decode_block_payload(header: ChunkDescriptor, block_data: Vec<u8>) -> Result<Vec<u8>> {
    let mut scratch = CompressionScratchArena::new();
    let dictionary_bank = ArchiveDictionaryBank::default();
    let compression_meta = header.compression_meta()?;
    let decoded = if compression_meta.raw_passthrough {
        block_data
    } else if crate::compression::supports_direct_buffer_output(compression_meta.algo) {
        let mut decoded = Vec::new();
        crate::compression::reverse_compression_request_with_scratch_into(
            crate::compression::DecompressionRequest {
                data: &block_data,
                algo: compression_meta.algo,
                raw_len: Some(header.raw_len as usize),
                dictionary_id: compression_meta.dictionary_id,
                dictionary: dictionary_bank
                    .dictionary_bytes(compression_meta.dictionary_id, compression_meta.algo),
            },
            &mut scratch,
            &mut decoded,
        )?;
        decoded
    } else {
        crate::compression::reverse_compression_request_with_scratch(
            crate::compression::DecompressionRequest {
                data: &block_data,
                algo: compression_meta.algo,
                raw_len: Some(header.raw_len as usize),
                dictionary_id: compression_meta.dictionary_id,
                dictionary: dictionary_bank
                    .dictionary_bytes(compression_meta.dictionary_id, compression_meta.algo),
            },
            &mut scratch,
        )?
    };
    if decoded.len() != header.raw_len as usize {
        return Err(crate::OxideError::InvalidFormat(
            "decoded block size mismatch",
        ));
    }
    Ok(decoded)
}

pub(super) fn decode_block_payload_with_scratch(
    header: ChunkDescriptor,
    block_data: PooledBuffer,
    scratch: &mut CompressionScratchArena,
    pool: &BufferPool,
    dictionary_bank: &ArchiveDictionaryBank,
) -> Result<DecodedBlock> {
    let compression_meta = header.compression_meta()?;
    let decoded = if compression_meta.raw_passthrough {
        DecodedBlock::Pooled(block_data)
    } else if crate::compression::supports_direct_buffer_output(compression_meta.algo) {
        let mut decoded = pool.acquire_with_capacity(header.raw_len as usize);
        crate::compression::reverse_compression_request_with_scratch_into(
            crate::compression::DecompressionRequest {
                data: block_data.as_slice(),
                algo: compression_meta.algo,
                raw_len: Some(header.raw_len as usize),
                dictionary_id: compression_meta.dictionary_id,
                dictionary: dictionary_bank
                    .dictionary_bytes(compression_meta.dictionary_id, compression_meta.algo),
            },
            scratch,
            decoded.as_mut_vec(),
        )?;
        DecodedBlock::Pooled(decoded)
    } else {
        DecodedBlock::Owned(
            crate::compression::reverse_compression_request_with_scratch(
                crate::compression::DecompressionRequest {
                    data: block_data.as_slice(),
                    algo: compression_meta.algo,
                    raw_len: Some(header.raw_len as usize),
                    dictionary_id: compression_meta.dictionary_id,
                    dictionary: dictionary_bank
                        .dictionary_bytes(compression_meta.dictionary_id, compression_meta.algo),
                },
                scratch,
            )?,
        )
    };
    if decoded.len() != header.raw_len as usize {
        return Err(crate::OxideError::InvalidFormat(
            "decoded block size mismatch",
        ));
    }
    Ok(decoded)
}
