use std::fs;
use std::io::{Read, Seek};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use crossbeam_channel::{Receiver, bounded};

use crate::core::WorkerRuntimeSnapshot;
use crate::format::{ArchiveReader, BlockHeader, FOOTER_SIZE};
use crate::telemetry::{RunTelemetryOptions, TelemetryEvent, TelemetrySink};
use crate::types::Result;
use super::directory;
use super::types::*;
use super::telemetry::*;
use super::archiver::container_prefix_bytes;
use super::super::types::ArchiveSourceKind;

pub struct Extractor {
    pub num_workers: usize,
}

impl Extractor {
    pub fn new(num_workers: usize) -> Self {
        Self { num_workers }
    }

    pub fn read_archive_payload_with_metrics<R: Read + Seek>(
        &self,
        reader: R,
        started_at: Instant,
        options: &RunTelemetryOptions,
        sink: &mut dyn TelemetrySink,
    ) -> Result<DecodedArchivePayload> {
        let mut archive = ArchiveReader::new(reader)?;
        let flags = u32::from(archive.global_header().feature_bits);
        let source_kind = directory::source_kind_from_flags(flags);
        let block_capacity = archive.block_count() as usize;
        let mut expected_total = 0usize;
        let worker_count = self.num_workers.max(1);
        let queue_capacity = worker_count.saturating_mul(4).max(1);
        let (task_tx, task_rx) = bounded::<DecodeTask>(queue_capacity);
        let (result_tx, result_rx) = bounded::<(usize, Result<Vec<u8>>)>(queue_capacity);
        let runtime_state = Arc::new(DecodeRuntimeState::new(worker_count, started_at));
        let mut worker_handles = Vec::with_capacity(worker_count);

        for worker_id in 0..worker_count {
            let local_task_rx = task_rx.clone();
            let local_result_tx = result_tx.clone();
            let local_runtime = Arc::clone(&runtime_state);
            let handle = thread::spawn(move || -> DecodeWorkerOutcome {
                let started = Instant::now();
                let mut tasks_completed = 0usize;
                let mut busy = Duration::ZERO;
                local_runtime.mark_worker_started(worker_id);

                while let Ok(task) = local_task_rx.recv() {
                    let decode_started = Instant::now();
                    let decoded = decode_block_payload(task.header, task.block_data);
                    let busy_elapsed = decode_started.elapsed();
                    busy += busy_elapsed;
                    local_runtime.record_worker_task(worker_id, busy_elapsed);
                    tasks_completed += 1;
                    if local_result_tx.send((task.index, decoded)).is_err() {
                        break;
                    }
                }
                local_runtime.mark_worker_stopped(worker_id);

                DecodeWorkerOutcome {
                    worker_id,
                    tasks_completed,
                    busy,
                    uptime: started.elapsed(),
                }
            });
            worker_handles.push(handle);
        }
        drop(result_tx);

        let mut stage_timings = ExtractStageTimings::default();
        let mut archive_bytes_total =
            container_prefix_bytes(block_capacity as u32) + FOOTER_SIZE as u64;
        let mut submitted = 0usize;
        let mut received = 0usize;
        let mut first_error: Option<crate::OxideError> = None;
        let mut decoded_bytes_completed = 0u64;
        let mut decoded_blocks = vec![None; block_capacity];
        let mut last_emit_at = Instant::now();
        let emit_every = options.progress_interval.max(Duration::from_millis(100));
        for entry in archive.iter_blocks() {
            let read_started = Instant::now();
            let (header, block_data) = entry?;
            stage_timings.archive_read += read_started.elapsed();
            expected_total = expected_total.saturating_add(header.raw_len as usize);
            archive_bytes_total = archive_bytes_total.saturating_add(block_data.len() as u64);

            while submitted.saturating_sub(received) >= queue_capacity {
                receive_decode_result(
                    &result_rx,
                    &mut stage_timings,
                    &mut decoded_blocks,
                    &runtime_state,
                    &mut decoded_bytes_completed,
                    &mut received,
                    &mut first_error,
                )?;
                emit_extract_progress_if_due(
                    source_kind,
                    started_at,
                    archive_bytes_total,
                    decoded_bytes_completed,
                    block_capacity as u32,
                    received as u32,
                    runtime_state.snapshot(),
                    emit_every,
                    &mut last_emit_at,
                    false,
                    sink,
                );
            }

            let submit_started = Instant::now();
            task_tx
                .send(DecodeTask {
                    index: submitted,
                    header,
                    block_data,
                })
                .map_err(|_| {
                    crate::OxideError::CompressionError(
                        "decode queue closed before submission completed".to_string(),
                    )
                })?;
            stage_timings.decode_submit += submit_started.elapsed();
            submitted += 1;
            runtime_state.record_submission();
            emit_extract_progress_if_due(
                source_kind,
                started_at,
                archive_bytes_total,
                decoded_bytes_completed,
                block_capacity as u32,
                received as u32,
                runtime_state.snapshot(),
                emit_every,
                &mut last_emit_at,
                false,
                sink,
            );
        }
        drop(task_tx);

        if submitted != block_capacity {
            return Err(crate::OxideError::InvalidFormat(
                "archive block count mismatch during decode",
            ));
        }

        while received < submitted {
            receive_decode_result(
                &result_rx,
                &mut stage_timings,
                &mut decoded_blocks,
                &runtime_state,
                &mut decoded_bytes_completed,
                &mut received,
                &mut first_error,
            )?;
            emit_extract_progress_if_due(
                source_kind,
                started_at,
                archive_bytes_total,
                decoded_bytes_completed,
                block_capacity as u32,
                received as u32,
                runtime_state.snapshot(),
                emit_every,
                &mut last_emit_at,
                false,
                sink,
            );
        }

        let workers = join_decode_workers(worker_handles)?;
        if let Some(error) = first_error {
            return Err(error);
        }

        let merge_started = Instant::now();
        let mut output = Vec::with_capacity(expected_total);
        for block in decoded_blocks {
            let block = block.ok_or(crate::OxideError::InvalidFormat(
                "missing decoded block payload",
            ))?;
            output.extend_from_slice(&block);
        }
        stage_timings.merge += merge_started.elapsed();

        if options.emit_final_progress {
            emit_extract_progress(
                source_kind,
                started_at,
                archive_bytes_total,
                decoded_bytes_completed,
                block_capacity as u32,
                received as u32,
                runtime_state.snapshot(),
                sink,
            );
        }

        Ok(DecodedArchivePayload {
            flags,
            payload: output,
            archive_bytes_total,
            blocks_total: submitted as u32,
            workers,
            stage_timings,
        })
    }

    pub fn restore_decoded_payload(
        &self,
        output_path: &Path,
        decoded: &mut DecodedArchivePayload,
        extensions: &mut BTreeMap<String, ReportValue>,
    ) -> Result<(ArchiveSourceKind, u64)> {
        let directory_decode_started = Instant::now();
        if let Some(entries) = directory::decode_directory_entries(&decoded.payload, decoded.flags)?
        {
            decoded.stage_timings.directory_decode += directory_decode_started.elapsed();
            let output_bytes_total = entries
                .iter()
                .filter_map(|entry| match entry {
                    directory::DirectoryBundleEntry::File { data, .. } => Some(data.len() as u64),
                    directory::DirectoryBundleEntry::Directory { .. } => None,
                })
                .sum();
            extensions.insert(
                "extract.directory_entries".to_string(),
                ReportValue::U64(entries.len() as u64),
            );

            let write_started = Instant::now();
            directory::write_directory_entries(output_path, entries)?;
            decoded.stage_timings.output_write += write_started.elapsed();
            Ok((ArchiveSourceKind::Directory, output_bytes_total))
        } else {
            decoded.stage_timings.directory_decode += directory_decode_started.elapsed();
            if let Some(parent) = output_path
                .parent()
                .filter(|path| !path.as_os_str().is_empty())
            {
                fs::create_dir_all(parent)?;
            }
            let write_started = Instant::now();
            fs::write(output_path, &decoded.payload)?;
            decoded.stage_timings.output_write += write_started.elapsed();
            Ok((ArchiveSourceKind::File, decoded.payload.len() as u64))
        }
    }
}

pub fn receive_decode_result(
    result_rx: &Receiver<(usize, Result<Vec<u8>>)>,
    stage_timings: &mut ExtractStageTimings,
    decoded_blocks: &mut [Option<Vec<u8>>],
    runtime_state: &DecodeRuntimeState,
    decoded_bytes_completed: &mut u64,
    received: &mut usize,
    first_error: &mut Option<crate::OxideError>,
) -> Result<()> {
    let wait_started = Instant::now();
    let (index, block) = result_rx.recv().map_err(|_| {
        crate::OxideError::CompressionError(
            "decode result channel closed before completion".to_string(),
        )
    })?;
    stage_timings.decode_wait += wait_started.elapsed();
    if index >= decoded_blocks.len() {
        return Err(crate::OxideError::InvalidFormat(
            "decode result index out of bounds",
        ));
    }
    match block {
        Ok(bytes) => {
            if decoded_blocks[index].is_some() {
                return Err(crate::OxideError::InvalidFormat(
                    "duplicate decode result index",
                ));
            }
            *decoded_bytes_completed =
                decoded_bytes_completed.saturating_add(bytes.len() as u64);
            decoded_blocks[index] = Some(bytes);
        }
        Err(error) => {
            if first_error.is_none() {
                *first_error = Some(error);
            }
        }
    }
    *received += 1;
    runtime_state.record_completion();
    Ok(())
}

pub fn join_decode_workers(
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
            crate::OxideError::CompressionError(format!(
                "decode worker thread panicked: {details}"
            ))
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

pub fn decode_block_payload(header: BlockHeader, block_data: Vec<u8>) -> Result<Vec<u8>> {
    let compression_meta = header.compression_meta()?;
    let decoded = if compression_meta.raw_passthrough {
        block_data
    } else {
        crate::compression::reverse_compression(&block_data, compression_meta.algo)?
    };
    let strategy = header.strategy()?;
    let restored = crate::preprocessing::reverse_preprocessing(&decoded, &strategy)?;
    if restored.len() != header.raw_len as usize {
        return Err(crate::OxideError::InvalidFormat(
            "decoded block size mismatch",
        ));
    }
    Ok(restored)
}
