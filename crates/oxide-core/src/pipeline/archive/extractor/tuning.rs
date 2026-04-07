use super::*;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct ExtractBlockProfile {
    pub(super) max_encoded_len: usize,
    pub(super) max_raw_len: usize,
    pub(super) queue_block_bytes_p95: usize,
}

impl ExtractBlockProfile {
    pub(super) fn from_plan(headers: &[ChunkDescriptor], plan: &DecodePlan) -> Self {
        let mut profile = Self::default();
        let mut queue_block_bytes = Vec::with_capacity(plan.block_count());
        for (block_index, header) in headers.iter().enumerate() {
            if !plan.includes(block_index) {
                continue;
            }

            profile.max_encoded_len = profile.max_encoded_len.max(header.encoded_len as usize);
            profile.max_raw_len = profile.max_raw_len.max(header.raw_len as usize);
            queue_block_bytes.push((header.raw_len as usize).max(header.encoded_len as usize));
        }
        profile.queue_block_bytes_p95 = percentile_ceil(&mut queue_block_bytes, 95);
        profile
    }

    pub(super) fn max_queue_block_bytes(self) -> usize {
        self.max_raw_len.max(self.max_encoded_len).max(1)
    }

    pub(super) fn budgeted_queue_block_bytes(self) -> usize {
        self.queue_block_bytes_p95.max(1)
    }
}

fn percentile_ceil(values: &mut [usize], percentile: usize) -> usize {
    if values.is_empty() {
        return 0;
    }

    values.sort_unstable();
    let percentile = percentile.clamp(1, 100);
    let rank = values.len().saturating_mul(percentile).div_ceil(100);
    values[rank.saturating_sub(1)]
}

pub(super) fn select_extract_buffer_pool(
    base_pool: &Arc<BufferPool>,
    required_capacity: usize,
) -> Arc<BufferPool> {
    let required_capacity = required_capacity.max(1);
    if base_pool.default_capacity() >= required_capacity {
        return Arc::clone(base_pool);
    }

    Arc::new(BufferPool::new(required_capacity, base_pool.max_buffers()))
}

#[inline]
pub(super) fn extract_inflight_block_limit(
    block_capacity: usize,
    budgeted_block_bytes: usize,
    performance: &PipelinePerformanceOptions,
) -> usize {
    let budgeted_block_bytes = budgeted_block_bytes.max(1);
    let inflight_bytes = performance.max_inflight_bytes.max(budgeted_block_bytes);
    inflight_bytes
        .div_ceil(budgeted_block_bytes)
        .max(1)
        .min(block_capacity.max(1))
}

#[inline]
pub(super) fn decode_queue_capacity(
    worker_count: usize,
    block_capacity: usize,
    inflight_block_limit: usize,
) -> usize {
    worker_count
        .saturating_mul(DECODE_QUEUE_MULTIPLIER)
        .max(MIN_DECODE_QUEUE_CAPACITY)
        .min(inflight_block_limit.max(1))
        .min(block_capacity.max(1))
        .max(1)
}

#[inline]
pub(super) fn ordered_write_queue_capacity(
    worker_count: usize,
    decode_queue_capacity: usize,
    block_capacity: usize,
    inflight_block_limit: usize,
    block_profile: ExtractBlockProfile,
) -> usize {
    let worker_bounded = worker_count
        .saturating_mul(ORDERED_WRITE_QUEUE_MULTIPLIER)
        .max(MIN_ORDERED_WRITE_QUEUE_CAPACITY)
        .min(inflight_block_limit.max(1))
        .min(block_capacity.max(1))
        .max(1);

    let inflight_headroom = inflight_block_limit.saturating_sub(decode_queue_capacity);
    let adaptive_headroom = inflight_headroom
        .saturating_mul(ordered_write_headroom_share_numerator(block_profile))
        .div_ceil(ORDERED_WRITE_HEADROOM_SHARE_DENOMINATOR);
    let adaptive_bounded = decode_queue_capacity.saturating_add(adaptive_headroom);

    worker_bounded
        .max(adaptive_bounded)
        .max(decode_queue_capacity)
        .min(inflight_block_limit.max(1))
        .min(block_capacity.max(1))
}

#[inline]
pub(super) fn ordered_write_headroom_share_numerator(block_profile: ExtractBlockProfile) -> usize {
    let skew_ratio = block_profile
        .max_queue_block_bytes()
        .div_ceil(block_profile.budgeted_queue_block_bytes());

    match skew_ratio {
        0..=2 => 3,
        3..=4 => 2,
        _ => 1,
    }
}

#[inline]
pub(super) fn reorder_pending_limit(
    ordered_write_queue_capacity: usize,
    block_capacity: usize,
    inflight_block_limit: usize,
) -> usize {
    ordered_write_queue_capacity
        .saturating_mul(REORDER_PENDING_MULTIPLIER)
        .min(inflight_block_limit.max(1))
        .min(block_capacity.max(1))
        .max(1)
}

pub(super) fn apply_directory_restore_stats(
    pipeline_stats: &mut ExtractPipelineStats,
    stage_timings: &mut ExtractStageTimings,
    restore_stats: &DirectoryRestoreStats,
) {
    stage_timings.ordered_write = stage_timings
        .ordered_write
        .saturating_sub(restore_stats.ordered_write_time);
    stage_timings.directory_decode += restore_stats.directory_decode;
    stage_timings.prepared_file_open += restore_stats.prepared_file_open;
    stage_timings.prepared_file_permit_wait += restore_stats.prepared_file_permit_wait;
    stage_timings.prepared_file_wait += restore_stats.prepared_file_wait;
    stage_timings.output_prepare_directories += restore_stats.output_prepare_directories;
    stage_timings.output_write += restore_stats.output_write;
    stage_timings.output_create += restore_stats.output_create;
    stage_timings.output_create_directories += restore_stats.output_create_directories;
    stage_timings.output_create_files += restore_stats.output_create_files;
    stage_timings.output_data += restore_stats.output_data;
    stage_timings.output_flush += restore_stats.output_flush;
    stage_timings.output_metadata += restore_stats.output_metadata;
    stage_timings.output_metadata_files += restore_stats.output_metadata_files;
    stage_timings.output_metadata_directories += restore_stats.output_metadata_directories;
    stage_timings.file_transition_wait += restore_stats.file_transition_wait;
    if restore_stats.write_shard_count > 1 {
        pipeline_stats.write_shard_count = restore_stats.write_shard_count;
        pipeline_stats.write_shard_queue_peak = restore_stats.write_shard_queue_peak.clone();
        stage_timings.write_shard_blocked = restore_stats.write_shard_blocked.clone();
        stage_timings.write_shard_output_data = restore_stats.write_shard_output_data.clone();
    } else {
        pipeline_stats.set_write_shard_count(restore_stats.write_shard_count.max(1));
        for (shard, elapsed) in restore_stats
            .write_shard_output_data
            .iter()
            .copied()
            .enumerate()
        {
            stage_timings.record_write_shard_output_data(shard, elapsed);
        }
    }
    pipeline_stats.record_ready_file_frontier(restore_stats.ready_file_frontier);
    pipeline_stats.record_planner_ready_queue_peak(restore_stats.planner_ready_queue_peak);
    pipeline_stats.record_active_files_peak(restore_stats.active_files_peak);
}
