use std::time::{Duration, Instant};

use crate::format::ReorderBuffer;
use crate::types::Result;

pub trait OrderedChunkWriter {
    fn write_chunk(&mut self, bytes: &[u8]) -> Result<()>;
}

impl<T: OrderedChunkWriter + ?Sized> OrderedChunkWriter for &mut T {
    fn write_chunk(&mut self, bytes: &[u8]) -> Result<()> {
        (**self).write_chunk(bytes)
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ReorderPushStats {
    pub wrote_blocks: usize,
    pub wrote_bytes: u64,
    pub write_elapsed: Duration,
    pub pending_blocks: usize,
    pub pending_bytes: u64,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ReorderWriterStats {
    pub wrote_blocks: usize,
    pub wrote_bytes: u64,
    pub write_elapsed: Duration,
    pub pending_blocks_peak: usize,
    pub pending_bytes_peak: u64,
}

#[derive(Debug)]
pub struct BoundedReorderWriter<W, T = Vec<u8>> {
    writer: W,
    reorder: ReorderBuffer<(T, usize)>,
    pending_bytes: u64,
    stats: ReorderWriterStats,
}

impl<W: OrderedChunkWriter, T: AsRef<[u8]>> BoundedReorderWriter<W, T> {
    pub fn with_limit(writer: W, max_pending: usize) -> Self {
        Self {
            writer,
            reorder: ReorderBuffer::with_limit(max_pending.max(1)),
            pending_bytes: 0,
            stats: ReorderWriterStats::default(),
        }
    }

    pub fn push(&mut self, id: usize, item: T) -> Result<ReorderPushStats> {
        let item_len = item.as_ref().len();
        self.pending_bytes = self.pending_bytes.saturating_add(item_len as u64);

        let ready = match self.reorder.push(id, (item, item_len)) {
            Ok(ready) => ready,
            Err(error) => {
                self.pending_bytes = self.pending_bytes.saturating_sub(item_len as u64);
                return Err(error);
            }
        };

        self.stats.pending_blocks_peak = self
            .stats
            .pending_blocks_peak
            .max(self.reorder.pending_len());
        self.stats.pending_bytes_peak = self.stats.pending_bytes_peak.max(self.pending_bytes);

        let mut push_stats = ReorderPushStats {
            pending_blocks: self.reorder.pending_len(),
            pending_bytes: self.pending_bytes,
            ..ReorderPushStats::default()
        };

        for (chunk, len) in ready {
            self.pending_bytes = self.pending_bytes.saturating_sub(len as u64);

            let write_started = Instant::now();
            self.writer.write_chunk(chunk.as_ref())?;
            let write_elapsed = write_started.elapsed();

            push_stats.wrote_blocks = push_stats.wrote_blocks.saturating_add(1);
            push_stats.wrote_bytes = push_stats.wrote_bytes.saturating_add(len as u64);
            push_stats.write_elapsed += write_elapsed;
        }

        push_stats.pending_blocks = self.reorder.pending_len();
        push_stats.pending_bytes = self.pending_bytes;

        self.stats.wrote_blocks = self
            .stats
            .wrote_blocks
            .saturating_add(push_stats.wrote_blocks);
        self.stats.wrote_bytes = self
            .stats
            .wrote_bytes
            .saturating_add(push_stats.wrote_bytes);
        self.stats.write_elapsed += push_stats.write_elapsed;
        self.stats.pending_blocks_peak = self
            .stats
            .pending_blocks_peak
            .max(push_stats.pending_blocks);
        self.stats.pending_bytes_peak = self.stats.pending_bytes_peak.max(push_stats.pending_bytes);

        Ok(push_stats)
    }

    pub fn finish(self, expected_blocks: usize) -> Result<(W, ReorderWriterStats)> {
        if self.stats.wrote_blocks != expected_blocks {
            return Err(crate::OxideError::InvalidFormat(
                "decoded writer did not emit all expected blocks",
            ));
        }
        if self.reorder.pending_len() > 0 || self.pending_bytes != 0 {
            return Err(crate::OxideError::InvalidFormat(
                "decoded reorder writer closed with pending blocks",
            ));
        }
        Ok((self.writer, self.stats))
    }

    pub fn pending_len(&self) -> usize {
        self.reorder.pending_len()
    }
}
