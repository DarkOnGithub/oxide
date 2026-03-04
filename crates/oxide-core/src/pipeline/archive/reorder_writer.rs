use std::collections::BTreeMap;
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
pub struct BoundedReorderWriter<W> {
    writer: W,
    reorder: ReorderBuffer<Vec<u8>>,
    pending_sizes: BTreeMap<usize, usize>,
    next_write_id: usize,
    pending_bytes: u64,
    stats: ReorderWriterStats,
}

impl<W: OrderedChunkWriter> BoundedReorderWriter<W> {
    pub fn with_limit(writer: W, max_pending: usize) -> Self {
        Self {
            writer,
            reorder: ReorderBuffer::with_limit(max_pending.max(1)),
            pending_sizes: BTreeMap::new(),
            next_write_id: 0,
            pending_bytes: 0,
            stats: ReorderWriterStats::default(),
        }
    }

    pub fn push(&mut self, id: usize, item: Vec<u8>) -> Result<ReorderPushStats> {
        if self.pending_sizes.contains_key(&id) {
            return Err(crate::OxideError::InvalidFormat(
                "duplicate decoded block id in reorder writer",
            ));
        }

        let item_len = item.len();
        self.pending_sizes.insert(id, item_len);
        self.pending_bytes = self.pending_bytes.saturating_add(item_len as u64);

        let ready = match self.reorder.push(id, item) {
            Ok(ready) => ready,
            Err(error) => {
                self.pending_sizes.remove(&id);
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

        for chunk in ready {
            let expected_id = self.next_write_id;
            let len =
                self.pending_sizes
                    .remove(&expected_id)
                    .ok_or(crate::OxideError::InvalidFormat(
                        "decoded reorder pending state drift detected",
                    ))?;
            self.pending_bytes = self.pending_bytes.saturating_sub(len as u64);

            let write_started = Instant::now();
            self.writer.write_chunk(&chunk)?;
            let write_elapsed = write_started.elapsed();

            self.next_write_id = self.next_write_id.saturating_add(1);

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
        if self.next_write_id != expected_blocks {
            return Err(crate::OxideError::InvalidFormat(
                "decoded writer did not emit all expected blocks",
            ));
        }
        if !self.pending_sizes.is_empty() || self.reorder.pending_len() > 0 {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct CollectWriter {
        output: Vec<u8>,
    }

    impl OrderedChunkWriter for CollectWriter {
        fn write_chunk(&mut self, bytes: &[u8]) -> Result<()> {
            self.output.extend_from_slice(bytes);
            Ok(())
        }
    }

    #[test]
    fn flushes_blocks_in_id_order() {
        let writer = CollectWriter::default();
        let mut reorder = BoundedReorderWriter::with_limit(writer, 4);

        reorder.push(1, vec![2, 2]).expect("push id=1");
        reorder.push(0, vec![1]).expect("push id=0");
        reorder.push(2, vec![3]).expect("push id=2");

        let (writer, stats) = reorder.finish(3).expect("finish");
        assert_eq!(writer.output, vec![1, 2, 2, 3]);
        assert_eq!(stats.wrote_blocks, 3);
        assert_eq!(stats.wrote_bytes, 4);
        assert!(stats.pending_blocks_peak >= 1);
    }

    #[test]
    fn enforces_pending_limit_for_out_of_order_pushes() {
        let writer = CollectWriter::default();
        let mut reorder = BoundedReorderWriter::with_limit(writer, 1);

        reorder.push(1, vec![1]).expect("first out-of-order push");
        let error = reorder.push(2, vec![2]).expect_err("limit should fail");
        assert!(matches!(
            error,
            crate::OxideError::InvalidFormat("reorder buffer capacity exceeded")
        ));
    }
}
