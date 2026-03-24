/// Chunk boundary mode used by the scanner and planner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkingMode {
    Fixed,
}

/// Planner-selected chunking policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkingPolicy {
    pub mode: ChunkingMode,
    pub min_size: usize,
    pub target_size: usize,
    pub max_size: usize,
    pub superchunk_size: usize,
    pub cdc_mask: u32,
}

impl ChunkingPolicy {
    pub fn fixed(target_size: usize) -> Self {
        let target_size = target_size.max(1);
        Self {
            mode: ChunkingMode::Fixed,
            min_size: target_size,
            target_size,
            max_size: target_size,
            superchunk_size: target_size.saturating_mul(8),
            cdc_mask: 0,
        }
    }

    pub fn fixed_for_target(target_size: usize) -> Self {
        Self::fixed(target_size)
    }

    pub fn upper_bound(self, start: usize, len: usize) -> usize {
        let superchunk_end = if self.superchunk_size == 0 {
            len
        } else {
            ((start / self.superchunk_size) + 1)
                .saturating_mul(self.superchunk_size)
                .min(len)
        };
        start
            .saturating_add(self.max_size)
            .min(superchunk_end)
            .min(len)
    }
}
