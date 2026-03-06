use crate::CompressionPreset;

/// Chunk boundary mode used by the scanner and planner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkingMode {
    Fixed,
    Adaptive,
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

    pub fn for_preset(target_size: usize, preset: CompressionPreset) -> Self {
        let target_size = target_size.max(1);
        match preset {
            CompressionPreset::Fast => Self::fixed(target_size),
            CompressionPreset::Default => Self::adaptive(target_size, 16, 8),
            CompressionPreset::High => Self::adaptive(target_size, 32, 4),
        }
    }

    fn adaptive(target_size: usize, superchunk_multiplier: usize, divisor: usize) -> Self {
        let min_size = (target_size / 2).max(1);
        let max_size = target_size.saturating_mul(2).max(min_size);
        let expected = target_size.next_power_of_two().max(256);
        Self {
            mode: ChunkingMode::Adaptive,
            min_size,
            target_size,
            max_size,
            superchunk_size: target_size.saturating_mul(superchunk_multiplier.max(1)),
            cdc_mask: (expected / divisor.max(1))
                .next_power_of_two()
                .saturating_sub(1) as u32,
        }
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

pub fn find_content_defined_cut(
    data: &[u8],
    start: usize,
    min_cut: usize,
    max_cut: usize,
    mask: u32,
) -> Option<usize> {
    if min_cut >= max_cut || mask == 0 {
        return None;
    }

    let mut hash = 0u32;
    for &byte in &data[start..min_cut] {
        hash = update_hash(hash, byte);
    }

    for index in min_cut..max_cut {
        hash = update_hash(hash, data[index]);
        if hash & mask == 0 {
            return Some(index + 1);
        }
    }

    None
}

#[inline]
fn update_hash(hash: u32, byte: u8) -> u32 {
    hash.rotate_left(5)
        .wrapping_add(byte as u32)
        .wrapping_mul(0x1e35_a7bd)
}
