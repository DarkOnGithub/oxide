/// Chunk boundary mode used by the scanner and planner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkingMode {
    Fixed,
    ContentDefined,
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

    pub fn content_defined_for_target(target_size: usize) -> Self {
        let target_size = target_size.max(4 * 1024);
        let min_size = (target_size / 4).max(4 * 1024);
        let max_size = (target_size.saturating_mul(4)).max(target_size);
        let superchunk_size = max_size;
        let mask_bits = target_mask_bits(target_size);
        Self {
            mode: ChunkingMode::ContentDefined,
            min_size,
            target_size,
            max_size,
            superchunk_size,
            cdc_mask: mask_from_bits(mask_bits),
        }
    }

    pub fn with_bounds(mut self, min_size: usize, max_size: usize) -> Self {
        self.min_size = min_size.max(1).min(self.target_size.max(1));
        self.max_size = max_size.max(self.target_size.max(self.min_size));
        self.superchunk_size = self.superchunk_size.max(self.max_size);
        self
    }

    pub fn with_superchunk_size(mut self, superchunk_size: usize) -> Self {
        self.superchunk_size = superchunk_size.max(self.max_size.max(1));
        self
    }

    pub fn with_cdc_mask_bits(mut self, mask_bits: u32) -> Self {
        self.cdc_mask = mask_from_bits(mask_bits);
        self
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

    pub fn find_boundary(self, data: &[u8], start: usize) -> usize {
        if start >= data.len() {
            return data.len();
        }

        match self.mode {
            ChunkingMode::Fixed => start
                .saturating_add(self.target_size)
                .min(self.upper_bound(start, data.len())),
            ChunkingMode::ContentDefined => self.find_cdc_boundary(data, start),
        }
    }

    fn find_cdc_boundary(self, data: &[u8], start: usize) -> usize {
        let len = data.len();
        let min_cut = start.saturating_add(self.min_size).min(len);
        let max_cut = self.upper_bound(start, len).max(min_cut).min(len);
        if min_cut >= max_cut {
            return max_cut;
        }

        let mut hash = 0u64;
        for (index, &byte) in data[start..max_cut].iter().enumerate() {
            hash = hash
                .rotate_left(1)
                .wrapping_add((byte as u64).wrapping_mul(0x9E37_79B1_85EB_CA87));
            let absolute = start + index + 1;
            if absolute >= min_cut && (hash & u64::from(self.cdc_mask.max(1))) == 0 {
                return absolute;
            }
        }

        max_cut
    }
}

const fn target_mask_bits(target_size: usize) -> u32 {
    let mut size = if target_size == 0 { 1 } else { target_size };
    let mut bits = 0u32;
    while size > 1 {
        bits += 1;
        size >>= 1;
    }
    if bits < 8 {
        8
    } else if bits > 22 {
        22
    } else {
        bits
    }
}

const fn mask_from_bits(bits: u32) -> u32 {
    if bits >= 31 {
        u32::MAX >> 1
    } else {
        (1u32 << bits).saturating_sub(1)
    }
}
