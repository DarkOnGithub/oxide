/// Chunk boundary mode used by the scanner and planner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkingMode {
    Fixed,
    Cdc,
}

/// Planner-selected chunking policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkingPolicy {
    pub mode: ChunkingMode,
    pub min_size: usize,
    pub target_size: usize,
    pub max_size: usize,
    pub superchunk_size: usize,
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
        }
    }

    pub fn cdc(target_size: usize, min_size: usize, max_size: usize) -> Self {
        let target_size = target_size.max(1);
        let min_size = min_size.max(1).min(target_size);
        let max_size = max_size.max(target_size).max(min_size);
        Self {
            mode: ChunkingMode::Cdc,
            min_size,
            target_size,
            max_size,
            superchunk_size: max_size.saturating_mul(8),
        }
    }

    pub fn cdc_for_target(target_size: usize) -> Self {
        let target_size = target_size.max(1);
        Self::cdc(
            target_size,
            (target_size / 4).max(1),
            target_size.saturating_mul(2).max(target_size),
        )
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

#[derive(Debug, Clone)]
pub struct ChunkStreamState {
    policy: ChunkingPolicy,
    stream_offset: u64,
    chunk_start_offset: u64,
    chunk_len: usize,
    hash: u64,
    chunk_limit: usize,
    normalization_target: usize,
    short_mask: u64,
    long_mask: u64,
}

impl ChunkStreamState {
    pub fn new(policy: ChunkingPolicy) -> Self {
        Self::with_offset(policy, 0)
    }

    pub fn with_offset(policy: ChunkingPolicy, offset: u64) -> Self {
        let (short_mask, long_mask) = cdc_masks_for_target(policy.target_size);
        let mut state = Self {
            policy,
            stream_offset: offset,
            chunk_start_offset: offset,
            chunk_len: 0,
            hash: 0,
            chunk_limit: 1,
            normalization_target: 1,
            short_mask,
            long_mask,
        };
        state.recompute_chunk_limits();
        state
    }

    pub fn current_chunk_len(&self) -> usize {
        self.chunk_len
    }

    pub fn consume_until_boundary(&mut self, data: &[u8]) -> Option<usize> {
        if data.is_empty() {
            return None;
        }

        let take = self.remaining_capacity().min(data.len());
        for (index, byte) in data[..take].iter().enumerate() {
            self.hash = self
                .hash
                .rotate_left(1)
                .wrapping_add(FASTCDC_GEAR[*byte as usize]);
            self.chunk_len += 1;
            self.stream_offset += 1;

            if self.should_cut() {
                self.start_new_chunk();
                return Some(index + 1);
            }
        }

        None
    }

    pub fn force_boundary(&mut self) {
        if self.chunk_len == 0 {
            return;
        }

        self.start_new_chunk();
    }

    fn should_cut(&self) -> bool {
        match self.policy.mode {
            ChunkingMode::Fixed => self.chunk_len >= self.chunk_limit,
            ChunkingMode::Cdc => {
                if self.chunk_len >= self.chunk_limit {
                    return true;
                }

                if self.chunk_len < self.policy.min_size {
                    return false;
                }

                if self.chunk_len < self.normalization_target {
                    (self.hash & self.short_mask) == 0
                } else {
                    (self.hash & self.long_mask) == 0
                }
            }
        }
    }

    fn remaining_capacity(&self) -> usize {
        self.chunk_limit.saturating_sub(self.chunk_len).max(1)
    }

    fn start_new_chunk(&mut self) {
        self.chunk_start_offset = self.stream_offset;
        self.chunk_len = 0;
        self.hash = 0;
        self.recompute_chunk_limits();
    }

    fn recompute_chunk_limits(&mut self) {
        let superchunk_limit = remaining_superchunk_budget(self.policy, self.chunk_start_offset);
        let chunk_limit = match self.policy.mode {
            ChunkingMode::Fixed => self.policy.target_size,
            ChunkingMode::Cdc => self.policy.max_size,
        };

        self.chunk_limit = chunk_limit.min(superchunk_limit).max(1);
        self.normalization_target = self
            .policy
            .target_size
            .clamp(self.policy.min_size.min(self.chunk_limit), self.chunk_limit);
    }
}

pub fn find_cdc_boundary(policy: ChunkingPolicy, data: &[u8], start: usize) -> usize {
    if start >= data.len() {
        return data.len();
    }

    let upper_bound = policy.upper_bound(start, data.len());
    if upper_bound <= start {
        return upper_bound;
    }

    let mut state = ChunkStreamState::with_offset(policy, start as u64);
    if let Some(boundary_len) = state.consume_until_boundary(&data[start..upper_bound]) {
        start + boundary_len
    } else {
        upper_bound
    }
}

const fn cdc_masks_for_target(target_size: usize) -> (u64, u64) {
    let mut value = if target_size == 0 {
        1
    } else {
        target_size.next_power_of_two()
    };
    let mut bits = 0u32;
    while value > 1 {
        value >>= 1;
        bits += 1;
    }

    let small_bits = if bits >= 62 { 63 } else { bits + 1 };
    let large_bits = if bits <= 1 {
        1
    } else if bits >= 64 {
        63
    } else {
        bits - 1
    };

    let short_mask = if small_bits >= 63 {
        u64::MAX
    } else {
        (1u64 << small_bits).saturating_sub(1)
    };
    let long_mask = if large_bits >= 63 {
        u64::MAX
    } else {
        (1u64 << large_bits).saturating_sub(1)
    };

    (short_mask, long_mask)
}

const fn splitmix64(mut state: u64) -> u64 {
    state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

const fn build_fastcdc_gear_table() -> [u64; 256] {
    let mut table = [0u64; 256];
    let mut index = 0usize;
    while index < 256 {
        table[index] = splitmix64(index as u64 + 1);
        index += 1;
    }
    table
}

const FASTCDC_GEAR: [u64; 256] = build_fastcdc_gear_table();

const fn remaining_superchunk_budget(policy: ChunkingPolicy, chunk_start_offset: u64) -> usize {
    if policy.superchunk_size == 0 {
        return match policy.mode {
            ChunkingMode::Fixed => {
                if policy.target_size == 0 {
                    1
                } else {
                    policy.target_size
                }
            }
            ChunkingMode::Cdc => {
                if policy.max_size == 0 {
                    1
                } else {
                    policy.max_size
                }
            }
        };
    }

    let offset = (chunk_start_offset % policy.superchunk_size as u64) as usize;
    let remaining = policy.superchunk_size.saturating_sub(offset);
    if remaining == 0 { 1 } else { remaining }
}
