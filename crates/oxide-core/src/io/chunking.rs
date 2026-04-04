const CDC_WINDOW_SIZE: usize = 16;
// The rolling-window slot math below uses bit masking instead of modulo, so the
// window size must remain a power of two.
const _: () = assert!(CDC_WINDOW_SIZE.is_power_of_two());
const CDC_WINDOW_MASK: usize = CDC_WINDOW_SIZE - 1;

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
    pub cdc_mask: u64,
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
            cdc_mask: cdc_mask_for_target(target_size),
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

pub fn find_cdc_boundary(policy: ChunkingPolicy, data: &[u8], start: usize) -> usize {
    if start >= data.len() {
        return data.len();
    }

    let upper_bound = policy.upper_bound(start, data.len());
    if upper_bound <= start.saturating_add(policy.min_size) {
        return upper_bound;
    }

    let mut hash = 0u64;
    let mut window = [0u8; CDC_WINDOW_SIZE];
    let mut filled = 0usize;
    let mut slot = 0usize;

    for index in start..upper_bound {
        let byte = data[index];
        if filled < CDC_WINDOW_SIZE {
            hash = hash.rotate_left(1) ^ CDC_TABLE[byte as usize];
            window[filled] = byte;
            filled += 1;
            if filled == CDC_WINDOW_SIZE {
                slot = (index + 1) & CDC_WINDOW_MASK;
            }
        } else {
            let outgoing = window[slot];
            window[slot] = byte;
            hash =
                hash.rotate_left(1) ^ CDC_TABLE[byte as usize] ^ CDC_OUT_TABLE[outgoing as usize];
            slot = (slot + 1) & CDC_WINDOW_MASK;
        }

        let chunk_size = index + 1 - start;
        if chunk_size >= policy.min_size
            && chunk_size < policy.max_size
            && (hash & policy.cdc_mask) == 0
        {
            return index + 1;
        }
    }

    upper_bound
}

const fn cdc_mask_for_target(target_size: usize) -> u64 {
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

    if bits >= 63 {
        u64::MAX
    } else {
        (1u64 << bits).saturating_sub(1)
    }
}

const fn splitmix64(mut state: u64) -> u64 {
    state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

const fn build_cdc_table() -> [u64; 256] {
    let mut table = [0u64; 256];
    let mut index = 0usize;
    while index < 256 {
        table[index] = splitmix64(index as u64 + 1);
        index += 1;
    }
    table
}

const CDC_TABLE: [u64; 256] = build_cdc_table();

const fn build_cdc_out_table() -> [u64; 256] {
    let mut table = [0u64; 256];
    let mut index = 0usize;
    while index < 256 {
        table[index] = CDC_TABLE[index].rotate_left(CDC_WINDOW_SIZE as u32);
        index += 1;
    }
    table
}

const CDC_OUT_TABLE: [u64; 256] = build_cdc_out_table();
