use crate::io::chunking::{find_cdc_boundary, ChunkingPolicy};

const CDC_WINDOW_SIZE: usize = 16;

fn reference_find_cdc_boundary(policy: ChunkingPolicy, data: &[u8], start: usize) -> usize {
    const CDC_TABLE: [u64; 256] = build_cdc_table();

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

    for index in start..upper_bound {
        let byte = data[index];
        if filled < CDC_WINDOW_SIZE {
            hash = hash.rotate_left(1) ^ CDC_TABLE[byte as usize];
            window[filled] = byte;
            filled += 1;
        } else {
            let slot = index % CDC_WINDOW_SIZE;
            let outgoing = window[slot];
            window[slot] = byte;
            hash = hash.rotate_left(1)
                ^ CDC_TABLE[byte as usize]
                ^ CDC_TABLE[outgoing as usize].rotate_left(CDC_WINDOW_SIZE as u32);
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

#[test]
fn cdc_boundary_matches_reference_for_unaligned_starts() {
    let policy = ChunkingPolicy::cdc(32, 8, 64);
    let data = (0..512)
        .map(|index| ((index * 97 + 13) % 251) as u8)
        .collect::<Vec<_>>();

    for start in [1usize, 7, 15, 16, 17, 31, 47, 63, 95] {
        assert_eq!(
            find_cdc_boundary(policy, &data, start),
            reference_find_cdc_boundary(policy, &data, start),
            "mismatch at start {start}"
        );
    }
}
