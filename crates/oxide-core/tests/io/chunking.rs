use oxide_core::io::chunking::{ChunkStreamState, ChunkingPolicy, find_cdc_boundary};

#[test]
fn cdc_boundary_matches_streaming_fastcdc_for_unaligned_starts() {
    let policy = ChunkingPolicy::cdc(32, 8, 64);
    let data = (0..512)
        .map(|index| ((index * 97 + 13) % 251) as u8)
        .collect::<Vec<_>>();

    for start in [1usize, 7, 15, 16, 17, 31, 47, 63, 95] {
        let mut state = ChunkStreamState::with_offset(policy, start as u64);
        let upper_bound = policy.upper_bound(start, data.len());
        let streamed = state
            .consume_until_boundary(&data[start..upper_bound])
            .map(|len| start + len)
            .unwrap_or(upper_bound);

        assert_eq!(
            find_cdc_boundary(policy, &data, start),
            streamed,
            "mismatch at start {start}"
        );
    }
}

#[test]
fn stream_state_carries_cdc_boundaries_across_pushes() {
    let policy = ChunkingPolicy::cdc(32, 8, 64);
    let data = (0..192)
        .map(|index| ((index * 41 + 7) % 251) as u8)
        .collect::<Vec<_>>();

    let first_cut = find_cdc_boundary(policy, &data, 0);
    let mut state = ChunkStreamState::new(policy);
    let split = 11usize;

    assert_eq!(state.consume_until_boundary(&data[..split]), None);
    let second = state
        .consume_until_boundary(&data[split..first_cut])
        .expect("boundary should carry across pushes");

    assert_eq!(split + second, first_cut);
    assert_eq!(state.current_chunk_len(), 0);
}

#[test]
fn cdc_boundary_respects_superchunk_limit() {
    let mut policy = ChunkingPolicy::cdc(32, 8, 64);
    policy.superchunk_size = 48;
    let data = vec![b'x'; 256];

    let boundary = find_cdc_boundary(policy, &data, 40);
    assert!(boundary <= 48);
}
