use super::{Lz4Scratch, apply_into_vec, apply_with_scratch, recycle_output, reverse_into_vec};

#[test]
fn direct_buffer_round_trip_reuses_output_vec() {
    let payload = b"banana bandana banana";
    let mut scratch = Lz4Scratch::default();
    let mut compressed = Vec::with_capacity(64);
    let original_ptr = compressed.as_ptr();

    apply_into_vec(payload, &mut scratch, &mut compressed).expect("lz4 compression should succeed");

    assert_eq!(compressed.as_ptr(), original_ptr);

    let mut decoded = Vec::with_capacity(payload.len());
    let decoded_ptr = decoded.as_ptr();
    reverse_into_vec(&compressed, &mut decoded).expect("lz4 decompression should succeed");

    assert_eq!(decoded.as_ptr(), decoded_ptr);
    assert_eq!(decoded, payload);
}

#[test]
fn scratch_output_can_be_recycled_for_reuse() {
    let payload = b"banana bandana banana";
    let mut scratch = Lz4Scratch::default();

    let compressed =
        apply_with_scratch(payload, &mut scratch).expect("lz4 compression should succeed");
    recycle_output(compressed.clone(), &mut scratch);
    let decoded = super::reverse_with_scratch(&compressed, &mut scratch)
        .expect("lz4 decompression should succeed");

    assert_eq!(decoded, payload);
}

#[test]
fn apply_into_vec_grows_undersized_output_vec() {
    let payload: Vec<u8> = (0..16_384).map(|idx| ((idx * 73) % 251) as u8).collect();
    let mut scratch = Lz4Scratch::default();
    let mut compressed = Vec::with_capacity(512);

    apply_into_vec(&payload, &mut scratch, &mut compressed)
        .expect("lz4 compression should grow undersized output");

    let mut decoded = Vec::with_capacity(1024);
    reverse_into_vec(&compressed, &mut decoded)
        .expect("lz4 decompression should succeed after growth");

    assert_eq!(decoded, payload);
}
