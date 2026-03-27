use super::{
    LZMA_MAX_RETAINED_OUTPUT_CAPACITY, LzmaScratch, ZSTD_MAX_RETAINED_OUTPUT_CAPACITY, ZstdScratch,
};

#[test]
fn lzma_recycle_caps_retained_output_capacity() {
    let mut scratch = LzmaScratch::default();
    scratch.recycle_output(Vec::with_capacity(LZMA_MAX_RETAINED_OUTPUT_CAPACITY * 2));

    assert!(scratch.allocated_bytes() <= LZMA_MAX_RETAINED_OUTPUT_CAPACITY);
}

#[test]
fn zstd_recycle_caps_retained_output_capacity() {
    let mut scratch = ZstdScratch::default();
    scratch.recycle_output(Vec::with_capacity(ZSTD_MAX_RETAINED_OUTPUT_CAPACITY * 2));

    assert!(scratch.allocated_bytes() <= ZSTD_MAX_RETAINED_OUTPUT_CAPACITY);
}
