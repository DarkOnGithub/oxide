use super::{
    LZMA_LEVEL_7_TO_8_DICT_SIZE, LZMA_LEVEL_9_DICT_SIZE, PRESET_EXTREME, resolve_dictionary_size,
    resolve_level,
};

#[test]
fn plain_level_nine_does_not_enable_extreme() {
    assert_eq!(resolve_level(Some(9), false).expect("resolve level"), 9);
}

#[test]
fn explicit_extreme_sets_extreme_bit() {
    assert_eq!(
        resolve_level(Some(9), true).expect("resolve level"),
        9 | PRESET_EXTREME
    );
}

#[test]
fn tuned_dictionary_size_tracks_level_defaults() {
    assert_eq!(
        resolve_dictionary_size(Some(7), None).expect("dictionary size"),
        LZMA_LEVEL_7_TO_8_DICT_SIZE as u32
    );
    assert_eq!(
        resolve_dictionary_size(Some(9), None).expect("dictionary size"),
        LZMA_LEVEL_9_DICT_SIZE as u32
    );
}
