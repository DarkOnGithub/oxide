use super::{
    classify_path, classify_sample, dictionary_sample_offsets, ArchiveDictionaryMode,
    DictionaryClass, DictionaryTrainer,
};
use crate::CompressionAlgo;
use std::path::Path;

#[test]
fn classifier_detects_text_and_binary() {
    assert_eq!(
        classify_sample(b"hello world\nhello world\n"),
        DictionaryClass::StructuredText
    );
    assert_eq!(
        classify_sample(&[0, 159, 200, 1, 2, 3, 4]),
        DictionaryClass::Binary
    );
}

#[test]
fn trainer_builds_zstd_dictionary_bank_when_samples_are_available() {
    let mut trainer = DictionaryTrainer::new(ArchiveDictionaryMode::Auto);
    for _ in 0..16 {
        trainer.observe(br#"{"kind":"log","message":"banana bandana banana"}"#);
    }

    let bank = trainer
        .build(CompressionAlgo::Zstd, Some(6))
        .expect("dictionary bank should build");

    assert!(!bank.is_empty());
    assert!(bank.dictionaries().iter().any(|dictionary| {
        dictionary.algo == CompressionAlgo::Zstd
            && dictionary.class == DictionaryClass::StructuredText
    }));
}

#[test]
fn trainer_observe_with_path_keeps_general_and_extension_classes() {
    let mut trainer = DictionaryTrainer::new(ArchiveDictionaryMode::Auto);
    for _ in 0..16 {
        trainer.observe_with_path(
            Path::new("nested/config.json"),
            br#"{"kind":"log","message":"banana bandana banana"}"#,
        );
    }

    let bank = trainer
        .build(CompressionAlgo::Zstd, Some(6))
        .expect("dictionary bank should build");

    assert!(bank
        .dictionaries()
        .iter()
        .any(|dictionary| { dictionary.class == DictionaryClass::StructuredText }));
    assert!(bank
        .dictionaries()
        .iter()
        .any(|dictionary| { dictionary.class == DictionaryClass::Extension("json".to_string()) }));
}

#[test]
fn trainer_samples_multiple_windows_for_large_inputs() {
    let mut trainer = DictionaryTrainer::new(ArchiveDictionaryMode::Auto);
    let mut sample = vec![0u8; 16 * 1024 * 2];
    sample.extend(br#"{"kind":"log","message":"banana bandana banana"}"#.repeat(512));

    for _ in 0..16 {
        trainer.observe_with_path(Path::new("nested/config.json"), &sample);
    }

    let bank = trainer
        .build(CompressionAlgo::Zstd, Some(6))
        .expect("dictionary bank should build");

    assert!(bank
        .dictionaries()
        .iter()
        .any(|dictionary| dictionary.class == DictionaryClass::StructuredText));
}

#[test]
fn dictionary_sample_offsets_only_expand_for_large_inputs() {
    assert_eq!(dictionary_sample_offsets(16 * 1024), vec![0]);
    assert_eq!(dictionary_sample_offsets(32 * 1024), vec![0]);
    assert_eq!(
        dictionary_sample_offsets(64 * 1024),
        vec![0, 24 * 1024, 48 * 1024]
    );
}

#[test]
fn classify_path_detects_normalized_extension_dictionary_class() {
    assert_eq!(
        classify_path(Path::new("nested/INDEX.HTML")),
        Some(DictionaryClass::Extension("html".to_string()))
    );
}
