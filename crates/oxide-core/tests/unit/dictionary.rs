use super::{ArchiveDictionaryMode, DictionaryClass, DictionaryTrainer, classify_sample};
use crate::CompressionAlgo;

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
    assert!(
        bank.dictionary(DictionaryClass::StructuredText.id(), CompressionAlgo::Zstd)
            .is_some()
    );
}
