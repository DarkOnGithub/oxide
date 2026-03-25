use std::collections::BTreeMap;

use crate::{CompressionAlgo, OxideError, Result};

const DEFAULT_DICT_SIZE: usize = 64 * 1024;
const DEFAULT_SAMPLE_SIZE: usize = 16 * 1024;
const MAX_SAMPLES_PER_CLASS: usize = 256;
const MAX_SAMPLE_BYTES_PER_CLASS: usize = 4 * 1024 * 1024;
const MIN_SAMPLES_PER_CLASS: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ArchiveDictionaryMode {
    #[default]
    Off,
    Auto,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum DictionaryClass {
    Text,
    StructuredText,
    Binary,
}

impl DictionaryClass {
    pub fn id(self) -> u8 {
        match self {
            Self::Text => 1,
            Self::StructuredText => 2,
            Self::Binary => 3,
        }
    }

    pub fn from_id(id: u8) -> Result<Self> {
        match id {
            1 => Ok(Self::Text),
            2 => Ok(Self::StructuredText),
            3 => Ok(Self::Binary),
            _ => Err(OxideError::InvalidFormat("invalid archive dictionary class id")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveDictionary {
    pub id: u8,
    pub algo: CompressionAlgo,
    pub class: DictionaryClass,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ArchiveDictionaryBank {
    dictionaries: Vec<ArchiveDictionary>,
}

impl ArchiveDictionaryBank {
    pub fn new(dictionaries: Vec<ArchiveDictionary>) -> Result<Self> {
        let bank = Self { dictionaries };
        bank.validate()?;
        Ok(bank)
    }

    pub fn is_empty(&self) -> bool {
        self.dictionaries.is_empty()
    }

    pub fn dictionaries(&self) -> &[ArchiveDictionary] {
        &self.dictionaries
    }

    pub fn dictionary(&self, id: u8, algo: CompressionAlgo) -> Option<&ArchiveDictionary> {
        if id == 0 {
            return None;
        }

        self.dictionaries
            .iter()
            .find(|dictionary| dictionary.id == id && dictionary.algo == algo)
    }

    pub fn dictionary_bytes(&self, id: u8, algo: CompressionAlgo) -> Option<&[u8]> {
        self.dictionary(id, algo)
            .map(|dictionary| dictionary.bytes.as_slice())
    }

    pub fn select_for_chunk(&self, algo: CompressionAlgo, source: &[u8]) -> Option<&ArchiveDictionary> {
        if algo != CompressionAlgo::Zstd || source.is_empty() {
            return None;
        }

        let class = classify_sample(source);
        self.dictionaries
            .iter()
            .find(|dictionary| dictionary.algo == algo && dictionary.class == class)
    }

    pub fn validate(&self) -> Result<()> {
        let mut seen_ids = BTreeMap::<u8, CompressionAlgo>::new();
        for dictionary in &self.dictionaries {
            if dictionary.id == 0 {
                return Err(OxideError::InvalidFormat(
                    "archive dictionary id 0 is reserved",
                ));
            }
            if dictionary.bytes.is_empty() {
                return Err(OxideError::InvalidFormat(
                    "archive dictionary payload must not be empty",
                ));
            }
            if let Some(existing_algo) = seen_ids.insert(dictionary.id, dictionary.algo)
                && existing_algo != dictionary.algo
            {
                return Err(OxideError::InvalidFormat(
                    "archive dictionary ids must be unique per bank",
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct DictionaryTrainer {
    mode: ArchiveDictionaryMode,
    samples_by_class: BTreeMap<DictionaryClass, Vec<Vec<u8>>>,
    sample_bytes_by_class: BTreeMap<DictionaryClass, usize>,
}

impl DictionaryTrainer {
    pub fn new(mode: ArchiveDictionaryMode) -> Self {
        Self {
            mode,
            samples_by_class: BTreeMap::new(),
            sample_bytes_by_class: BTreeMap::new(),
        }
    }

    pub fn mode(&self) -> ArchiveDictionaryMode {
        self.mode
    }

    pub fn observe(&mut self, sample: &[u8]) {
        if self.mode == ArchiveDictionaryMode::Off || sample.is_empty() {
            return;
        }

        let class = classify_sample(sample);
        let clipped_len = sample.len().min(DEFAULT_SAMPLE_SIZE);
        if clipped_len == 0 {
            return;
        }

        let current_bytes = *self.sample_bytes_by_class.get(&class).unwrap_or(&0);
        let samples = self.samples_by_class.entry(class).or_default();
        if samples.len() >= MAX_SAMPLES_PER_CLASS || current_bytes >= MAX_SAMPLE_BYTES_PER_CLASS {
            return;
        }

        let clipped = sample[..clipped_len].to_vec();
        self.sample_bytes_by_class
            .insert(class, current_bytes.saturating_add(clipped.len()));
        samples.push(clipped);
    }

    pub fn build(self, algo: CompressionAlgo, _level: Option<i32>) -> Result<ArchiveDictionaryBank> {
        if self.mode == ArchiveDictionaryMode::Off || algo != CompressionAlgo::Zstd {
            return Ok(ArchiveDictionaryBank::default());
        }

        let mut dictionaries = Vec::new();
        for (class, samples) in self.samples_by_class {
            if samples.len() < MIN_SAMPLES_PER_CLASS {
                continue;
            }

            let dict = zstd::dict::from_samples(&samples, DEFAULT_DICT_SIZE).map_err(|error| {
                OxideError::CompressionError(format!(
                    "zstd dictionary training failed for {:?}: {error}",
                    class
                ))
            })?;
            if dict.is_empty() {
                continue;
            }

            let dictionary = ArchiveDictionary {
                id: class.id(),
                algo,
                class,
                bytes: dict,
            };
            dictionaries.push(dictionary);
        }

        ArchiveDictionaryBank::new(dictionaries)
    }
}

pub fn classify_sample(sample: &[u8]) -> DictionaryClass {
    if sample.is_empty() {
        return DictionaryClass::Binary;
    }

    let mut printable = 0usize;
    let mut whitespace = 0usize;
    let mut nul = 0usize;
    let mut structured = 0usize;

    for &byte in sample.iter().take(DEFAULT_SAMPLE_SIZE) {
        match byte {
            0 => nul += 1,
            b'\n' | b'\r' | b'\t' | b' ' => {
                printable += 1;
                whitespace += 1;
            }
            0x20..=0x7e => {
                printable += 1;
                if matches!(byte, b'{' | b'}' | b'[' | b']' | b':' | b',' | b'<' | b'>' | b'/' | b'=' | b'"') {
                    structured += 1;
                }
            }
            _ => {}
        }
    }

    let len = sample.len().min(DEFAULT_SAMPLE_SIZE).max(1);
    if nul > 0 {
        return DictionaryClass::Binary;
    }

    let printable_ratio = printable as f64 / len as f64;
    let structured_ratio = structured as f64 / len as f64;
    let whitespace_ratio = whitespace as f64 / len as f64;

    if printable_ratio >= 0.85 {
        if structured_ratio >= 0.08 || whitespace_ratio >= 0.12 {
            DictionaryClass::StructuredText
        } else {
            DictionaryClass::Text
        }
    } else {
        DictionaryClass::Binary
    }
}

#[cfg(test)]
mod tests {
    use super::{ArchiveDictionaryMode, DictionaryClass, DictionaryTrainer, classify_sample};
    use crate::CompressionAlgo;

    #[test]
    fn classifier_detects_text_and_binary() {
        assert_eq!(classify_sample(b"hello world\nhello world\n"), DictionaryClass::StructuredText);
        assert_eq!(classify_sample(&[0, 159, 200, 1, 2, 3, 4]), DictionaryClass::Binary);
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
        assert!(bank.dictionary(DictionaryClass::StructuredText.id(), CompressionAlgo::Zstd).is_some());
    }
}
