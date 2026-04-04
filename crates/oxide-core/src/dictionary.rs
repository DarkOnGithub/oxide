use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use crate::{CompressionAlgo, OxideError, Result};

const DEFAULT_DICT_SIZE: usize = 64 * 1024;
const DEFAULT_SAMPLE_SIZE: usize = 16 * 1024;
const MAX_SAMPLES_PER_CLASS: usize = 256;
const MAX_SAMPLE_BYTES_PER_CLASS: usize = 4 * 1024 * 1024;
const MIN_SAMPLES_PER_CLASS: usize = 8;

pub(crate) const DICTIONARY_SAMPLE_WINDOW_SIZE: usize = DEFAULT_SAMPLE_SIZE;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ArchiveDictionaryMode {
    #[default]
    Off,
    Auto,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum DictionaryClass {
    Text,
    StructuredText,
    Binary,
    Extension(String),
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

    pub fn select_for_chunk(
        &self,
        algo: CompressionAlgo,
        source: &[u8],
        source_path: Option<&Path>,
    ) -> Option<&ArchiveDictionary> {
        if algo != CompressionAlgo::Zstd || source.is_empty() {
            return None;
        }

        if self.dictionaries.is_empty() {
            return None;
        }

        if let Some(class) = source_path.and_then(classify_path)
            && let Some(dictionary) = self
                .dictionaries
                .iter()
                .find(|dictionary| dictionary.algo == algo && dictionary.class == class)
            {
                return Some(dictionary);
            }

        let class = classify_sample(source);
        self.dictionaries
            .iter()
            .find(|dictionary| dictionary.algo == algo && dictionary.class == class)
    }

    pub fn validate(&self) -> Result<()> {
        let mut seen_ids = BTreeSet::<u8>::new();
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
            if !seen_ids.insert(dictionary.id) {
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
        for_each_dictionary_sample_window(sample, |window| {
            self.observe_class(window, classify_sample(window));
        });
    }

    pub fn observe_with_path<P>(&mut self, path: P, sample: &[u8])
    where
        P: AsRef<Path>,
    {
        let path_class = classify_path(path.as_ref());
        for_each_dictionary_sample_window(sample, |window| {
            let sample_class = classify_sample(window);
            self.observe_class(window, sample_class.clone());

            if let Some(path_class) = path_class.as_ref()
                && *path_class != sample_class {
                    self.observe_class(window, path_class.clone());
                }
        });
    }

    fn observe_class(&mut self, sample: &[u8], class: DictionaryClass) {
        if self.mode == ArchiveDictionaryMode::Off || sample.is_empty() {
            return;
        }

        let clipped_len = sample.len().min(DEFAULT_SAMPLE_SIZE);
        if clipped_len == 0 {
            return;
        }

        let current_bytes = *self.sample_bytes_by_class.get(&class).unwrap_or(&0);
        let samples = self.samples_by_class.entry(class.clone()).or_default();
        if samples.len() >= MAX_SAMPLES_PER_CLASS || current_bytes >= MAX_SAMPLE_BYTES_PER_CLASS {
            return;
        }

        let clipped = sample[..clipped_len].to_vec();
        self.sample_bytes_by_class
            .insert(class, current_bytes.saturating_add(clipped.len()));
        samples.push(clipped);
    }

    pub fn build(
        self,
        algo: CompressionAlgo,
        _level: Option<i32>,
    ) -> Result<ArchiveDictionaryBank> {
        if self.mode == ArchiveDictionaryMode::Off || algo != CompressionAlgo::Zstd {
            return Ok(ArchiveDictionaryBank::default());
        }

        let mut trained = Vec::new();
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

            let sample_bytes = *self.sample_bytes_by_class.get(&class).unwrap_or(&0);
            trained.push((class, dict, sample_bytes));
        }

        trained.sort_by(|left, right| right.2.cmp(&left.2).then_with(|| left.0.cmp(&right.0)));

        let mut dictionaries = Vec::with_capacity(trained.len().min(u8::MAX as usize));
        for (index, (class, bytes, _sample_bytes)) in
            trained.into_iter().take(u8::MAX as usize).enumerate()
        {
            let dictionary = ArchiveDictionary {
                id: (index + 1) as u8,
                algo,
                class,
                bytes,
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
                if matches!(
                    byte,
                    b'{' | b'}' | b'[' | b']' | b':' | b',' | b'<' | b'>' | b'/' | b'=' | b'"'
                ) {
                    structured += 1;
                }
            }
            _ => {}
        }
    }

    let len = sample.len().clamp(1, DEFAULT_SAMPLE_SIZE);
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

pub fn classify_path(path: &Path) -> Option<DictionaryClass> {
    normalized_extension_from_path(path).map(DictionaryClass::Extension)
}

pub fn normalized_extension_from_path(path: &Path) -> Option<String> {
    let extension = path.extension()?.to_str()?;
    normalized_extension(extension)
}

pub fn normalized_extension(extension: &str) -> Option<String> {
    let extension = extension.trim();
    let extension = extension.trim_start_matches('.');
    if extension.is_empty() {
        return None;
    }

    Some(extension.to_ascii_lowercase())
}

#[inline]
fn for_each_dictionary_sample_offset(sample_len: usize, mut visit: impl FnMut(usize)) {
    let window_size = DICTIONARY_SAMPLE_WINDOW_SIZE.min(sample_len);
    if window_size == 0 {
        return;
    }

    let max_start = sample_len.saturating_sub(window_size);
    visit(0);
    if max_start >= window_size.saturating_mul(2) {
        let middle = max_start / 2;
        if middle != 0 {
            visit(middle);
        }
        if max_start != 0 && max_start != middle {
            visit(max_start);
        }
    }
}

#[inline]
fn for_each_dictionary_sample_window(sample: &[u8], mut visit: impl FnMut(&[u8])) {
    for_each_dictionary_sample_offset(sample.len(), |offset| {
        let end = offset
            .saturating_add(DICTIONARY_SAMPLE_WINDOW_SIZE)
            .min(sample.len());
        visit(&sample[offset..end]);
    });
}

