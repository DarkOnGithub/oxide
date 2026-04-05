use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use crate::{CompressionAlgo, OxideError, Result};

const DEFAULT_DICT_SIZE: usize = 64 * 1024;
const DEFAULT_SAMPLE_SIZE: usize = 16 * 1024;
const MAX_SAMPLES_PER_CLASS: usize = 512;
const MAX_SAMPLE_BYTES_PER_CLASS: usize = 8 * 1024 * 1024;
const MIN_SAMPLES_PER_DICTIONARY: usize = 8;
const MAX_DICTIONARIES_PER_CLASS: usize = 4;
const MAX_SAMPLE_WINDOWS_PER_INPUT: usize = 8;

pub const DICTIONARY_SIGNATURE_LEN: usize = 16;

pub(crate) const DICTIONARY_SAMPLE_WINDOW_SIZE: usize = DEFAULT_SAMPLE_SIZE;

type DictionarySignature = [u8; DICTIONARY_SIGNATURE_LEN];

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
    pub sample_count: u16,
    pub sample_bytes: u32,
    pub signature: DictionarySignature,
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

        let path_class = source_path.and_then(classify_path);
        let class = classify_sample(source);
        let signature = sample_signature(source);
        self.dictionaries
            .iter()
            .filter(|dictionary| dictionary.algo == algo)
            .filter_map(|dictionary| {
                dictionary_selection_score(dictionary, path_class.as_ref(), &class, &signature)
                    .map(|score| (score, dictionary))
            })
            .max_by(|(left_score, left), (right_score, right)| {
                left_score
                    .cmp(right_score)
                    .then_with(|| left.sample_count.cmp(&right.sample_count))
                    .then_with(|| left.sample_bytes.cmp(&right.sample_bytes))
                    .then_with(|| left.id.cmp(&right.id))
            })
            .map(|(_, dictionary)| dictionary)
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
    samples_by_class: BTreeMap<DictionaryClass, Vec<DictionaryTrainingSample>>,
    sample_bytes_by_class: BTreeMap<DictionaryClass, usize>,
}

#[derive(Debug, Clone)]
struct DictionaryTrainingSample {
    bytes: Vec<u8>,
    signature: DictionarySignature,
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
                && *path_class != sample_class
            {
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
        let signature = sample_signature(&clipped);
        self.sample_bytes_by_class
            .insert(class, current_bytes.saturating_add(clipped.len()));
        samples.push(DictionaryTrainingSample {
            bytes: clipped,
            signature,
        });
    }

    pub fn build(
        self,
        algo: CompressionAlgo,
        _level: Option<i32>,
    ) -> Result<ArchiveDictionaryBank> {
        if self.mode == ArchiveDictionaryMode::Off || algo != CompressionAlgo::Zstd {
            return Ok(ArchiveDictionaryBank::default());
        }

        let DictionaryTrainer {
            mode: _,
            samples_by_class,
            sample_bytes_by_class,
        } = self;

        let mut trained = Vec::new();
        for (class, samples) in samples_by_class {
            if samples.len() < MIN_SAMPLES_PER_DICTIONARY {
                continue;
            }

            let desired_dictionary_count = desired_dictionary_count(samples.len());
            for cluster in cluster_training_samples(&samples, desired_dictionary_count) {
                if cluster.len() < MIN_SAMPLES_PER_DICTIONARY {
                    continue;
                }

                let cluster_sample_bytes = cluster
                    .iter()
                    .map(|&index| samples[index].bytes.len())
                    .sum::<usize>();
                let cluster_samples = cluster
                    .iter()
                    .map(|&index| samples[index].bytes.as_slice())
                    .collect::<Vec<_>>();
                let dict = zstd::dict::from_samples(&cluster_samples, DEFAULT_DICT_SIZE).map_err(
                    |error| {
                        OxideError::CompressionError(format!(
                            "zstd dictionary training failed for {:?}: {error}",
                            class
                        ))
                    },
                )?;
                if dict.is_empty() {
                    continue;
                }

                trained.push((
                    class.clone(),
                    dict,
                    cluster_sample_bytes,
                    u16::try_from(cluster.len()).unwrap_or(u16::MAX),
                    average_signature(cluster.iter().map(|&index| samples[index].signature)),
                ));
            }
        }

        trained.sort_by(|left, right| {
            right
                .2
                .cmp(&left.2)
                .then_with(|| right.3.cmp(&left.3))
                .then_with(|| left.0.cmp(&right.0))
                .then_with(|| left.4.cmp(&right.4))
        });

        let mut dictionaries = Vec::with_capacity(trained.len().min(u8::MAX as usize));
        for (index, (class, bytes, sample_bytes, sample_count, signature)) in
            trained.into_iter().take(u8::MAX as usize).enumerate()
        {
            let dictionary = ArchiveDictionary {
                id: (index + 1) as u8,
                algo,
                class,
                sample_count,
                sample_bytes: u32::try_from(sample_bytes).unwrap_or(u32::MAX),
                signature,
                bytes,
            };
            dictionaries.push(dictionary);
        }

        let _ = sample_bytes_by_class;
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
pub fn dictionary_sample_offsets(sample_len: usize) -> Vec<usize> {
    let window_size = DICTIONARY_SAMPLE_WINDOW_SIZE.min(sample_len);
    if window_size == 0 {
        return Vec::new();
    }

    let max_start = sample_len.saturating_sub(window_size);
    let mut offsets = Vec::with_capacity(MAX_SAMPLE_WINDOWS_PER_INPUT);
    offsets.push(0);
    if max_start == 0 {
        return offsets;
    }

    let stride = (max_start / (MAX_SAMPLE_WINDOWS_PER_INPUT - 1)).max(window_size);
    let mut offset = stride;
    while offset < max_start && offsets.len() + 1 < MAX_SAMPLE_WINDOWS_PER_INPUT {
        if offsets.last().copied() != Some(offset) {
            offsets.push(offset);
        }
        offset = offset.saturating_add(stride);
    }

    if offsets.last().copied() != Some(max_start) {
        offsets.push(max_start);
    }
    offsets
}

#[inline]
fn for_each_dictionary_sample_window(sample: &[u8], mut visit: impl FnMut(&[u8])) {
    for offset in dictionary_sample_offsets(sample.len()) {
        let end = offset
            .saturating_add(DICTIONARY_SAMPLE_WINDOW_SIZE)
            .min(sample.len());
        visit(&sample[offset..end]);
    }
}

fn desired_dictionary_count(sample_count: usize) -> usize {
    (sample_count / MIN_SAMPLES_PER_DICTIONARY).clamp(1, MAX_DICTIONARIES_PER_CLASS)
}

fn cluster_training_samples(
    samples: &[DictionaryTrainingSample],
    desired_clusters: usize,
) -> Vec<Vec<usize>> {
    if samples.is_empty() {
        return Vec::new();
    }

    let desired_clusters = desired_clusters
        .clamp(1, MAX_DICTIONARIES_PER_CLASS)
        .min(samples.len() / MIN_SAMPLES_PER_DICTIONARY)
        .max(1);
    if desired_clusters == 1 {
        return vec![(0..samples.len()).collect()];
    }

    let mut centroids = vec![samples[0].signature];
    while centroids.len() < desired_clusters {
        let mut best_index = None;
        let mut best_distance = 0u32;
        for (index, sample) in samples.iter().enumerate() {
            let distance = centroids
                .iter()
                .map(|centroid| signature_distance(sample.signature, *centroid))
                .min()
                .unwrap_or(0);
            if distance > best_distance {
                best_distance = distance;
                best_index = Some(index);
            }
        }

        let Some(best_index) = best_index else {
            break;
        };
        let candidate = samples[best_index].signature;
        if centroids.contains(&candidate) {
            break;
        }
        centroids.push(candidate);
    }

    if centroids.len() <= 1 {
        return vec![(0..samples.len()).collect()];
    }

    let mut assignments = vec![0usize; samples.len()];
    for _ in 0..4 {
        let mut changed = false;
        for (index, sample) in samples.iter().enumerate() {
            let best_cluster = nearest_signature(sample.signature, &centroids);
            if assignments[index] != best_cluster {
                assignments[index] = best_cluster;
                changed = true;
            }
        }

        let mut sums = vec![[0u32; DICTIONARY_SIGNATURE_LEN]; centroids.len()];
        let mut counts = vec![0u32; centroids.len()];
        for (index, &cluster_index) in assignments.iter().enumerate() {
            counts[cluster_index] = counts[cluster_index].saturating_add(1);
            for bucket in 0..DICTIONARY_SIGNATURE_LEN {
                sums[cluster_index][bucket] = sums[cluster_index][bucket]
                    .saturating_add(samples[index].signature[bucket] as u32);
            }
        }

        for (cluster_index, centroid) in centroids.iter_mut().enumerate() {
            if counts[cluster_index] == 0 {
                continue;
            }
            for bucket in 0..DICTIONARY_SIGNATURE_LEN {
                centroid[bucket] = (sums[cluster_index][bucket] / counts[cluster_index]) as u8;
            }
        }

        if !changed {
            break;
        }
    }

    let mut clusters = vec![Vec::new(); centroids.len()];
    for (index, &cluster_index) in assignments.iter().enumerate() {
        clusters[cluster_index].push(index);
    }

    merge_small_clusters(samples, &mut clusters);
    clusters.retain(|cluster| !cluster.is_empty());
    if clusters.is_empty() {
        return vec![(0..samples.len()).collect()];
    }

    clusters.sort_by(|left, right| {
        let left_bytes = left
            .iter()
            .map(|&index| samples[index].bytes.len())
            .sum::<usize>();
        let right_bytes = right
            .iter()
            .map(|&index| samples[index].bytes.len())
            .sum::<usize>();
        right_bytes
            .cmp(&left_bytes)
            .then_with(|| right.len().cmp(&left.len()))
    });
    clusters
}

fn merge_small_clusters(samples: &[DictionaryTrainingSample], clusters: &mut [Vec<usize>]) {
    if clusters.len() <= 1 {
        return;
    }

    for cluster_index in 0..clusters.len() {
        if clusters[cluster_index].is_empty()
            || clusters[cluster_index].len() >= MIN_SAMPLES_PER_DICTIONARY
        {
            continue;
        }

        let source_centroid = average_signature(
            clusters[cluster_index]
                .iter()
                .map(|&index| samples[index].signature),
        );
        let Some(target_index) = (0..clusters.len())
            .filter(|&index| index != cluster_index && !clusters[index].is_empty())
            .min_by_key(|&index| {
                signature_distance(
                    source_centroid,
                    average_signature(
                        clusters[index]
                            .iter()
                            .map(|&sample_index| samples[sample_index].signature),
                    ),
                )
            })
        else {
            continue;
        };

        let moved = std::mem::take(&mut clusters[cluster_index]);
        clusters[target_index].extend(moved);
    }
}

fn dictionary_selection_score(
    dictionary: &ArchiveDictionary,
    path_class: Option<&DictionaryClass>,
    sample_class: &DictionaryClass,
    sample_signature: &DictionarySignature,
) -> Option<(i32, u32)> {
    let base_score = match &dictionary.class {
        DictionaryClass::Extension(_) if path_class == Some(&dictionary.class) => 20_000,
        DictionaryClass::Extension(_) => return None,
        _ if dictionary.class == *sample_class => 10_000,
        _ => return None,
    };

    let distance = signature_distance(dictionary.signature, *sample_signature);
    Some((
        base_score - distance as i32,
        u32::MAX.saturating_sub(distance),
    ))
}

fn sample_signature(sample: &[u8]) -> DictionarySignature {
    let mut buckets = [0u32; DICTIONARY_SIGNATURE_LEN];
    for &byte in sample.iter().take(DEFAULT_SAMPLE_SIZE) {
        let bucket = ((byte as usize) * DICTIONARY_SIGNATURE_LEN) / 256;
        buckets[bucket] = buckets[bucket].saturating_add(1);
    }

    normalize_signature(buckets, sample.len().min(DEFAULT_SAMPLE_SIZE).max(1) as u32)
}

fn normalize_signature(
    buckets: [u32; DICTIONARY_SIGNATURE_LEN],
    total: u32,
) -> DictionarySignature {
    let mut signature = [0u8; DICTIONARY_SIGNATURE_LEN];
    for (index, value) in buckets.into_iter().enumerate() {
        signature[index] = ((value.saturating_mul(255) + total / 2) / total) as u8;
    }
    signature
}

fn average_signature(signatures: impl Iterator<Item = DictionarySignature>) -> DictionarySignature {
    let mut sums = [0u32; DICTIONARY_SIGNATURE_LEN];
    let mut count = 0u32;
    for signature in signatures {
        count = count.saturating_add(1);
        for bucket in 0..DICTIONARY_SIGNATURE_LEN {
            sums[bucket] = sums[bucket].saturating_add(signature[bucket] as u32);
        }
    }

    if count == 0 {
        return [0u8; DICTIONARY_SIGNATURE_LEN];
    }

    let mut average = [0u8; DICTIONARY_SIGNATURE_LEN];
    for bucket in 0..DICTIONARY_SIGNATURE_LEN {
        average[bucket] = (sums[bucket] / count) as u8;
    }
    average
}

fn nearest_signature(signature: DictionarySignature, centroids: &[DictionarySignature]) -> usize {
    centroids
        .iter()
        .enumerate()
        .min_by_key(|(_, centroid)| signature_distance(signature, **centroid))
        .map(|(index, _)| index)
        .unwrap_or(0)
}

fn signature_distance(left: DictionarySignature, right: DictionarySignature) -> u32 {
    left.iter()
        .zip(right.iter())
        .map(|(left, right)| (*left as i32 - *right as i32).unsigned_abs())
        .sum()
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    fn structured_json_sample(seed: &str) -> Vec<u8> {
        format!(
            "{{\"kind\":\"config\",\"seed\":\"{seed}\",\"items\":[\"alpha\",\"beta\",\"gamma\"],\"enabled\":true}}"
        )
        .repeat(64)
        .into_bytes()
    }

    fn html_like_sample(seed: &str) -> Vec<u8> {
        format!(
            "<html><body><section data-seed=\"{seed}\"><p>alpha beta gamma</p><div class=\"card\">delta</div></section></body></html>"
        )
        .repeat(64)
        .into_bytes()
    }

    #[test]
    fn trainer_builds_multiple_dictionaries_for_same_extension_class() {
        let mut trainer = DictionaryTrainer::new(ArchiveDictionaryMode::Auto);
        for index in 0..24 {
            trainer.observe_with_path(
                Path::new(&format!("dataset/config-{index}.json")),
                &structured_json_sample("json"),
            );
            trainer.observe_with_path(
                Path::new(&format!("dataset/page-{index}.json")),
                &html_like_sample("html"),
            );
        }

        let bank = trainer
            .build(CompressionAlgo::Zstd, Some(6))
            .expect("dictionary bank should build");

        let json_dictionaries = bank
            .dictionaries()
            .iter()
            .filter(|dictionary| dictionary.class == DictionaryClass::Extension("json".into()))
            .collect::<Vec<_>>();

        assert!(json_dictionaries.len() >= 2);
        assert!(
            json_dictionaries
                .iter()
                .all(|dictionary| dictionary.sample_count >= 8)
        );
        assert!(
            json_dictionaries
                .windows(2)
                .any(|pair| pair[0].signature != pair[1].signature)
        );
    }

    #[test]
    fn selection_prefers_best_matching_dictionary_signature_within_class() {
        let json_signature = sample_signature(&structured_json_sample("json"));
        let html_signature = sample_signature(&html_like_sample("html"));
        let bank = ArchiveDictionaryBank::new(vec![
            ArchiveDictionary {
                id: 1,
                algo: CompressionAlgo::Zstd,
                class: DictionaryClass::Extension("json".to_string()),
                sample_count: 12,
                sample_bytes: 128 * 1024,
                signature: json_signature,
                bytes: vec![1, 2, 3],
            },
            ArchiveDictionary {
                id: 2,
                algo: CompressionAlgo::Zstd,
                class: DictionaryClass::Extension("json".to_string()),
                sample_count: 12,
                sample_bytes: 128 * 1024,
                signature: html_signature,
                bytes: vec![4, 5, 6],
            },
        ])
        .expect("dictionary bank should validate");

        let selected = bank
            .select_for_chunk(
                CompressionAlgo::Zstd,
                &html_like_sample("html"),
                Some(Path::new("nested/data.json")),
            )
            .expect("dictionary should be selected");

        assert_eq!(selected.id, 2);
    }

    #[test]
    fn dictionary_sample_offsets_cover_multiple_windows_for_large_inputs() {
        assert_eq!(dictionary_sample_offsets(16 * 1024), vec![0]);
        assert_eq!(dictionary_sample_offsets(32 * 1024), vec![0, 16 * 1024]);
        assert!(dictionary_sample_offsets(256 * 1024).len() > 3);
    }
}
