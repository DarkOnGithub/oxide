use std::collections::HashMap;

use crate::format::StoredDictionary;
use crate::{Batch, FileFormat, Result};

const DEFAULT_DICTIONARY_BYTES: usize = 16 * 1024;
const DEFAULT_FRAGMENT_BYTES: usize = 96;
const MIN_DICTIONARY_SOURCE_BYTES: usize = 512;

#[derive(Debug, Clone)]
pub struct DictionaryCatalog {
    entries: Vec<StoredDictionary>,
    global_dict_id: Option<u16>,
    type_dict_ids: HashMap<FileFormat, u16>,
}

impl DictionaryCatalog {
    pub fn from_batches(batches: &[Batch], max_bytes: usize) -> Result<Self> {
        let max_bytes = max_bytes.max(DEFAULT_FRAGMENT_BYTES);
        let mut next_id = 1u16;
        let mut entries = Vec::new();
        let mut global_sources = Vec::with_capacity(batches.len());
        let mut format_sources = HashMap::<FileFormat, Vec<&[u8]>>::new();

        for batch in batches {
            let bytes = batch.data();
            if bytes.len() < MIN_DICTIONARY_SOURCE_BYTES {
                continue;
            }
            global_sources.push(bytes);
            format_sources
                .entry(batch.file_type_hint)
                .or_default()
                .push(bytes);
        }

        let global_dict_id =
            build_dictionary_entry(&global_sources, max_bytes, &mut next_id, &mut entries)?;

        let mut type_dict_ids = HashMap::new();
        for (format, sources) in format_sources {
            if format == FileFormat::Common {
                continue;
            }
            if let Some(id) =
                build_dictionary_entry(&sources, max_bytes, &mut next_id, &mut entries)?
            {
                type_dict_ids.insert(format, id);
            }
        }

        Ok(Self {
            entries,
            global_dict_id,
            type_dict_ids,
        })
    }

    pub fn entries(&self) -> &[StoredDictionary] {
        &self.entries
    }

    pub fn total_bytes(&self) -> usize {
        self.entries.iter().map(|entry| entry.data.len()).sum()
    }

    pub fn count(&self) -> usize {
        self.entries.len()
    }

    pub fn get(&self, dict_id: u16) -> Option<&[u8]> {
        self.entries
            .iter()
            .find(|entry| entry.id == dict_id)
            .map(|entry| entry.data.as_slice())
    }

    pub fn candidate_dict_ids(&self, format: FileFormat) -> Vec<u16> {
        let mut candidates = vec![0];
        if let Some(id) = self.global_dict_id {
            candidates.push(id);
        }
        if let Some(id) = self.type_dict_ids.get(&format).copied() {
            if !candidates.contains(&id) {
                candidates.push(id);
            }
        }
        candidates
    }
}

impl Default for DictionaryCatalog {
    fn default() -> Self {
        Self::from_batches(&[], DEFAULT_DICTIONARY_BYTES).unwrap_or_else(|_| Self {
            entries: Vec::new(),
            global_dict_id: None,
            type_dict_ids: HashMap::new(),
        })
    }
}

fn build_dictionary_entry(
    sources: &[&[u8]],
    max_bytes: usize,
    next_id: &mut u16,
    entries: &mut Vec<StoredDictionary>,
) -> Result<Option<u16>> {
    let dictionary = build_dictionary_bytes(sources, max_bytes);
    if dictionary.is_empty() {
        return Ok(None);
    }

    let id = *next_id;
    *next_id = next_id.saturating_add(1);
    entries.push(StoredDictionary::new(id, dictionary)?);
    Ok(Some(id))
}

fn build_dictionary_bytes(sources: &[&[u8]], max_bytes: usize) -> Vec<u8> {
    let mut dictionary = Vec::with_capacity(max_bytes);
    let mut total_source_bytes = 0usize;
    for source in sources {
        total_source_bytes = total_source_bytes.saturating_add(source.len());
    }
    if total_source_bytes < MIN_DICTIONARY_SOURCE_BYTES {
        return dictionary;
    }

    for source in sources {
        append_fragments(source, &mut dictionary, max_bytes);
        if dictionary.len() >= max_bytes {
            break;
        }
    }
    dictionary
}

fn append_fragments(source: &[u8], dictionary: &mut Vec<u8>, max_bytes: usize) {
    if dictionary.len() >= max_bytes || source.is_empty() {
        return;
    }

    let fragment_bytes = DEFAULT_FRAGMENT_BYTES.min(max_bytes.saturating_sub(dictionary.len()));
    if fragment_bytes == 0 {
        return;
    }

    let mut starts = vec![0];
    if source.len() > fragment_bytes {
        starts.push(source.len() / 2);
    }
    if source.len() > fragment_bytes * 2 {
        starts.push(source.len().saturating_sub(fragment_bytes));
    }

    starts.sort_unstable();
    starts.dedup();

    for start in starts {
        if dictionary.len() >= max_bytes {
            break;
        }
        let end = start.saturating_add(fragment_bytes).min(source.len());
        if end > start {
            let remaining = max_bytes - dictionary.len();
            let take = (end - start).min(remaining);
            dictionary.extend_from_slice(&source[start..start + take]);
        }
    }
}
