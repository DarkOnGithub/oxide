use std::collections::BTreeSet;

use crate::{OxideError, Result};

const DICTIONARY_STORE_HEADER_BYTES: usize = 4;
const DICTIONARY_ENTRY_HEADER_BYTES: usize = 8;

/// Dictionary payload referenced by chunk descriptors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredDictionary {
    pub id: u16,
    pub data: Vec<u8>,
}

impl StoredDictionary {
    pub fn new(id: u16, data: Vec<u8>) -> Result<Self> {
        if id == 0 {
            return Err(OxideError::InvalidFormat(
                "dictionary id 0 is reserved for no-dictionary",
            ));
        }
        if data.is_empty() {
            return Err(OxideError::InvalidFormat(
                "dictionary payload cannot be empty",
            ));
        }
        Ok(Self { id, data })
    }
}

pub fn encode_dictionary_store(dictionaries: &[StoredDictionary]) -> Result<Vec<u8>> {
    validate_dictionaries(dictionaries)?;
    if dictionaries.is_empty() {
        return Ok(Vec::new());
    }

    let mut total_len = DICTIONARY_STORE_HEADER_BYTES;
    for dictionary in dictionaries {
        total_len = total_len
            .checked_add(DICTIONARY_ENTRY_HEADER_BYTES)
            .and_then(|len| len.checked_add(dictionary.data.len()))
            .ok_or(OxideError::InvalidFormat(
                "dictionary store length overflow",
            ))?;
    }

    let count = u16::try_from(dictionaries.len())
        .map_err(|_| OxideError::InvalidFormat("dictionary count exceeds u16 range"))?;
    let mut bytes = Vec::with_capacity(total_len);
    bytes.extend_from_slice(&count.to_le_bytes());
    bytes.extend_from_slice(&0u16.to_le_bytes());

    for dictionary in dictionaries {
        let len = u32::try_from(dictionary.data.len())
            .map_err(|_| OxideError::InvalidFormat("dictionary length exceeds u32 range"))?;
        bytes.extend_from_slice(&dictionary.id.to_le_bytes());
        bytes.extend_from_slice(&0u16.to_le_bytes());
        bytes.extend_from_slice(&len.to_le_bytes());
        bytes.extend_from_slice(&dictionary.data);
    }

    Ok(bytes)
}

pub fn decode_dictionary_store(bytes: &[u8]) -> Result<Vec<StoredDictionary>> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    if bytes.len() < DICTIONARY_STORE_HEADER_BYTES {
        return Err(OxideError::InvalidFormat(
            "dictionary store section is too short",
        ));
    }

    let count = u16::from_le_bytes([bytes[0], bytes[1]]) as usize;
    let reserved = u16::from_le_bytes([bytes[2], bytes[3]]);
    if reserved != 0 {
        return Err(OxideError::InvalidFormat(
            "dictionary store reserved bits must be zero",
        ));
    }

    let mut cursor = DICTIONARY_STORE_HEADER_BYTES;
    let mut dictionaries = Vec::with_capacity(count);
    for _ in 0..count {
        if cursor + DICTIONARY_ENTRY_HEADER_BYTES > bytes.len() {
            return Err(OxideError::InvalidFormat(
                "dictionary store entry header truncated",
            ));
        }

        let id = u16::from_le_bytes([bytes[cursor], bytes[cursor + 1]]);
        let reserved = u16::from_le_bytes([bytes[cursor + 2], bytes[cursor + 3]]);
        let len = u32::from_le_bytes([
            bytes[cursor + 4],
            bytes[cursor + 5],
            bytes[cursor + 6],
            bytes[cursor + 7],
        ]) as usize;
        cursor += DICTIONARY_ENTRY_HEADER_BYTES;

        if reserved != 0 {
            return Err(OxideError::InvalidFormat(
                "dictionary store entry reserved bits must be zero",
            ));
        }
        let end = cursor.checked_add(len).ok_or(OxideError::InvalidFormat(
            "dictionary store offset overflow",
        ))?;
        if end > bytes.len() {
            return Err(OxideError::InvalidFormat(
                "dictionary store entry payload truncated",
            ));
        }

        dictionaries.push(StoredDictionary::new(id, bytes[cursor..end].to_vec())?);
        cursor = end;
    }

    if cursor != bytes.len() {
        return Err(OxideError::InvalidFormat(
            "dictionary store has trailing bytes",
        ));
    }

    validate_dictionaries(&dictionaries)?;
    Ok(dictionaries)
}

pub fn validate_dictionaries(dictionaries: &[StoredDictionary]) -> Result<()> {
    let mut ids = BTreeSet::new();
    for dictionary in dictionaries {
        if dictionary.id == 0 {
            return Err(OxideError::InvalidFormat(
                "dictionary id 0 is reserved for no-dictionary",
            ));
        }
        if dictionary.data.is_empty() {
            return Err(OxideError::InvalidFormat(
                "dictionary payload cannot be empty",
            ));
        }
        if !ids.insert(dictionary.id) {
            return Err(OxideError::InvalidFormat(
                "duplicate dictionary id in dictionary store",
            ));
        }
    }
    Ok(())
}
