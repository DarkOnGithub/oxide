use crate::{
    ArchiveDictionary, ArchiveDictionaryBank, ArchiveEntryKind, ArchiveListingEntry,
    ArchiveTimestamp, CompressionAlgo, DictionaryClass, OxideError, Result,
};

const MANIFEST_MAGIC: [u8; 4] = *b"OXM2";
const MANIFEST_VERSION: u8 = 1;

const ENTRY_FLAG_DIRECTORY: u8 = 1 << 0;
const ENTRY_FLAG_SYMLINK: u8 = 1 << 1;
const ENTRY_FLAG_MODE_PRESENT: u8 = 1 << 2;
const ENTRY_FLAG_UID_PRESENT: u8 = 1 << 3;
const ENTRY_FLAG_GID_PRESENT: u8 = 1 << 4;
const ENTRY_FLAG_MTIME_SECONDS_PRESENT: u8 = 1 << 5;
const ENTRY_FLAG_MTIME_NANOS_PRESENT: u8 = 1 << 6;
const ENTRY_FLAG_RESERVED_MASK: u8 = 1 << 7;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ArchiveManifest {
    dictionary_bank: ArchiveDictionaryBank,
    entries: Vec<ArchiveListingEntry>,
}

impl ArchiveManifest {
    pub fn new(entries: Vec<ArchiveListingEntry>) -> Self {
        Self {
            dictionary_bank: ArchiveDictionaryBank::default(),
            entries,
        }
    }

    pub fn with_dictionary_bank(mut self, dictionary_bank: ArchiveDictionaryBank) -> Self {
        self.dictionary_bank = dictionary_bank;
        self
    }

    pub fn entries(&self) -> &[ArchiveListingEntry] {
        &self.entries
    }

    pub fn dictionary_bank(&self) -> &ArchiveDictionaryBank {
        &self.dictionary_bank
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        validate_entries(&self.entries)?;
        self.dictionary_bank.validate()?;

        let mut out = Vec::new();
        out.extend_from_slice(&MANIFEST_MAGIC);
        out.push(MANIFEST_VERSION);
        encode_varint(
            &mut out,
            u64::try_from(self.dictionary_bank.dictionaries().len())
                .map_err(|_| OxideError::InvalidFormat("manifest dictionary count overflow"))?,
        );

        for dictionary in self.dictionary_bank.dictionaries() {
            out.push(dictionary.id);
            out.push(dictionary.algo.to_flags());
            out.push(dictionary.class.id());
            encode_varint(
                &mut out,
                u64::try_from(dictionary.bytes.len()).map_err(|_| {
                    OxideError::InvalidFormat("manifest dictionary length overflow")
                })?,
            );
            out.extend_from_slice(&dictionary.bytes);
        }

        encode_varint(
            &mut out,
            u64::try_from(self.entries.len())
                .map_err(|_| OxideError::InvalidFormat("manifest entry count overflow"))?,
        );

        let mut previous_path = Vec::new();
        let mut previous_mode = 0u32;
        let mut previous_uid = 0u32;
        let mut previous_gid = 0u32;
        let mut previous_mtime_seconds = 0i64;
        let mut previous_mtime_nanoseconds = 0u32;

        for entry in &self.entries {
            let mut flags = match entry.kind {
                ArchiveEntryKind::Directory => ENTRY_FLAG_DIRECTORY,
                ArchiveEntryKind::File => 0,
                ArchiveEntryKind::Symlink => ENTRY_FLAG_SYMLINK,
            };

            if entry.mode != previous_mode {
                flags |= ENTRY_FLAG_MODE_PRESENT;
            }
            if entry.uid != previous_uid {
                flags |= ENTRY_FLAG_UID_PRESENT;
            }
            if entry.gid != previous_gid {
                flags |= ENTRY_FLAG_GID_PRESENT;
            }
            if entry.mtime.seconds != previous_mtime_seconds {
                flags |= ENTRY_FLAG_MTIME_SECONDS_PRESENT;
            }
            if entry.mtime.nanoseconds != previous_mtime_nanoseconds {
                flags |= ENTRY_FLAG_MTIME_NANOS_PRESENT;
            }

            out.push(flags);

            let path_bytes = entry.path.as_bytes();
            let prefix_len = common_prefix_len(&previous_path, path_bytes);
            let suffix = &path_bytes[prefix_len..];
            encode_varint(&mut out, prefix_len as u64);
            encode_varint(
                &mut out,
                u64::try_from(suffix.len())
                    .map_err(|_| OxideError::InvalidFormat("manifest path length overflow"))?,
            );
            out.extend_from_slice(suffix);

            if matches!(entry.kind, ArchiveEntryKind::Symlink) {
                let target = entry.target.as_ref().ok_or(OxideError::InvalidFormat(
                    "symlink manifest entry missing target",
                ))?;
                let target_bytes = target.as_bytes();
                encode_varint(
                    &mut out,
                    u64::try_from(target_bytes.len()).map_err(|_| {
                        OxideError::InvalidFormat("manifest target length overflow")
                    })?,
                );
                out.extend_from_slice(target_bytes);
            }

            if matches!(entry.kind, ArchiveEntryKind::File) {
                encode_varint(&mut out, entry.size);
            }
            if flags & ENTRY_FLAG_MODE_PRESENT != 0 {
                encode_varint(&mut out, entry.mode as u64);
            }
            if flags & ENTRY_FLAG_UID_PRESENT != 0 {
                encode_varint(&mut out, entry.uid as u64);
            }
            if flags & ENTRY_FLAG_GID_PRESENT != 0 {
                encode_varint(&mut out, entry.gid as u64);
            }
            if flags & ENTRY_FLAG_MTIME_SECONDS_PRESENT != 0 {
                encode_varint(
                    &mut out,
                    zigzag_encode_i64(entry.mtime.seconds - previous_mtime_seconds),
                );
            }
            if flags & ENTRY_FLAG_MTIME_NANOS_PRESENT != 0 {
                encode_varint(&mut out, entry.mtime.nanoseconds as u64);
            }

            previous_path.clear();
            previous_path.extend_from_slice(path_bytes);
            previous_mode = entry.mode;
            previous_uid = entry.uid;
            previous_gid = entry.gid;
            previous_mtime_seconds = entry.mtime.seconds;
            previous_mtime_nanoseconds = entry.mtime.nanoseconds;
        }

        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut cursor = 0usize;
        if bytes.len() < MANIFEST_MAGIC.len() + 1 {
            return Err(OxideError::InvalidFormat("archive manifest is truncated"));
        }
        if bytes[..MANIFEST_MAGIC.len()] != MANIFEST_MAGIC {
            return Err(OxideError::InvalidFormat("invalid archive manifest magic"));
        }
        cursor += MANIFEST_MAGIC.len();
        let version = bytes[cursor];
        cursor += 1;
        if version != MANIFEST_VERSION {
            return Err(OxideError::InvalidFormat(
                "unsupported archive manifest version",
            ));
        }

        let dictionary_count = decode_varint(bytes, &mut cursor)?;
        let dictionary_count = usize::try_from(dictionary_count).map_err(|_| {
            OxideError::InvalidFormat("manifest dictionary count exceeds usize range")
        })?;
        let mut dictionaries = Vec::with_capacity(dictionary_count);
        for _ in 0..dictionary_count {
            if cursor + 3 > bytes.len() {
                return Err(OxideError::InvalidFormat(
                    "truncated archive dictionary header",
                ));
            }

            let id = bytes[cursor];
            let algo = CompressionAlgo::from_flags(bytes[cursor + 1])?;
            let class = DictionaryClass::from_id(bytes[cursor + 2])?;
            cursor += 3;

            let dictionary_len = decode_varint(bytes, &mut cursor)?;
            let dictionary_len = usize::try_from(dictionary_len).map_err(|_| {
                OxideError::InvalidFormat("manifest dictionary length exceeds usize range")
            })?;
            let dictionary_end =
                cursor
                    .checked_add(dictionary_len)
                    .ok_or(OxideError::InvalidFormat(
                        "manifest dictionary range overflow",
                    ))?;
            if dictionary_end > bytes.len() {
                return Err(OxideError::InvalidFormat(
                    "truncated archive dictionary bytes",
                ));
            }

            dictionaries.push(ArchiveDictionary {
                id,
                algo,
                class,
                bytes: bytes[cursor..dictionary_end].to_vec(),
            });
            cursor = dictionary_end;
        }
        let dictionary_bank = ArchiveDictionaryBank::new(dictionaries)?;

        let entry_count = decode_varint(bytes, &mut cursor)?;
        let entry_count = usize::try_from(entry_count)
            .map_err(|_| OxideError::InvalidFormat("manifest entry count exceeds usize range"))?;

        let mut entries = Vec::with_capacity(entry_count);
        let mut previous_path = Vec::new();
        let mut previous_mode = 0u32;
        let mut previous_uid = 0u32;
        let mut previous_gid = 0u32;
        let mut previous_mtime_seconds = 0i64;
        let mut previous_mtime_nanoseconds = 0u32;
        let mut content_offset = 0u64;

        for _ in 0..entry_count {
            if cursor >= bytes.len() {
                return Err(OxideError::InvalidFormat(
                    "truncated archive manifest entry flags",
                ));
            }

            let flags = bytes[cursor];
            cursor += 1;
            if flags & ENTRY_FLAG_RESERVED_MASK != 0 {
                return Err(OxideError::InvalidFormat(
                    "invalid archive manifest entry flags",
                ));
            }

            let prefix_len = decode_varint(bytes, &mut cursor)?;
            let prefix_len = usize::try_from(prefix_len)
                .map_err(|_| OxideError::InvalidFormat("manifest prefix length overflow"))?;
            if prefix_len > previous_path.len() {
                return Err(OxideError::InvalidFormat(
                    "manifest path prefix exceeds previous path length",
                ));
            }

            let suffix_len = decode_varint(bytes, &mut cursor)?;
            let suffix_len = usize::try_from(suffix_len)
                .map_err(|_| OxideError::InvalidFormat("manifest suffix length overflow"))?;
            let suffix_end = cursor
                .checked_add(suffix_len)
                .ok_or(OxideError::InvalidFormat("manifest path range overflow"))?;
            if suffix_end > bytes.len() {
                return Err(OxideError::InvalidFormat(
                    "truncated archive manifest path data",
                ));
            }

            let mut path_bytes = previous_path[..prefix_len].to_vec();
            path_bytes.extend_from_slice(&bytes[cursor..suffix_end]);
            cursor = suffix_end;

            let path = String::from_utf8(path_bytes.clone())
                .map_err(|_| OxideError::InvalidFormat("archive manifest path is not utf8"))?;

            let kind_bits = flags & (ENTRY_FLAG_DIRECTORY | ENTRY_FLAG_SYMLINK);
            let kind = match kind_bits {
                0 => ArchiveEntryKind::File,
                ENTRY_FLAG_DIRECTORY => ArchiveEntryKind::Directory,
                ENTRY_FLAG_SYMLINK => ArchiveEntryKind::Symlink,
                _ => {
                    return Err(OxideError::InvalidFormat(
                        "invalid archive manifest entry kind flags",
                    ));
                }
            };
            let target = if matches!(kind, ArchiveEntryKind::Symlink) {
                let target_len = decode_varint(bytes, &mut cursor)?;
                let target_len = usize::try_from(target_len)
                    .map_err(|_| OxideError::InvalidFormat("manifest target length overflow"))?;
                let target_end = cursor
                    .checked_add(target_len)
                    .ok_or(OxideError::InvalidFormat("manifest target range overflow"))?;
                if target_end > bytes.len() {
                    return Err(OxideError::InvalidFormat(
                        "truncated archive manifest target data",
                    ));
                }
                let target =
                    String::from_utf8(bytes[cursor..target_end].to_vec()).map_err(|_| {
                        OxideError::InvalidFormat("archive manifest symlink target is not utf8")
                    })?;
                cursor = target_end;
                Some(target)
            } else {
                None
            };
            let size = if matches!(kind, ArchiveEntryKind::File) {
                decode_varint(bytes, &mut cursor)?
            } else {
                0
            };

            let mode = if flags & ENTRY_FLAG_MODE_PRESENT != 0 {
                let value = decode_varint(bytes, &mut cursor)?;
                u32::try_from(value)
                    .map_err(|_| OxideError::InvalidFormat("manifest mode exceeds u32 range"))?
            } else {
                previous_mode
            };

            let uid = if flags & ENTRY_FLAG_UID_PRESENT != 0 {
                let value = decode_varint(bytes, &mut cursor)?;
                u32::try_from(value)
                    .map_err(|_| OxideError::InvalidFormat("manifest uid exceeds u32 range"))?
            } else {
                previous_uid
            };

            let gid = if flags & ENTRY_FLAG_GID_PRESENT != 0 {
                let value = decode_varint(bytes, &mut cursor)?;
                u32::try_from(value)
                    .map_err(|_| OxideError::InvalidFormat("manifest gid exceeds u32 range"))?
            } else {
                previous_gid
            };

            let mtime_seconds = if flags & ENTRY_FLAG_MTIME_SECONDS_PRESENT != 0 {
                let delta = zigzag_decode_i64(decode_varint(bytes, &mut cursor)?);
                previous_mtime_seconds
                    .checked_add(delta)
                    .ok_or(OxideError::InvalidFormat("manifest mtime seconds overflow"))?
            } else {
                previous_mtime_seconds
            };

            let mtime_nanoseconds = if flags & ENTRY_FLAG_MTIME_NANOS_PRESENT != 0 {
                let value = decode_varint(bytes, &mut cursor)?;
                let value = u32::try_from(value).map_err(|_| {
                    OxideError::InvalidFormat("manifest mtime nanoseconds exceeds u32 range")
                })?;
                if value >= 1_000_000_000 {
                    return Err(OxideError::InvalidFormat(
                        "archive manifest mtime nanoseconds out of range",
                    ));
                }
                value
            } else {
                previous_mtime_nanoseconds
            };

            let entry_content_offset = match kind {
                ArchiveEntryKind::Directory => 0,
                ArchiveEntryKind::Symlink => 0,
                ArchiveEntryKind::File => {
                    let offset = content_offset;
                    content_offset = content_offset
                        .checked_add(size)
                        .ok_or(OxideError::InvalidFormat("manifest content range overflow"))?;
                    offset
                }
            };

            entries.push(ArchiveListingEntry {
                path,
                kind,
                target,
                size,
                mode,
                mtime: ArchiveTimestamp {
                    seconds: mtime_seconds,
                    nanoseconds: mtime_nanoseconds,
                },
                uid,
                gid,
                content_offset: entry_content_offset,
            });

            previous_path = path_bytes;
            previous_mode = mode;
            previous_uid = uid;
            previous_gid = gid;
            previous_mtime_seconds = mtime_seconds;
            previous_mtime_nanoseconds = mtime_nanoseconds;
        }

        if cursor != bytes.len() {
            return Err(OxideError::InvalidFormat(
                "archive manifest has trailing data",
            ));
        }

        validate_entries(&entries)?;
        Ok(Self {
            dictionary_bank,
            entries,
        })
    }
}

fn validate_entries(entries: &[ArchiveListingEntry]) -> Result<()> {
    let mut expected_content_offset = 0u64;

    for entry in entries {
        match entry.kind {
            ArchiveEntryKind::Directory => {
                if entry.size != 0 || entry.content_offset != 0 {
                    return Err(OxideError::InvalidFormat(
                        "directory manifest entries must not carry content ranges",
                    ));
                }
            }
            ArchiveEntryKind::Symlink => {
                if entry.size != 0 || entry.content_offset != 0 || entry.target.is_none() {
                    return Err(OxideError::InvalidFormat(
                        "symlink manifest entries must not carry content ranges",
                    ));
                }
            }
            ArchiveEntryKind::File => {
                if entry.content_offset != expected_content_offset {
                    return Err(OxideError::InvalidFormat(
                        "file manifest content offsets must be contiguous",
                    ));
                }
                expected_content_offset = expected_content_offset
                    .checked_add(entry.size)
                    .ok_or(OxideError::InvalidFormat("manifest content range overflow"))?;
            }
        }
    }

    Ok(())
}

fn common_prefix_len(left: &[u8], right: &[u8]) -> usize {
    left.iter()
        .zip(right.iter())
        .take_while(|(left, right)| left == right)
        .count()
}

fn encode_varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn decode_varint(bytes: &[u8], cursor: &mut usize) -> Result<u64> {
    let mut shift = 0u32;
    let mut value = 0u64;

    loop {
        if *cursor >= bytes.len() {
            return Err(OxideError::InvalidFormat("truncated manifest varint"));
        }
        if shift >= 64 {
            return Err(OxideError::InvalidFormat("manifest varint overflow"));
        }

        let byte = bytes[*cursor];
        *cursor += 1;

        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
        shift += 7;
    }
}

fn zigzag_encode_i64(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)) as u64
}

fn zigzag_decode_i64(value: u64) -> i64 {
    ((value >> 1) as i64) ^ (-((value & 1) as i64))
}

#[cfg(test)]
#[path = "../../../tests/format/oxz/manifest.rs"]
mod tests;
