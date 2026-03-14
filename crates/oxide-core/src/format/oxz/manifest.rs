use crate::{ArchiveEntryKind, ArchiveListingEntry, ArchiveTimestamp, OxideError, Result};

pub const ARCHIVE_MANIFEST_MAGIC: [u8; 4] = *b"OXMF";
pub const ARCHIVE_MANIFEST_VERSION: u16 = 2;
const ARCHIVE_MANIFEST_HEADER_SIZE: usize = 12;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ArchiveManifest {
    entries: Vec<ArchiveListingEntry>,
}

impl ArchiveManifest {
    pub fn new(entries: Vec<ArchiveListingEntry>) -> Self {
        Self { entries }
    }

    pub fn entries(&self) -> &[ArchiveListingEntry] {
        &self.entries
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        validate_entries(&self.entries)?;
        let entry_count = u32::try_from(self.entries.len())
            .map_err(|_| OxideError::InvalidFormat("manifest entry count overflow"))?;
        let mut out = Vec::with_capacity(ARCHIVE_MANIFEST_HEADER_SIZE);
        out.extend_from_slice(&ARCHIVE_MANIFEST_MAGIC);
        out.extend_from_slice(&ARCHIVE_MANIFEST_VERSION.to_le_bytes());
        out.extend_from_slice(&0u16.to_le_bytes());
        out.extend_from_slice(&entry_count.to_le_bytes());

        for entry in &self.entries {
            out.push(entry.kind.to_flags());
            encode_path(&mut out, &entry.path)?;
            out.extend_from_slice(&entry.size.to_le_bytes());
            out.extend_from_slice(&entry.mode.to_le_bytes());
            out.extend_from_slice(&entry.mtime.seconds.to_le_bytes());
            out.extend_from_slice(&entry.mtime.nanoseconds.to_le_bytes());
            out.extend_from_slice(&entry.uid.to_le_bytes());
            out.extend_from_slice(&entry.gid.to_le_bytes());
            out.extend_from_slice(&entry.content_offset.to_le_bytes());
        }

        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < ARCHIVE_MANIFEST_HEADER_SIZE {
            return Err(OxideError::InvalidFormat("archive manifest is too short"));
        }
        if bytes[..4] != ARCHIVE_MANIFEST_MAGIC {
            return Err(OxideError::InvalidFormat("invalid archive manifest magic"));
        }

        let version = u16::from_le_bytes([bytes[4], bytes[5]]);
        if version != ARCHIVE_MANIFEST_VERSION {
            return Err(OxideError::InvalidFormat(
                "unsupported archive manifest version",
            ));
        }

        let reserved = u16::from_le_bytes([bytes[6], bytes[7]]);
        if reserved != 0 {
            return Err(OxideError::InvalidFormat(
                "invalid archive manifest reserved bits",
            ));
        }

        let entry_count = u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);
        let mut cursor = ARCHIVE_MANIFEST_HEADER_SIZE;
        let mut entries = Vec::with_capacity(entry_count as usize);

        for _ in 0..entry_count {
            if cursor >= bytes.len() {
                return Err(OxideError::InvalidFormat(
                    "truncated archive manifest entry kind",
                ));
            }

            let flags = bytes[cursor];
            let kind = ArchiveEntryKind::from_flags(flags).ok_or(OxideError::InvalidFormat(
                "invalid archive manifest entry flags",
            ))?;
            cursor += 1;

            let path = decode_path(bytes, &mut cursor)?;
            if cursor + 32 > bytes.len() {
                return Err(OxideError::InvalidFormat(
                    "truncated archive manifest entry metadata",
                ));
            }
            let size = u64::from_le_bytes([
                bytes[cursor],
                bytes[cursor + 1],
                bytes[cursor + 2],
                bytes[cursor + 3],
                bytes[cursor + 4],
                bytes[cursor + 5],
                bytes[cursor + 6],
                bytes[cursor + 7],
            ]);
            cursor += 8;
            let mode = u32::from_le_bytes([
                bytes[cursor],
                bytes[cursor + 1],
                bytes[cursor + 2],
                bytes[cursor + 3],
            ]);
            cursor += 4;
            let mtime_seconds = i64::from_le_bytes([
                bytes[cursor],
                bytes[cursor + 1],
                bytes[cursor + 2],
                bytes[cursor + 3],
                bytes[cursor + 4],
                bytes[cursor + 5],
                bytes[cursor + 6],
                bytes[cursor + 7],
            ]);
            cursor += 8;
            let mtime_nanoseconds = u32::from_le_bytes([
                bytes[cursor],
                bytes[cursor + 1],
                bytes[cursor + 2],
                bytes[cursor + 3],
            ]);
            cursor += 4;
            if mtime_nanoseconds >= 1_000_000_000 {
                return Err(OxideError::InvalidFormat(
                    "archive manifest mtime nanoseconds out of range",
                ));
            }
            let uid = u32::from_le_bytes([
                bytes[cursor],
                bytes[cursor + 1],
                bytes[cursor + 2],
                bytes[cursor + 3],
            ]);
            cursor += 4;
            let gid = u32::from_le_bytes([
                bytes[cursor],
                bytes[cursor + 1],
                bytes[cursor + 2],
                bytes[cursor + 3],
            ]);
            cursor += 4;
            let content_offset = u64::from_le_bytes([
                bytes[cursor],
                bytes[cursor + 1],
                bytes[cursor + 2],
                bytes[cursor + 3],
                bytes[cursor + 4],
                bytes[cursor + 5],
                bytes[cursor + 6],
                bytes[cursor + 7],
            ]);
            cursor += 8;

            if matches!(kind, ArchiveEntryKind::Directory) && (size != 0 || content_offset != 0) {
                return Err(OxideError::InvalidFormat(
                    "directory manifest entries must not carry content ranges",
                ));
            }

            entries.push(ArchiveListingEntry {
                path,
                kind,
                size,
                mode,
                mtime: ArchiveTimestamp {
                    seconds: mtime_seconds,
                    nanoseconds: mtime_nanoseconds,
                },
                uid,
                gid,
                content_offset,
            });
        }

        if cursor != bytes.len() {
            return Err(OxideError::InvalidFormat(
                "archive manifest has trailing data",
            ));
        }

        validate_entries(&entries)?;

        Ok(Self { entries })
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

#[cfg(test)]
mod tests {
    use super::ArchiveManifest;
    use crate::{ArchiveEntryKind, ArchiveListingEntry, ArchiveTimestamp};

    #[test]
    fn manifest_round_trips_extended_entry_metadata() {
        let manifest = ArchiveManifest::new(vec![
            ArchiveListingEntry::directory(
                "nested".to_string(),
                0o755,
                ArchiveTimestamp {
                    seconds: 1_710_000_000,
                    nanoseconds: 42,
                },
                1000,
                100,
            ),
            ArchiveListingEntry::file(
                "nested/data.bin".to_string(),
                7,
                0o640,
                ArchiveTimestamp {
                    seconds: 1_710_000_123,
                    nanoseconds: 99,
                },
                1000,
                100,
                0,
            ),
        ]);

        let bytes = manifest.encode().expect("encode manifest");
        let decoded = ArchiveManifest::decode(&bytes).expect("decode manifest");

        assert_eq!(decoded, manifest);
        assert_eq!(decoded.entries()[0].kind, ArchiveEntryKind::Directory);
        assert_eq!(decoded.entries()[1].content_offset, 0);
    }
}

fn encode_path(out: &mut Vec<u8>, path: &str) -> Result<()> {
    let bytes = path.as_bytes();
    let len = u32::try_from(bytes.len())
        .map_err(|_| OxideError::InvalidFormat("path length overflow"))?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(bytes);
    Ok(())
}

fn decode_path(bytes: &[u8], cursor: &mut usize) -> Result<String> {
    if *cursor + 4 > bytes.len() {
        return Err(OxideError::InvalidFormat(
            "truncated archive manifest path length",
        ));
    }

    let len = u32::from_le_bytes([
        bytes[*cursor],
        bytes[*cursor + 1],
        bytes[*cursor + 2],
        bytes[*cursor + 3],
    ]) as usize;
    *cursor += 4;

    let end = cursor.checked_add(len).ok_or(OxideError::InvalidFormat(
        "archive manifest path offset overflow",
    ))?;
    if end > bytes.len() {
        return Err(OxideError::InvalidFormat(
            "truncated archive manifest path data",
        ));
    }

    let path = std::str::from_utf8(&bytes[*cursor..end])
        .map_err(|_| OxideError::InvalidFormat("archive manifest path is not utf8"))?
        .to_string();
    *cursor = end;
    Ok(path)
}
