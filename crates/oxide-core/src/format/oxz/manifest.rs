use crate::{ArchiveEntryKind, ArchiveListingEntry, OxideError, Result};

pub const ARCHIVE_MANIFEST_MAGIC: [u8; 4] = *b"OXMF";
pub const ARCHIVE_MANIFEST_VERSION: u16 = 1;
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
        let entry_count = u32::try_from(self.entries.len())
            .map_err(|_| OxideError::InvalidFormat("manifest entry count overflow"))?;
        let mut out = Vec::with_capacity(ARCHIVE_MANIFEST_HEADER_SIZE);
        out.extend_from_slice(&ARCHIVE_MANIFEST_MAGIC);
        out.extend_from_slice(&ARCHIVE_MANIFEST_VERSION.to_le_bytes());
        out.extend_from_slice(&0u16.to_le_bytes());
        out.extend_from_slice(&entry_count.to_le_bytes());

        for entry in &self.entries {
            out.push(match entry.kind {
                ArchiveEntryKind::File => 1,
                ArchiveEntryKind::Directory => 0,
            });
            encode_path(&mut out, &entry.path)?;
            out.extend_from_slice(&entry.size.to_le_bytes());
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

            let kind = match bytes[cursor] {
                0 => ArchiveEntryKind::Directory,
                1 => ArchiveEntryKind::File,
                _ => {
                    return Err(OxideError::InvalidFormat(
                        "invalid archive manifest entry kind",
                    ))
                }
            };
            cursor += 1;

            let path = decode_path(bytes, &mut cursor)?;
            if cursor + 8 > bytes.len() {
                return Err(OxideError::InvalidFormat(
                    "truncated archive manifest entry size",
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

            entries.push(ArchiveListingEntry { path, kind, size });
        }

        if cursor != bytes.len() {
            return Err(OxideError::InvalidFormat(
                "archive manifest has trailing data",
            ));
        }

        Ok(Self { entries })
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
