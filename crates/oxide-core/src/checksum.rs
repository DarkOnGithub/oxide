use std::io::{Read, Seek, SeekFrom};

use crate::{ChunkDescriptor, OxideError, Result};

#[inline]
pub fn compute_checksum(bytes: &[u8]) -> u32 {
    crc32c::crc32c(bytes)
}

#[inline]
pub fn verify_checksum(bytes: &[u8], expected: u32) -> bool {
    compute_checksum(bytes) == expected
}

/// Vérifie l'intégrité des chunks d'une archive.
///
/// Pour chaque ChunkDescriptor :
/// - on va à `payload_offset`
/// - on lit `encoded_len` octets
/// - on recalcule le checksum
/// - on compare avec `descriptor.checksum`
///
/// Les chunks de référence sont ignorés car leur champ `checksum` contient
/// en réalité l'index de la référence, pas un vrai checksum.
pub fn detect_corrupted_chunks<R: Read + Seek>(
    reader: &mut R,
    descriptors: &[ChunkDescriptor],
) -> Result<()> {
    for descriptor in descriptors {
        if descriptor.is_reference() {
            continue;
        }

        reader.seek(SeekFrom::Start(descriptor.payload_offset))?;

        let mut buffer = vec![0u8; descriptor.encoded_len as usize];
        reader.read_exact(&mut buffer)?;

        if !verify_checksum(&buffer, descriptor.checksum) {
            return Err(OxideError::InvalidFormat("chunk checksum mismatch"));
        }
    }

    Ok(())
}