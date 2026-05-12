use reed_solomon_erasure::galois_8::ReedSolomon;

use crate::format::oxz::headers::RecoveryMetadata;
use crate::{OxideError, Result};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use crate::format::oxz::ArchiveReader;
use crate::format::oxz::headers::HEADER_FLAG_RECOVERY;
/// Reed-Solomon encoder used by the OXZ writer.
///
/// The writer calls `push_data` once per encoded payload block.
/// At the end of the archive, `finish` pads those variable-sized blocks
/// to a common shard size and generates parity shards.
#[derive(Debug)]
pub struct RecoveryEncoder {
    pub percentage: u8,
    pub data_block_count: u32,
    data_blocks: Vec<Vec<u8>>,
    max_block_len: usize,
}

impl RecoveryEncoder {
    pub fn new(percentage: u8) -> Self {
        Self {
            percentage,
            data_block_count: 0,
            data_blocks: Vec::new(),
            max_block_len: 0,
        }
    }

    /// Called by the writer each time an encoded block is written to disk.
    pub fn push_data(&mut self, data: &[u8]) {
        self.data_block_count = self.data_block_count.saturating_add(1);
        self.max_block_len = self.max_block_len.max(data.len());
        self.data_blocks.push(data.to_vec());
    }

    /// Builds the Reed-Solomon parity bytes and the fixed 17-byte metadata.
    ///
    /// Returned parity layout:
    ///
    /// parity shard 0 || parity shard 1 || ... || parity shard N-1
    ///
    /// All shards must have the same size for Reed-Solomon, so smaller archive
    /// blocks are padded with zeroes only for the mathematical computation.
    /// Their real sizes are still stored in the normal chunk table.
    pub fn finish(self) -> Result<(RecoveryMetadata, Vec<u8>)> {
        if !(1..=20).contains(&self.percentage) {
            return Err(OxideError::InvalidRecoveryPercentage(self.percentage));
        }

        let data_count = self.data_blocks.len();

        if data_count == 0 || self.max_block_len == 0 {
            let meta = RecoveryMetadata {
                percentage: self.percentage,
                data_block_count: self.data_block_count,
                parity_block_count: 0,
                parity_bytes_len: 0,
            };

            return Ok((meta, Vec::new()));
        }

        if data_count != self.data_block_count as usize {
            return Err(OxideError::RecoveryDataInvalid);
        }

        let parity_count = parity_count_for_percentage(data_count, self.percentage)?;

        let r = ReedSolomon::new(data_count, parity_count)
            .map_err(|_| OxideError::RecoveryDataInvalid)?;

        let mut shards = Vec::with_capacity(data_count + parity_count);

        for mut block in self.data_blocks {
            block.resize(self.max_block_len, 0);
            shards.push(block);
        }

        for _ in 0..parity_count {
            shards.push(vec![0u8; self.max_block_len]);
        }

        r.encode(&mut shards)
            .map_err(|_| OxideError::RecoveryDataInvalid)?;

        let parity_bytes_len = parity_count
            .checked_mul(self.max_block_len)
            .and_then(|len| u64::try_from(len).ok())
            .ok_or(OxideError::RecoveryDataInvalid)?;

        let mut parity_bytes = Vec::with_capacity(parity_bytes_len as usize);

        for shard in shards.into_iter().skip(data_count) {
            parity_bytes.extend_from_slice(&shard);
        }

        let meta = RecoveryMetadata {
            percentage: self.percentage,
            data_block_count: self.data_block_count,
            parity_block_count: u32::try_from(parity_count)
                .map_err(|_| OxideError::RecoveryDataInvalid)?,
            parity_bytes_len,
        };

        Ok((meta, parity_bytes))
    }
}

fn parity_count_for_percentage(data_count: usize, percentage: u8) -> Result<usize> {
    let parity_count = data_count
        .checked_mul(percentage as usize)
        .and_then(|n| n.checked_add(99))
        .map(|n| n / 100)
        .ok_or(OxideError::RecoveryDataInvalid)?;

    Ok(parity_count.max(1))
}

pub fn protect_existing_archive(
    input_path: &std::path::Path,
    output_path: &std::path::Path,
    percentage: u8,
) -> Result<()> {
    let mut in_file = File::open(input_path)?;
    let reader = ArchiveReader::new(in_file.try_clone()?)?;

    let header = reader.global_header();

    if header.flags & HEADER_FLAG_RECOVERY != 0 {
        return Err(OxideError::InvalidFormat("L'archive est déjà protégée"));
    }

    let mut encoder = RecoveryEncoder::new(percentage);

    for i in 0..reader.block_count() {
        let desc = reader.block_descriptor(i)?;
        
        if desc.is_reference() {
            encoder.push_data(&[]);
        } else {
            let mut block_data = vec![0u8; desc.encoded_len as usize];
            in_file.seek(SeekFrom::Start(desc.payload_offset))?;
            in_file.read_exact(&mut block_data)?;
            encoder.push_data(&block_data);
        }
    }

    let (meta, parity_bytes) = encoder.finish()?;

    let mut out_file = File::create(output_path)?;

    in_file.seek(SeekFrom::Start(0))?;
    let mut data_section = (&mut in_file).take(header.footer_offset);
    std::io::copy(&mut data_section, &mut out_file)?;

    out_file.write_all(&parity_bytes)?;
    out_file.write_all(&[meta.percentage])?;
    out_file.write_all(&meta.data_block_count.to_le_bytes())?;
    out_file.write_all(&meta.parity_block_count.to_le_bytes())?;
    out_file.write_all(&meta.parity_bytes_len.to_le_bytes())?;

    in_file.seek(SeekFrom::Start(header.footer_offset))?;
    std::io::copy(&mut in_file, &mut out_file)?;

    let new_flags = header.flags | HEADER_FLAG_RECOVERY;
    out_file.seek(SeekFrom::Start(6))?; 
    out_file.write_all(&new_flags.to_le_bytes())?;

    Ok(())
}

pub fn repair_corrupted_archive(
    input_path: &std::path::Path,
    output_path: &std::path::Path,
) -> Result<()> {
    let mut in_file = File::open(input_path)?;
    
    std::fs::copy(input_path, output_path)?;
    let mut out_file = std::fs::OpenOptions::new().read(true).write(true).open(output_path)?;

    let reader = ArchiveReader::new(in_file.try_clone()?)?;
    let meta = reader.recovery_metadata().ok_or(crate::OxideError::RecoveryDataInvalid)?.clone();
    
    let data_count = meta.data_block_count as usize;
    let parity_count = meta.parity_block_count as usize;
    
    if data_count == 0 || parity_count == 0 {
        return Ok(()); 
    }

    let rs = ReedSolomon::new(data_count, parity_count)
        .map_err(|_| crate::OxideError::RecoveryDataInvalid)?;

    let mut shards: Vec<Option<Vec<u8>>> = vec![None; data_count + parity_count];
    let mut max_len = 0;
    let mut missing_count = 0;

    for i in 0..reader.block_count() {
        let desc = reader.block_descriptor(i)?;
        if desc.is_reference() {
            shards[i as usize] = Some(Vec::new());
            continue;
        }

        let mut block_data = vec![0u8; desc.encoded_len as usize];
        in_file.seek(SeekFrom::Start(desc.payload_offset))?;
        
        match in_file.read_exact(&mut block_data) {
            Ok(_) => {
                max_len = max_len.max(block_data.len());
                shards[i as usize] = Some(block_data);
            }
            Err(_) => {
                missing_count += 1;
            }
        }
    }

    if missing_count == 0 {
        return Ok(());
    }

    if missing_count > parity_count {
        return Err(crate::OxideError::TooMuchCorruption {
            max_allowed: parity_count,
            found: missing_count,
        });
    }

    let parity_start = reader.global_header().footer_offset - 17 - meta.parity_bytes_len;
    in_file.seek(SeekFrom::Start(parity_start))?;
    let mut all_parity = vec![0u8; meta.parity_bytes_len as usize];
    in_file.read_exact(&mut all_parity)?;

    let parity_chunk_size = meta.parity_bytes_len as usize / parity_count;
    max_len = max_len.max(parity_chunk_size); 
    for (i, chunk) in all_parity.chunks(parity_chunk_size).enumerate() {
        shards[data_count + i] = Some(chunk.to_vec());
    }

    for shard in shards.iter_mut().flatten() {
        shard.resize(max_len, 0);
    }

    rs.reconstruct(&mut shards).map_err(|_| crate::OxideError::RecoveryDataInvalid)?;

    for i in 0..reader.block_count() {
        let desc = reader.block_descriptor(i)?;
        if desc.is_reference() {
            continue;
        }
        if let Some(ref recovered_data) = shards[i as usize] {
            out_file.seek(SeekFrom::Start(desc.payload_offset))?;
            out_file.write_all(&recovered_data[..desc.encoded_len as usize])?;
        }
    }

    Ok(())
}