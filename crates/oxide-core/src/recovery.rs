use reed_solomon_erasure::galois_8::ReedSolomon;

use crate::format::oxz::headers::RecoveryMetadata;
use crate::{OxideError, Result};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
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
                max_block_len: 0,
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
            max_block_len: self.max_block_len as u32,
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
    let file_len = in_file.seek(SeekFrom::End(0))?;
    if file_len < 40 {
        return Err(OxideError::InvalidFormat("Archive is too small"));
    }

    // 1. Quick check for existing protection (Byte 6)
    let mut header_flags = [0u8; 2];
    in_file.seek(SeekFrom::Start(6))?;
    in_file.read_exact(&mut header_flags)?;
    let flags = u16::from_le_bytes(header_flags);
    if flags & HEADER_FLAG_RECOVERY != 0 {
        return Err(OxideError::InvalidFormat("Archive is already protected"));
    }

    // 2. Read the original footer
    in_file.seek(SeekFrom::End(-40))?;
    let mut footer_bytes = [0u8; 40];
    in_file.read_exact(&mut footer_bytes)?;

    // 3. Dynamic shard sizing to stay under 256 total shards (Reed-Solomon Galois-8 limit)
    let target_data_shards = 200u64;
    let mut shard_size = (file_len + target_data_shards - 1) / target_data_shards;
    shard_size = shard_size.max(1024 * 1024); // Minimum 1MB chunks
    
    let data_count = ((file_len + shard_size - 1) / shard_size) as usize;
    let parity_count = parity_count_for_percentage(data_count, percentage)?;
    
    if data_count + parity_count > 255 {
        return Err(OxideError::RecoveryDataInvalid);
    }

    let rs = reed_solomon_erasure::galois_8::ReedSolomon::new(data_count, parity_count)
        .map_err(|_| OxideError::RecoveryDataInvalid)?;

    let mut shards = vec![vec![0u8; shard_size as usize]; data_count + parity_count];
    let mut checksums = Vec::with_capacity(data_count);

    in_file.seek(SeekFrom::Start(0))?;
    for i in 0..data_count {
        let start_pos = (i as u64) * shard_size;
        let end_pos = ((i as u64 + 1) * shard_size).min(file_len);
        let bytes_to_read = (end_pos - start_pos) as usize;

        let mut bytes_read = 0;
        while bytes_read < bytes_to_read {
            let n = in_file.read(&mut shards[i][bytes_read..bytes_to_read])?;
            if n == 0 { break; }
            bytes_read += n;
        }
        
        // HOT FIX: Pre-flip the flag in the RAM shard so mathematical parity perfectly matches the final file!
        if i == 0 && bytes_to_read >= 8 {
            let new_flags = flags | HEADER_FLAG_RECOVERY;
            let flag_bytes = new_flags.to_le_bytes();
            shards[0][6] = flag_bytes[0];
            shards[0][7] = flag_bytes[1];
        }
        
        checksums.push(crate::checksum::compute_checksum(&shards[i]));
    }

    // 4. Compute Parity
    rs.encode(&mut shards).map_err(|_| OxideError::RecoveryDataInvalid)?;

    let mut out_file = File::create(output_path)?;
    
    // 5. Write the protected file structure
    // A. The original data (with the modified flag)
    for i in 0..data_count {
        let start_pos = (i as u64) * shard_size;
        let end_pos = ((i as u64 + 1) * shard_size).min(file_len);
        let bytes_to_write = (end_pos - start_pos) as usize;
        out_file.write_all(&shards[i][..bytes_to_write])?;
    }
    
    // B. The physical CRC32 map (to detect corruption without parsing Oxide chunks)
    for crc in &checksums {
        out_file.write_all(&crc.to_le_bytes())?;
    }
    
    // C. The Parity Shards
    for i in data_count..(data_count + parity_count) {
        out_file.write_all(&shards[i])?;
    }
    
    // D. The 21-byte Contract
    // HACK: We artificially add 40 bytes to parity_bytes_len so ArchiveReader skips the original footer properly!
    let parity_bytes_len = (parity_count as u64 * shard_size) + (data_count as u64 * 4) + 40;
    let meta = RecoveryMetadata {
        percentage,
        data_block_count: data_count as u32,
        parity_block_count: parity_count as u32,
        parity_bytes_len,
        max_block_len: shard_size as u32,
    };
    
    out_file.write_all(&[meta.percentage])?;
    out_file.write_all(&meta.data_block_count.to_le_bytes())?;
    out_file.write_all(&meta.parity_block_count.to_le_bytes())?;
    out_file.write_all(&meta.parity_bytes_len.to_le_bytes())?;
    out_file.write_all(&meta.max_block_len.to_le_bytes())?;
    
    // E. The duplicated valid footer for ArchiveReader
    out_file.write_all(&footer_bytes)?;

    Ok(())
}

pub fn repair_corrupted_archive(
    input_path: &std::path::Path,
    output_path: &std::path::Path,
) -> Result<()> {
    let mut in_file = File::open(input_path)?;
    let file_len = in_file.seek(SeekFrom::End(0))?;
    
    if file_len < 61 { // 40 (footer) + 21 (metadata)
        return Err(OxideError::RecoveryDataInvalid);
    }
    
    // 1. Read metadata from the physical end of the file (completely bypassing ArchiveReader!)
    in_file.seek(SeekFrom::End(-61))?;
    let mut rec_buf = [0u8; 21];
    in_file.read_exact(&mut rec_buf)?;
    
    let meta = RecoveryMetadata {
        percentage: rec_buf[0],
        data_block_count: u32::from_le_bytes(rec_buf[1..5].try_into().unwrap()),
        parity_block_count: u32::from_le_bytes(rec_buf[5..9].try_into().unwrap()),
        parity_bytes_len: u64::from_le_bytes(rec_buf[9..17].try_into().unwrap()),
        max_block_len: u32::from_le_bytes(rec_buf[17..21].try_into().unwrap()),
    };

    let data_count = meta.data_block_count as usize;
    let parity_count = meta.parity_block_count as usize;
    let shard_size = meta.max_block_len as usize;

    if data_count == 0 || parity_count == 0 {
        in_file.seek(SeekFrom::Start(0))?;
        let mut out_file = File::create(output_path)?;
        std::io::copy(&mut in_file, &mut out_file)?;
        return Ok(());
    }

    let checksums_len = (data_count * 4) as u64;
    let pure_parity_len = (parity_count * shard_size) as u64;
    
    // We artificially added 40 bytes to parity_bytes_len during protect
    let actual_parity_struct_len = meta.parity_bytes_len.saturating_sub(40);
    
    if actual_parity_struct_len != checksums_len + pure_parity_len {
        return Err(OxideError::RecoveryDataInvalid);
    }

    let original_file_len = file_len - 61 - actual_parity_struct_len;

    // 2. Read physical checksums
    in_file.seek(SeekFrom::Start(original_file_len))?;
    let mut expected_checksums = Vec::with_capacity(data_count);
    for _ in 0..data_count {
        let mut crc_buf = [0u8; 4];
        in_file.read_exact(&mut crc_buf)?;
        expected_checksums.push(u32::from_le_bytes(crc_buf));
    }

    // 3. Read parity
    let mut shards = vec![None; data_count + parity_count];
    for i in 0..parity_count {
        let mut shard = vec![0u8; shard_size];
        in_file.read_exact(&mut shard)?;
        shards[data_count + i] = Some(shard);
    }

    // 4. Scan original file chunks physically and check their CRC32
    let mut missing_count = 0;
    in_file.seek(SeekFrom::Start(0))?;
    
    for i in 0..data_count {
        let mut shard = vec![0u8; shard_size];
        let start_pos = (i * shard_size) as u64;
        let end_pos = ((i + 1) * shard_size).min(original_file_len as usize) as u64;
        let bytes_to_read = (end_pos - start_pos) as usize;

        let mut bytes_read = 0;
        let mut read_failed = false;
        while bytes_read < bytes_to_read {
            match in_file.read(&mut shard[bytes_read..bytes_to_read]) {
                Ok(0) => break,
                Ok(n) => bytes_read += n,
                Err(_) => { read_failed = true; break; }
            }
        }

        if read_failed || bytes_read < bytes_to_read {
            missing_count += 1;
            continue;
        }

        if crate::checksum::verify_checksum(&shard, expected_checksums[i]) {
            shards[i] = Some(shard);
        } else {
            missing_count += 1;
        }
    }

    if missing_count > parity_count {
        return Err(crate::OxideError::TooMuchCorruption {
            max_allowed: parity_count,
            found: missing_count,
        });
    }

    if missing_count > 0 {
        let rs = reed_solomon_erasure::galois_8::ReedSolomon::new(data_count, parity_count)
            .map_err(|_| OxideError::RecoveryDataInvalid)?;
        rs.reconstruct(&mut shards).map_err(|_| OxideError::RecoveryDataInvalid)?;
    }

    // 5. Write the fully repaired file
    let mut out_file = File::create(output_path)?;
    
    // A. Restored Original File (including the flag and original footer)
    for i in 0..data_count {
        if let Some(shard) = &shards[i] {
            let start_pos = (i * shard_size) as u64;
            let end_pos = ((i + 1) * shard_size).min(original_file_len as usize) as u64;
            let bytes_to_write = (end_pos - start_pos) as usize;
            out_file.write_all(&shard[..bytes_to_write])?;
        }
    }
    
    // B. Re-write the protection data so the output remains protected
    for crc in &expected_checksums { out_file.write_all(&crc.to_le_bytes())?; }
    for i in data_count..(data_count + parity_count) {
        out_file.write_all(shards[i].as_ref().unwrap())?;
    }
    out_file.write_all(&rec_buf)?;
    
    // C. Re-write the second footer
    in_file.seek(SeekFrom::End(-40))?;
    let mut footer_bytes = [0u8; 40];
    in_file.read_exact(&mut footer_bytes)?;
    out_file.write_all(&footer_bytes)?;

    Ok(())
}