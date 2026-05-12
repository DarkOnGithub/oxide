use reed_solomon_erasure::galois_8::ReedSolomon;

use crate::format::oxz::headers::RecoveryMetadata;
use crate::{OxideError, Result};

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