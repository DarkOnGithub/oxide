use std::io::{Read, Seek, Write};
use std::path::Path;
use std::sync::Arc;

use crate::buffer::BufferPool;
use crate::core::WorkerPool;
use crate::format::{ArchiveReader, ArchiveWriter};
use crate::io::InputScanner;
use crate::types::{CompressedBlock, CompressionAlgo, PreProcessingStrategy, Result};

/// End-to-end Phase 1 pipeline that wires scanner, workers, and OXZ I/O.
///
/// This pipeline performs pass-through processing for block payloads while preserving
/// the metadata and ordering guarantees needed by the archive format.
pub struct ArchivePipeline {
    scanner: InputScanner,
    num_workers: usize,
    compression_algo: CompressionAlgo,
    buffer_pool: Arc<BufferPool>,
}

impl ArchivePipeline {
    /// Creates a new archive pipeline.
    pub fn new(
        target_block_size: usize,
        num_workers: usize,
        buffer_pool: Arc<BufferPool>,
        compression_algo: CompressionAlgo,
    ) -> Self {
        Self {
            scanner: InputScanner::new(target_block_size),
            num_workers,
            compression_algo,
            buffer_pool,
        }
    }

    /// Reads an input file, processes blocks in parallel, and writes an OXZ archive.
    pub fn archive_file<P, W>(&self, path: P, writer: W) -> Result<W>
    where
        P: AsRef<Path>,
        W: Write,
    {
        let batches = self.scanner.scan_file(path.as_ref())?;
        let block_count = u32::try_from(batches.len())
            .map_err(|_| crate::OxideError::InvalidFormat("too many blocks for OXZ v1"))?;

        let worker_pool = WorkerPool::new(
            self.num_workers,
            Arc::clone(&self.buffer_pool),
            self.compression_algo,
        );

        let handle = worker_pool.spawn(|_worker_id, batch, pool, compression| {
            let mut scratch = pool.acquire();
            scratch.extend_from_slice(batch.data());

            // Move the pooled Vec out without allocating.
            let mut data = Vec::new();
            std::mem::swap(scratch.as_mut_vec(), &mut data);

            Ok(CompressedBlock::new(
                batch.id,
                data,
                PreProcessingStrategy::None,
                compression,
                batch.len() as u64,
            ))
        });

        for batch in batches {
            handle.submit(batch)?;
        }
        let blocks = handle.finish()?;

        let mut archive_writer = ArchiveWriter::new(writer, Arc::clone(&self.buffer_pool));
        archive_writer.write_global_header(block_count)?;
        for block in blocks {
            archive_writer.write_owned_block(block)?;
        }
        archive_writer.write_footer()
    }

    /// Extracts all block payloads from an OXZ archive in block order.
    pub fn extract_archive<R>(&self, reader: R) -> Result<Vec<u8>>
    where
        R: Read + Seek,
    {
        let mut archive = ArchiveReader::new(reader)?;
        let mut output = Vec::new();

        for entry in archive.iter_blocks() {
            let (_header, block_data) = entry?;
            output.extend_from_slice(&block_data);
        }

        Ok(output)
    }
}
