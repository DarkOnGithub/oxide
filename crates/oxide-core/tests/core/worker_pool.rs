use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use bytes::Bytes;
use oxide_core::{
    Batch, BufferPool, CompressedBlock, CompressionAlgo, FileFormat, PreProcessingStrategy,
    WorkerPool,
};

fn make_batch(id: usize, data: Vec<u8>) -> Batch {
    Batch::with_hint(id, "synthetic.bin", Bytes::from(data), FileFormat::Binary)
}

#[test]
fn worker_pool_processes_all_batches() -> Result<(), Box<dyn std::error::Error>> {
    let buffer_pool = Arc::new(BufferPool::new(128, 32));
    let worker_pool = WorkerPool::new(4, Arc::clone(&buffer_pool), CompressionAlgo::Lz4);
    let handle = worker_pool.spawn(|_worker_id, batch, pool, compression| {
        let mut scratch = pool.acquire();
        scratch.extend_from_slice(batch.data());

        Ok(CompressedBlock::new(
            batch.id,
            scratch.to_vec(),
            PreProcessingStrategy::None,
            compression,
            batch.len() as u64,
        ))
    });

    let mut expected_payloads = Vec::new();
    for id in 0..64usize {
        let payload = vec![id as u8; 64 + (id % 7)];
        expected_payloads.push(payload.clone());
        handle.submit(make_batch(id, payload))?;
    }

    let blocks = handle.finish()?;
    assert_eq!(blocks.len(), expected_payloads.len());

    for (idx, block) in blocks.iter().enumerate() {
        assert_eq!(block.id, idx);
        assert_eq!(block.data, expected_payloads[idx]);
        assert_eq!(block.compression, CompressionAlgo::Lz4);
        assert_eq!(block.pre_proc, PreProcessingStrategy::None);
        assert!(block.verify_crc32());
    }

    let metrics = buffer_pool.metrics();
    assert!(metrics.created > 0);

    Ok(())
}

#[test]
fn worker_pool_balances_mixed_workloads() -> Result<(), Box<dyn std::error::Error>> {
    let worker_count = 4usize;
    let buffer_pool = Arc::new(BufferPool::new(64, 16));
    let worker_pool = WorkerPool::new(
        worker_count,
        Arc::clone(&buffer_pool),
        CompressionAlgo::Deflate,
    );
    let task_counts: Arc<Vec<AtomicUsize>> = Arc::new(
        (0..worker_count)
            .map(|_| AtomicUsize::new(0))
            .collect::<Vec<_>>(),
    );
    let seen_workers = Arc::new(Mutex::new(std::collections::BTreeSet::new()));

    let counts_for_worker = Arc::clone(&task_counts);
    let seen_for_worker = Arc::clone(&seen_workers);
    let handle = worker_pool.spawn(move |worker_id, batch, _pool, compression| {
        counts_for_worker[worker_id].fetch_add(1, Ordering::AcqRel);
        seen_for_worker
            .lock()
            .expect("seen set mutex poisoned")
            .insert(worker_id);

        // Simulate uneven task costs to exercise work stealing.
        thread::sleep(Duration::from_millis((batch.id % 4) as u64));

        Ok(CompressedBlock::new(
            batch.id,
            batch.to_owned().to_vec(),
            PreProcessingStrategy::None,
            compression,
            batch.len() as u64,
        ))
    });

    for id in 0..120usize {
        let payload = vec![(id % 251) as u8; 32];
        handle.submit(make_batch(id, payload))?;
    }

    let blocks = handle.finish()?;
    assert_eq!(blocks.len(), 120);

    let seen = seen_workers.lock().expect("seen set mutex poisoned");
    assert_eq!(seen.len(), worker_count);

    let distributed_total: usize = task_counts
        .iter()
        .map(|counter| counter.load(Ordering::Acquire))
        .sum();
    assert_eq!(distributed_total, 120);
    assert!(
        task_counts
            .iter()
            .all(|counter| counter.load(Ordering::Acquire) > 0)
    );

    Ok(())
}

#[test]
fn shutdown_rejects_new_work_and_drains_existing_tasks() -> Result<(), Box<dyn std::error::Error>> {
    let buffer_pool = Arc::new(BufferPool::new(64, 8));
    let worker_pool = WorkerPool::new(2, buffer_pool, CompressionAlgo::Lzma);
    let handle = worker_pool.spawn(|_worker_id, batch, _pool, compression| {
        Ok(CompressedBlock::new(
            batch.id,
            batch.to_owned().to_vec(),
            PreProcessingStrategy::None,
            compression,
            batch.len() as u64,
        ))
    });

    for id in 0..16usize {
        handle.submit(make_batch(id, vec![id as u8; 24]))?;
    }

    handle.shutdown();
    assert!(handle.submit(make_batch(99, vec![1, 2, 3])).is_err());

    let blocks = handle.finish()?;
    assert_eq!(blocks.len(), 16);
    assert_eq!(blocks.first().map(|b| b.id), Some(0));
    assert_eq!(blocks.last().map(|b| b.id), Some(15));

    Ok(())
}
