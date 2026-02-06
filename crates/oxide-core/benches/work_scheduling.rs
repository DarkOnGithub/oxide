use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::thread;

use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use oxide_core::WorkStealingQueue;

fn synthetic_cost(task_index: usize) -> usize {
    match task_index % 8 {
        0 => 1_600,
        1 => 1_200,
        2 => 900,
        3 => 700,
        4 => 400,
        5 => 250,
        6 => 140,
        _ => 60,
    }
}

fn simulate_work(cost: usize) -> u64 {
    let mut acc = 0x9E37_79B9_7F4A_7C15u64 ^ (cost as u64);
    for i in 0..(cost * 32) {
        acc = acc
            .wrapping_mul(6364136223846793005)
            .wrapping_add(i as u64 + 1442695040888963407);
    }
    acc
}

fn run_static_scheduling(tasks: &[usize], num_workers: usize) -> u64 {
    let workers = num_workers.max(1);
    let chunk_size = tasks.len().div_ceil(workers);

    let mut handles = Vec::with_capacity(workers);
    for chunk in tasks.chunks(chunk_size.max(1)) {
        let local_tasks = chunk.to_vec();
        handles.push(thread::spawn(move || {
            local_tasks
                .into_iter()
                .fold(0u64, |sum, cost| sum ^ simulate_work(cost))
        }));
    }

    handles
        .into_iter()
        .map(|handle| handle.join().expect("static worker panicked"))
        .fold(0u64, |sum, worker_sum| sum ^ worker_sum)
}

fn run_work_stealing_scheduling(tasks: &[usize], num_workers: usize) -> u64 {
    let workers = num_workers.max(1);
    let queue = Arc::new(WorkStealingQueue::new(workers));
    for &task in tasks {
        queue.submit(task);
    }

    let completed = Arc::new(AtomicUsize::new(0));
    let checksum = Arc::new(AtomicU64::new(0));
    let total = tasks.len();

    let mut handles = Vec::with_capacity(workers);
    for worker_id in 0..workers {
        let worker = queue.worker(worker_id).expect("worker should be available");
        let completed = Arc::clone(&completed);
        let checksum = Arc::clone(&checksum);

        handles.push(thread::spawn(move || {
            loop {
                if let Some(cost) = worker.steal() {
                    let local = simulate_work(cost);
                    checksum.fetch_xor(local, Ordering::AcqRel);
                    completed.fetch_add(1, Ordering::AcqRel);
                    continue;
                }

                if completed.load(Ordering::Acquire) >= total {
                    break;
                }

                thread::yield_now();
            }
        }));
    }

    for handle in handles {
        handle.join().expect("work-stealing worker panicked");
    }

    checksum.load(Ordering::Acquire)
}

fn bench_work_scheduling(c: &mut Criterion) {
    let workers = num_cpus::get().clamp(2, 8);
    let tasks: Vec<usize> = (0..768).map(synthetic_cost).collect();

    let mut group = c.benchmark_group("work_scheduling");
    group.throughput(Throughput::Elements(tasks.len() as u64));

    group.bench_function("static_partition", |b| {
        b.iter(|| black_box(run_static_scheduling(&tasks, workers)))
    });

    group.bench_function("work_stealing", |b| {
        b.iter(|| black_box(run_work_stealing_scheduling(&tasks, workers)))
    });

    group.finish();
}

criterion_group!(benches, bench_work_scheduling);
criterion_main!(benches);
