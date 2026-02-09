use std::hint::black_box;
use std::io::Write;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use oxide_core::{BufferPool, MmapInput};
use tempfile::NamedTempFile;

fn bench_buffer_pool(c: &mut Criterion) {
    let pool = BufferPool::new(1024, 256);
    c.bench_function("buffer_pool_acquire_release", |b| {
        b.iter(|| {
            let mut buf = pool.acquire();
            buf.extend_from_slice(black_box(b"oxide"));
        })
    });
}

fn bench_mmap_vs_read(c: &mut Criterion) {
    let mut file = NamedTempFile::new().expect("create tempfile");
    let data = vec![b'a'; 4 * 1024 * 1024];
    file.write_all(&data).expect("write data");
    file.flush().expect("flush tempfile");

    let mmap = MmapInput::open(file.path()).expect("open mmap");
    let mut group = c.benchmark_group("mmap_vs_fs_read");
    group.throughput(Throughput::Bytes(data.len() as u64));

    group.bench_with_input(
        BenchmarkId::new("mmap_as_bytes", data.len()),
        &mmap,
        |b, mmap| {
            b.iter(|| {
                let bytes = mmap.as_bytes().expect("read mmap bytes");
                black_box(bytes);
            })
        },
    );

    group.bench_with_input(
        BenchmarkId::new("fs_read", data.len()),
        file.path(),
        |b, path| {
            b.iter(|| {
                let bytes = std::fs::read(path).expect("read file bytes");
                black_box(bytes);
            })
        },
    );

    group.finish();
}

fn run_benches(c: &mut Criterion) {
    bench_buffer_pool(c);
    bench_mmap_vs_read(c);
}

criterion_group!(benches, run_benches);
criterion_main!(benches);
