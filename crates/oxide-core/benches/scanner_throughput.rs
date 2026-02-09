use std::hint::black_box;
use std::io::Write;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use oxide_core::InputScanner;
use tempfile::NamedTempFile;

fn build_text_data(size: usize) -> Vec<u8> {
    let line = b"oxide scanner benchmark line\n";
    let mut data = Vec::with_capacity(size);
    while data.len() < size {
        let remaining = size - data.len();
        let take = remaining.min(line.len());
        data.extend_from_slice(&line[..take]);
    }
    data
}

fn build_binary_data(size: usize) -> Vec<u8> {
    (0..size).map(|idx| ((idx * 97) % 251) as u8).collect()
}

fn write_fixture(data: &[u8]) -> NamedTempFile {
    let mut file = NamedTempFile::new().expect("create fixture file");
    file.write_all(data).expect("write fixture file");
    file.flush().expect("flush fixture file");
    file
}

fn bench_input_scanner(c: &mut Criterion) {
    let text = build_text_data(8 * 1024 * 1024);
    let binary = build_binary_data(8 * 1024 * 1024);
    let text_file = write_fixture(&text);
    let binary_file = write_fixture(&binary);
    let scanner = InputScanner::new(64 * 1024);

    let mut group = c.benchmark_group("input_scanner");
    group.throughput(Throughput::Bytes(text.len() as u64));
    group.bench_with_input(
        BenchmarkId::new("text_scan", text.len()),
        text_file.path(),
        |b, path| {
            b.iter(|| {
                let batches = scanner.scan_file(path).expect("scan text fixture");
                black_box(batches.len());
            })
        },
    );

    group.throughput(Throughput::Bytes(binary.len() as u64));
    group.bench_with_input(
        BenchmarkId::new("binary_scan", binary.len()),
        binary_file.path(),
        |b, path| {
            b.iter(|| {
                let batches = scanner.scan_file(path).expect("scan binary fixture");
                black_box(batches.len());
            })
        },
    );

    group.finish();
}

criterion_group!(benches, bench_input_scanner);
criterion_main!(benches);
