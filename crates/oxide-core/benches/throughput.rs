use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use oxide_core::format::FormatDetector;

fn bench_format_detector(c: &mut Criterion) {
    let text_data = vec![b'a'; 4 * 1024 * 1024];
    let binary_data = vec![0xFFu8; 4 * 1024 * 1024];

    let mut group = c.benchmark_group("format_detector");

    group.throughput(Throughput::Bytes(text_data.len() as u64));
    group.bench_function("text_4mb", |b| {
        b.iter(|| FormatDetector::detect(black_box(&text_data)))
    });

    group.throughput(Throughput::Bytes(binary_data.len() as u64));
    group.bench_function("binary_4mb", |b| {
        b.iter(|| FormatDetector::detect(black_box(&binary_data)))
    });

    group.finish();
}

criterion_group!(benches, bench_format_detector);
criterion_main!(benches);
