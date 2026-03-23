use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use lz4_flex::block::{compress_prepend_size, decompress_size_prepended};
use oxide_core::compression::lz4;

fn build_text_fixture(size: usize) -> Vec<u8> {
    let line = b"oxide lz4 benchmark line\n";
    let mut data = Vec::with_capacity(size);

    while data.len() < size {
        let remaining = size - data.len();
        let take = remaining.min(line.len());
        data.extend_from_slice(&line[..take]);
    }

    data
}

fn build_mixed_fixture(size: usize) -> Vec<u8> {
    (0..size).map(|idx| ((idx * 97) % 251) as u8).collect()
}

fn bench_lz4_pair(c: &mut Criterion, name: &str, input: &[u8]) {
    let oxide_compressed = lz4::apply(input).expect("oxide lz4 compression should succeed");
    let flex_compressed = compress_prepend_size(input);

    assert_eq!(
        lz4::reverse(&oxide_compressed).expect("oxide lz4 decompression"),
        input
    );
    assert_eq!(
        decompress_size_prepended(&flex_compressed).expect("lz4_flex decompression"),
        input
    );

    let mut compress_group = c.benchmark_group(format!("lz4_compare/{name}/compress"));
    compress_group.throughput(Throughput::Bytes(input.len() as u64));
    compress_group.bench_with_input(BenchmarkId::new("oxide", input.len()), input, |b, data| {
        let data = data.as_ref();
        b.iter(|| black_box(lz4::apply(black_box(data)).expect("oxide lz4 compression")))
    });
    compress_group.bench_with_input(
        BenchmarkId::new("lz4_flex", input.len()),
        input,
        |b, data| b.iter(|| black_box(compress_prepend_size(black_box(data)))),
    );
    compress_group.finish();

    let mut decompress_group = c.benchmark_group(format!("lz4_compare/{name}/decompress"));
    decompress_group.throughput(Throughput::Bytes(input.len() as u64));
    decompress_group.bench_with_input(
        BenchmarkId::new("oxide", oxide_compressed.len()),
        &oxide_compressed,
        |b, data| {
            let data = data.as_ref();
            b.iter(|| black_box(lz4::reverse(black_box(data)).expect("oxide lz4 decompression")))
        },
    );
    decompress_group.bench_with_input(
        BenchmarkId::new("lz4_flex", flex_compressed.len()),
        &flex_compressed,
        |b, data| {
            let data = data.as_ref();
            b.iter(|| {
                black_box(
                    decompress_size_prepended(black_box(data)).expect("lz4_flex decompression"),
                )
            })
        },
    );
    decompress_group.finish();
}

fn bench_lz4_compare(c: &mut Criterion) {
    let text = build_text_fixture(4 * 1024 * 1024);
    let mixed = build_mixed_fixture(4 * 1024 * 1024);

    bench_lz4_pair(c, "text", &text);
    bench_lz4_pair(c, "mixed", &mixed);
}

criterion_group!(benches, bench_lz4_compare);
criterion_main!(benches);
