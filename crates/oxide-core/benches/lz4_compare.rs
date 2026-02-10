use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use lz4_flex::block::{
    compress_prepend_size as flex_compress, decompress_size_prepended as flex_decompress,
};
use oxide_core::compression::lz4 as ours;

struct Dataset {
    name: &'static str,
    data: Vec<u8>,
}

fn build_text_data(size: usize) -> Vec<u8> {
    let line = b"Oxide benchmark line: repeated textual payload for LZ4 throughput measurement.\n";
    let mut data = Vec::with_capacity(size);
    while data.len() < size {
        let remaining = size - data.len();
        let take = remaining.min(line.len());
        data.extend_from_slice(&line[..take]);
    }
    data
}

fn build_mixed_data(size: usize) -> Vec<u8> {
    let mut data = Vec::with_capacity(size);
    let mut i = 0usize;
    while data.len() < size {
        data.extend_from_slice(b"oxide-lz4-");
        data.push((i & 0xFF) as u8);
        data.push(((i * 7) & 0xFF) as u8);
        data.extend((0u8..=63).cycle().take(20));
        i += 1;
    }
    data.truncate(size);
    data
}

fn build_random_data(size: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(size);
    let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
    while out.len() < size {
        // xorshift64*
        state ^= state >> 12;
        state ^= state << 25;
        state ^= state >> 27;
        let next = state.wrapping_mul(0x2545_F491_4F6C_DD1D);
        out.extend_from_slice(&next.to_le_bytes());
    }
    out.truncate(size);
    out
}

fn datasets() -> Vec<Dataset> {
    let size = 1024 * 1024;
    vec![
        Dataset {
            name: "text_1mb",
            data: build_text_data(size),
        },
        Dataset {
            name: "mixed_1mb",
            data: build_mixed_data(size),
        },
        Dataset {
            name: "random_1mb",
            data: build_random_data(size),
        },
    ]
}

fn print_compression_ratio_summary() {
    let datasets = datasets();

    println!();
    println!("=== Compression Ratio Summary (lower is better) ===");
    println!(
        "{:<14} {:>12} {:>12} {:>12} {:>12}",
        "dataset", "ours_ratio", "flex_ratio", "ours_bytes", "flex_bytes"
    );

    for ds in &datasets {
        let ours_payload = ours::apply(&ds.data).expect("ours ratio encode");
        let flex_payload = flex_compress(&ds.data);

        let ours_ratio = ours_payload.len() as f64 / ds.data.len() as f64;
        let flex_ratio = flex_payload.len() as f64 / ds.data.len() as f64;

        println!(
            "{:<14} {:>12.6} {:>12.6} {:>12} {:>12}",
            ds.name,
            ours_ratio,
            flex_ratio,
            ours_payload.len(),
            flex_payload.len()
        );
    }
}

fn bench_lz4_compress(c: &mut Criterion) {
    let datasets = datasets();
    let mut group = c.benchmark_group("lz4_compress");

    for ds in &datasets {
        group.throughput(Throughput::Bytes(ds.data.len() as u64));
        group.bench_with_input(BenchmarkId::new("ours", ds.name), &ds.data, |b, data| {
            b.iter(|| ours::apply(black_box(data)).expect("ours compress"))
        });
        group.bench_with_input(
            BenchmarkId::new("lz4_flex", ds.name),
            &ds.data,
            |b, data| b.iter(|| flex_compress(black_box(data))),
        );
    }

    group.finish();
}

fn bench_lz4_decompress(c: &mut Criterion) {
    let datasets = datasets();
    let mut group = c.benchmark_group("lz4_decompress");

    for ds in &datasets {
        let ours_payload = ours::apply(&ds.data).expect("ours pre-compress");
        let flex_payload = flex_compress(&ds.data);

        group.throughput(Throughput::Bytes(ds.data.len() as u64));
        group.bench_with_input(
            BenchmarkId::new("ours", ds.name),
            &ours_payload,
            |b, payload| b.iter(|| ours::reverse(black_box(payload)).expect("ours decompress")),
        );
        group.bench_with_input(
            BenchmarkId::new("lz4_flex", ds.name),
            &flex_payload,
            |b, payload| {
                b.iter(|| flex_decompress(black_box(payload)).expect("lz4_flex decompress"))
            },
        );
    }

    group.finish();
}

fn bench_lz4_compare(c: &mut Criterion) {
    bench_lz4_compress(c);
    bench_lz4_decompress(c);
    print_compression_ratio_summary();
}

criterion_group!(benches, bench_lz4_compare);
criterion_main!(benches);
