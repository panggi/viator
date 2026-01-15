//! Benchmark for RESP protocol parsing.

use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};

fn benchmark_parsing(c: &mut Criterion) {
    // Placeholder benchmark
    c.benchmark_group("protocol")
        .throughput(Throughput::Bytes(1000))
        .bench_function("placeholder", |b| {
            b.iter(|| black_box(1 + 1))
        });
}

criterion_group!(benches, benchmark_parsing);
criterion_main!(benches);
