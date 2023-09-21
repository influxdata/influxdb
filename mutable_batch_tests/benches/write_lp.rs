use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use mutable_batch_lp::lines_to_batches;
use mutable_batch_tests::benchmark_lp;

fn generate_lp_bytes() -> Vec<(String, Bytes)> {
    benchmark_lp()
        .into_iter()
        .map(|(bench, lp)| (bench, lp.into()))
        .collect()
}

pub fn write_lp(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_lp");
    for (bench, lp_bytes) in generate_lp_bytes() {
        group.throughput(Throughput::Bytes(lp_bytes.len() as u64));
        group.bench_function(BenchmarkId::from_parameter(bench), |b| {
            b.iter(|| {
                let batches = lines_to_batches(std::str::from_utf8(&lp_bytes).unwrap(), 0).unwrap();
                assert_eq!(batches.len(), 1);
            });
        });
    }
    group.finish();
}

criterion_group!(benches, write_lp);
criterion_main!(benches);
