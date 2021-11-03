use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use entry::Entry;
use mutable_batch_entry::entry_to_batches;
use mutable_batch_tests::benchmark_lp;

fn generate_entry_bytes() -> Vec<(String, (usize, Bytes))> {
    benchmark_lp()
        .into_iter()
        .map(|(bench, lp)| {
            (
                bench,
                (lp.len(), entry::test_helpers::lp_to_entry(&lp).into()),
            )
        })
        .collect()
}

pub fn write_entry(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_entry");

    for (bench, (lp_bytes, entry_bytes)) in generate_entry_bytes() {
        group.throughput(Throughput::Bytes(lp_bytes as u64));
        group.bench_function(BenchmarkId::from_parameter(bench), |b| {
            b.iter(|| {
                let entry: Entry = entry_bytes.clone().try_into().unwrap();

                let batches = entry_to_batches(&entry).unwrap();
                assert_eq!(batches.len(), 1);
            });
        });
    }
    group.finish();
}

criterion_group!(benches, write_entry);
criterion_main!(benches);
