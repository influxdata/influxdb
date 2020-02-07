use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

fn timestamp_encode_sequential(c: &mut Criterion) {
    let mut group = c.benchmark_group("timestamp_encode_sequential");
    for batch_size in [
        10_i32, 25, 50, 100, 250, 500, 750, 1000, 5000, 10000, 50000, 100000,
    ]
    .iter()
    {
        group.throughput(Throughput::Bytes(*batch_size as u64 * 8));
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            batch_size,
            |b, &batch_size| {
                let src: Vec<i64> = (1..batch_size).map(i64::from).collect();
                let mut dst = vec![];
                b.iter(|| {
                    dst.truncate(0);
                    delorean::encoders::timestamp::encode_all(&src, &mut dst).unwrap();
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, timestamp_encode_sequential);
criterion_main!(benches);
