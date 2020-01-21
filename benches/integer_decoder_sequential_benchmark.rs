use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

fn integer_decode_sequential(c: &mut Criterion) {
    let mut group = c.benchmark_group("integer_decode_sequential");
    for batch_size in [
        10_i32, 25, 50, 100, 250, 500, 750, 1000, 5000, 10000, 50000, 100000,
    ]
    .iter()
    {
        let src: Vec<i64> = (1..*batch_size).map(i64::from).collect();
        let mut dst = vec![];
        delorean::encoders::integer::encode_all(&src, &mut dst).unwrap();

        group.throughput(Throughput::Bytes(dst.len() as u64));
        group.bench_with_input(BenchmarkId::from_parameter(batch_size), &src, |b, src| {
            let mut src_mut = src.clone();
            b.iter(|| {
                src_mut.truncate(0);
                delorean::encoders::integer::decode_all(&dst, &mut src_mut).unwrap();
            });
        });
    }
    group.finish();
}

criterion_group!(benches, integer_decode_sequential);
criterion_main!(benches);
