use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

mod fixtures;

fn float_decode_cpu(c: &mut Criterion) {
    let mut group = c.benchmark_group("float_decode_cpu");
    for batch_size in [10_i32, 25, 50, 100, 250, 500, 750, 1000, 5000, 10000, 45000].iter() {
        let decoded: Vec<f64> = fixtures::CPU_F64_EXAMPLE_VALUES[..*batch_size as usize].to_vec();
        let mut encoded = vec![];
        delorean::encoders::float::encode(&decoded, &mut encoded).unwrap();

        group.throughput(Throughput::Bytes(encoded.len() as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            &decoded,
            |b, decoded| {
                b.iter(|| {
                    let mut decoded_mut = Vec::with_capacity(decoded.len());
                    decoded_mut.truncate(0);
                    delorean::encoders::float::decode(&encoded, &mut decoded_mut).unwrap();
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, float_decode_cpu);
criterion_main!(benches);
