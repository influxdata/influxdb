use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rand::Rng;

fn integer_decode_random(c: &mut Criterion) {
    let mut group = c.benchmark_group("integer_decode_random");
    for batch_size in [
        10_i32, 25, 50, 100, 250, 500, 750, 1000, 5000, 10000, 50000, 100000,
    ]
    .iter()
    {
        let decoded: Vec<i64> = (1..*batch_size)
            .map(|_| rand::thread_rng().gen_range(0, 100))
            .collect();
        let mut encoded = vec![];
        delorean::encoders::integer::encode(&decoded, &mut encoded).unwrap();

        group.throughput(Throughput::Bytes(encoded.len() as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            &decoded,
            |b, decoded| {
                let mut decoded_mut = Vec::with_capacity(decoded.len());
                b.iter(|| {
                    decoded_mut.truncate(0);
                    delorean::encoders::integer::decode(&encoded, &mut decoded_mut).unwrap();
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, integer_decode_random);
criterion_main!(benches);
