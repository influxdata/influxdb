use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rand::{distributions::Uniform, Rng};

// The current float encoder produces the following compression:
//
// values   block size  compression
// 10	    32      	25.6 bits/value
// 25	    76      	24.32 bits/value
// 50	    86      	13.76 bits/value
// 100	    167     	13.36 bits/value
// 250	    388     	12.41 bits/value
// 500	    1165        18.64 bits/value
// 750	    1769        18.86 bits/value
// 1000	    2366        18.92 bits/value
// 5000	    11785       18.85 bits/value
// 10000	23559       18.84 bits/value
// 50000	117572      18.81 bits/value
// 100000	235166      18.81 bits/value
//
fn float_encode_random(c: &mut Criterion) {
    let mut group = c.benchmark_group("float_encode_random");
    for batch_size in [
        10_usize, 25, 50, 100, 250, 500, 750, 1000, 5000, 10000, 50000, 100000,
    ]
    .iter()
    {
        group.throughput(Throughput::Bytes(*batch_size as u64 * 8));
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            batch_size,
            |b, &batch_size| {
                let range = Uniform::from(0.0..100.0);
                let decoded: Vec<_> = rand::thread_rng()
                    .sample_iter(&range)
                    .take(batch_size)
                    .collect();
                let mut encoded = vec![];
                b.iter(|| {
                    encoded.truncate(0);
                    delorean::encoders::float::encode(&decoded, &mut encoded).unwrap();
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, float_encode_random);
criterion_main!(benches);
