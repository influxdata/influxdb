use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

mod fixtures;

// The current float encoder produces the following compression:
//
//  values      block size      compression
//  10	        91	            72.8 bits/value
//  25      	208         	66.56 bits/value
//  50      	411         	65.76 bits/value
//  100     	809         	64.72 bits/value
//  250     	2028            64.89 bits/value
//  500     	4059            64.94 bits/value
//  750     	6091            64.97 bits/value
//  1000        8122            64.97 bits/value
//  5000        40614           64.98 bits/value
//  10000       81223           64.97 bits/value
//  45000       365470          64.97 bits/value
//
fn float_encode_cpu(c: &mut Criterion) {
    let mut group = c.benchmark_group("float_encode_cpu");
    for batch_size in [
        10_i32, 25, 50, 100, 250, 500, 750, 1000, 5000, 10000, 45000,
    ]
    .iter()
    {
        group.throughput(Throughput::Bytes(*batch_size as u64 * 8));
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            batch_size,
            |b, &batch_size| {
                let decoded: Vec<f64> = fixtures::CPU_F64_EXAMPLE_VALUES[..batch_size as usize].to_vec();
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

criterion_group!(benches, float_encode_cpu);
criterion_main!(benches);
