use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

// The current float encoder produces the following compression:
//
//  values  block size  compression
//  10      33          26.4 bits/value
//  25      52          16.64 bits/value
//  50      78          12.48 bits/value
//  100     129         10.32 bits/value
//  250     290          9.28 bits/value
//  500     584          9.34 bits/value
//  750     878          9.36 bits/value
//  1000    1221         9.76 bits/value
//  5000    7013        11.22 bits/value
//  10000   15145       12.11 bits/value
//  50000   90090       14.41 bits/value
//  100000  192481      15.39 bits/value
//
fn float_encode_sequential(c: &mut Criterion) {
    let mut group = c.benchmark_group("float_encode_sequential");
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
                let src: Vec<f64> = (1..batch_size).map(f64::from).collect();
                let mut dst = vec![];
                b.iter(|| {
                    dst.truncate(0);
                    delorean::encoders::float::encode_all(&src, &mut dst).unwrap();
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, float_encode_sequential);
criterion_main!(benches);
