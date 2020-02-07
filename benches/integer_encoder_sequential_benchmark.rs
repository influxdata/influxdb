use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

// The current integer encoder produces the following compression. Note, since
// a sequential range of values can be encoded using RLE the compression
// statistics are not very interesting.
//
// values   block size  compression
// 10	    11	        8.80 bits/value
// 25	    11	        3.52 bits/value
// 50	    11	        1.76 bits/value
// 100	    11	        0.88 bits/value
// 250	    12	        0.38 bits/value
// 500	    12	        0.19 bits/value
// 750	    12	        0.12 bits/value
// 1000	    12	        0.09 bits/value
// 5000	    12	        0.01 bits/value
// 10000	12	        0.00 bits/value
// 50000	13	        0.00 bits/value
// 100000	13	        0.00 bits/value
//
fn integer_encode_sequential(c: &mut Criterion) {
    let mut group = c.benchmark_group("integer_encode_sequential");
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
                let decoded: Vec<i64> = (1..batch_size).map(i64::from).collect();
                let mut encoded = vec![];
                b.iter(|| {
                    encoded.truncate(0);
                    delorean::encoders::integer::encode(&decoded, &mut encoded).unwrap();
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, integer_encode_sequential);
criterion_main!(benches);
