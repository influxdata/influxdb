use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rand::Rng;

// The current integer encoder produces the following compression:
//
// values   block size  compression
// 10	    25	        20.00 bits/value
// 25	    33	        10.56 bits/value
// 50	    65	        10.40 bits/value
// 100	    121	         9.68 bits/value
// 250	    281	         8.99 bits/value
// 500	    561	         8.97 bits/value
// 750	    833	         8.88 bits/value
// 1000	    1105	     8.84 bits/value
// 5000	    5425	     8.68 bits/value
// 10000	10865	     8.69 bits/value
// 50000	54361	     8.69 bits/value
// 100000	108569	     8.68 bits/value
//
fn integer_encode_random(c: &mut Criterion) {
    let mut group = c.benchmark_group("integer_encode_random");
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
                let decoded: Vec<i64> = (1..batch_size)
                    .map(|_| rand::thread_rng().gen_range(0, 100))
                    .collect();
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

criterion_group!(benches, integer_encode_random);
criterion_main!(benches);
