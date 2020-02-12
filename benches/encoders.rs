use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rand::{distributions::Uniform, Rng};

mod fixtures;

const LARGER_BATCH_SIZES: [i32; 12] = [
    10, 25, 50, 100, 250, 500, 750, 1_000, 5_000, 10_000, 50_000, 100_000,
];

const SMALLER_BATCH_SIZES: [i32; 11] = [
    10, 25, 50, 100, 250, 500, 750, 1_000, 5_000, 10_000, 45_000,
];

fn benchmark_encode_sequential<T: From<i32>>(
    c: &mut Criterion,
    benchmark_group_name: &str,
    batch_sizes: &[i32],
    encode: fn(src: &[T], dst: &mut Vec<u8>) -> Result<(), Box<dyn std::error::Error>>
) {
    let mut group = c.benchmark_group(benchmark_group_name);

    for &batch_size in batch_sizes {
        group.throughput(Throughput::Bytes(batch_size as u64 * 8));
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            &batch_size,
            |b, &batch_size| {
                let decoded: Vec<_> = (1..batch_size).map(Into::into).collect();
                let mut encoded = vec![];
                b.iter(|| {
                    encoded.truncate(0);
                    encode(&decoded, &mut encoded).unwrap();
                });
            },
        );
    }
    group.finish();
}

fn benchmark_encode_random<T>(
    c: &mut Criterion,
    benchmark_group_name: &str,
    batch_sizes: &[i32],
    decoded_value_generation: fn(batch_size: i32) -> Vec<T>,
    encode: fn(src: &[T], dst: &mut Vec<u8>) -> Result<(), Box<dyn std::error::Error>>
) {
    let mut group = c.benchmark_group(benchmark_group_name);
    for &batch_size in batch_sizes {
        group.throughput(Throughput::Bytes(batch_size as u64 * 8));
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            &batch_size,
            |b, &batch_size| {
                let decoded = decoded_value_generation(batch_size);
                let mut encoded = vec![];
                b.iter(|| {
                    encoded.truncate(0);
                    encode(&decoded, &mut encoded).unwrap();
                });
            },
        );
    }
    group.finish();
}

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
    benchmark_encode_sequential(
        c,
        "float_encode_sequential",
        &LARGER_BATCH_SIZES,
        delorean::encoders::float::encode
    );
}

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
    benchmark_encode_sequential(
        c,
        "integer_encode_sequential",
        &LARGER_BATCH_SIZES,
        delorean::encoders::integer::encode
    );
}

fn timestamp_encode_sequential(c: &mut Criterion) {
    benchmark_encode_sequential(
        c,
        "timestamp_encode_sequential",
        &LARGER_BATCH_SIZES,
        delorean::encoders::timestamp::encode
    );
}

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
    benchmark_encode_random(
        c,
        "float_encode_random",
        &LARGER_BATCH_SIZES,
        |batch_size| {
            let range = Uniform::from(0.0..100.0);
            rand::thread_rng()
                .sample_iter(&range)
                .take(batch_size as usize)
                .collect()
        },
        delorean::encoders::float::encode
    )
}

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
    benchmark_encode_random(
        c,
        "integer_encode_random",
        &LARGER_BATCH_SIZES,
        |batch_size| {
            (1..batch_size)
                .map(|_| rand::thread_rng().gen_range(0, 100))
                .collect()
        },
        delorean::encoders::integer::encode
    )
}

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
    for &batch_size in &SMALLER_BATCH_SIZES {
        group.throughput(Throughput::Bytes(batch_size as u64 * 8));
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            &batch_size,
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

fn float_decode_cpu(c: &mut Criterion) {
    let mut group = c.benchmark_group("float_decode_cpu");
    for &batch_size in &SMALLER_BATCH_SIZES {
        let decoded: Vec<f64> = fixtures::CPU_F64_EXAMPLE_VALUES[..batch_size as usize].to_vec();
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

fn float_decode_sequential(c: &mut Criterion) {
    let mut group = c.benchmark_group("float_decode_sequential");
    for &batch_size in &LARGER_BATCH_SIZES {
        let decoded: Vec<f64> = (1..batch_size).map(f64::from).collect();
        let mut encoded = vec![];
        delorean::encoders::float::encode(&decoded, &mut encoded).unwrap();

        group.throughput(Throughput::Bytes(encoded.len() as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            &decoded,
            |b, decoded| {
                let mut decoded_mut = Vec::with_capacity(decoded.len());
                b.iter(|| {
                    decoded_mut.truncate(0);
                    delorean::encoders::float::decode(&encoded, &mut decoded_mut).unwrap();
                });
            },
        );
    }
    group.finish();
}

fn integer_decode_sequential(c: &mut Criterion) {
    let mut group = c.benchmark_group("integer_decode_sequential");
    for &batch_size in &LARGER_BATCH_SIZES {
        let decoded: Vec<i64> = (1..batch_size).map(i64::from).collect();
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

fn timestamp_decode_sequential(c: &mut Criterion) {
    let mut group = c.benchmark_group("timestamp_decode_sequential");
    for &batch_size in &LARGER_BATCH_SIZES {
        let decoded: Vec<i64> = (1..batch_size).map(i64::from).collect();
        let mut encoded = vec![];
        delorean::encoders::timestamp::encode(&decoded, &mut encoded).unwrap();

        group.throughput(Throughput::Bytes(encoded.len() as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            &decoded,
            |b, decoded| {
                let mut decoded_mut = Vec::with_capacity(decoded.len());
                b.iter(|| {
                    decoded_mut.truncate(0);
                    delorean::encoders::timestamp::decode(&encoded, &mut decoded_mut).unwrap();
                });
            },
        );
    }
    group.finish();
}

fn float_decode_random(c: &mut Criterion) {
    let mut group = c.benchmark_group("float_decode_random");
    for &batch_size in &LARGER_BATCH_SIZES {
        let range = Uniform::from(0.0..100.0);
        let decoded: Vec<_> = rand::thread_rng()
            .sample_iter(&range)
            .take(batch_size as usize)
            .collect();

        let mut encoded = vec![];
        delorean::encoders::float::encode(&decoded, &mut encoded).unwrap();

        group.throughput(Throughput::Bytes(encoded.len() as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            &decoded,
            |b, decoded| {
                let mut decoded_mut = Vec::with_capacity(decoded.len());
                b.iter(|| {
                    decoded_mut.truncate(0);
                    delorean::encoders::float::decode(&encoded, &mut decoded_mut).unwrap();
                });
            },
        );
    }
    group.finish();
}

fn integer_decode_random(c: &mut Criterion) {
    let mut group = c.benchmark_group("integer_decode_random");
    for &batch_size in &LARGER_BATCH_SIZES {
        let decoded: Vec<i64> = (1..batch_size)
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

criterion_group!(benches,
    float_encode_sequential, integer_encode_sequential, timestamp_encode_sequential,
    float_encode_random, integer_encode_random,
    float_encode_cpu, float_decode_cpu,
    float_decode_sequential, integer_decode_sequential, timestamp_decode_sequential,
    float_decode_random, integer_decode_random,
);

criterion_main!(benches);
