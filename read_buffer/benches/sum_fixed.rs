use std::mem::size_of;

use arrow::array::PrimitiveArray;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rand::prelude::*;

use arrow::datatypes::*;

use read_buffer::benchmarks::{ByteTrimmer, Fixed, FixedNull, NoOpTranscoder, ScalarEncoding};

const ROWS: [usize; 5] = [10, 100, 1_000, 10_000, 60_000];
const CHUNKS: [Chunks; 4] = [
    Chunks::All,
    Chunks::Even,
    Chunks::ManySmall,
    Chunks::RandomTenPercent,
];

const PHYSICAL_TYPES: [PhysicalType; 3] = [PhysicalType::I64, PhysicalType::I32, PhysicalType::I16];

#[derive(Debug)]
enum Chunks {
    All,              // sum up the entire column
    Even,             // sum up the even rows
    ManySmall,        // sum up chunks of 10 values
    RandomTenPercent, // sum up random 10% of values
}

enum EncType {
    Fixed,
    FixedNull,
}

enum PhysicalType {
    I64,
    I32,
    I16,
}

fn encoding_sum(c: &mut Criterion) {
    benchmark_none_sum(
        c,
        "encoding_none_sum",
        EncType::Fixed,
        &ROWS,
        &CHUNKS,
        &PHYSICAL_TYPES,
    );
    benchmark_none_sum(
        c,
        "encoding_none_sum",
        EncType::FixedNull,
        &ROWS,
        &CHUNKS,
        &PHYSICAL_TYPES,
    );
}

fn benchmark_none_sum(
    c: &mut Criterion,
    benchmark_group_name: &str,
    enc_type: EncType,
    row_size: &[usize],
    chunks: &[Chunks],
    physical_type: &[PhysicalType],
) {
    let mut group = c.benchmark_group(benchmark_group_name);
    for &num_rows in row_size {
        for chunk in chunks {
            for pt in physical_type {
                // Encoded incrementing values.

                let input: Vec<u32>;
                match chunk {
                    Chunks::All => input = (0..num_rows as u32).collect(),
                    Chunks::Even => input = gen_even_chunk(num_rows),
                    Chunks::ManySmall => input = gen_many_small_chunk(num_rows),
                    Chunks::RandomTenPercent => input = gen_random_10_percent(num_rows),
                }

                match pt {
                    PhysicalType::I64 => {
                        group
                            .throughput(Throughput::Bytes((input.len() * size_of::<i64>()) as u64));

                        match enc_type {
                            EncType::Fixed => {
                                let encoding = Fixed::<i64, i64, _>::new(
                                    (0..num_rows as i64).collect::<Vec<i64>>(),
                                    NoOpTranscoder {},
                                );

                                group.bench_with_input(
                                    BenchmarkId::from_parameter(format!(
                                        "{:?}_{:?}_i64",
                                        num_rows, chunk
                                    )),
                                    &input,
                                    |b, input| {
                                        b.iter(|| {
                                            // do work
                                            let _ = encoding.sum(&input);
                                        });
                                    },
                                );
                            }
                            EncType::FixedNull => {
                                let encoding = FixedNull::<Int64Type, i64, _>::new(
                                    PrimitiveArray::from(
                                        (0..num_rows as i64).collect::<Vec<i64>>(),
                                    ),
                                    ByteTrimmer {},
                                );

                                group.bench_with_input(
                                    BenchmarkId::from_parameter(format!(
                                        "{:?}_{:?}_i64",
                                        num_rows, chunk
                                    )),
                                    &input,
                                    |b, input| {
                                        b.iter(|| {
                                            // do work
                                            let _ = encoding.sum(&input);
                                        });
                                    },
                                );
                            }
                        }
                    }
                    PhysicalType::I32 => {
                        group
                            .throughput(Throughput::Bytes((input.len() * size_of::<i64>()) as u64));

                        match enc_type {
                            EncType::Fixed => {
                                let encoding = Fixed::<i32, i64, _>::new(
                                    (0..num_rows as i32).collect::<Vec<i32>>(),
                                    ByteTrimmer {},
                                );

                                group.bench_with_input(
                                    BenchmarkId::from_parameter(format!(
                                        "{:?}_{:?}_i32",
                                        num_rows, chunk
                                    )),
                                    &input,
                                    |b, input| {
                                        b.iter(|| {
                                            // do work
                                            let _ = encoding.sum(&input);
                                        });
                                    },
                                );
                            }
                            EncType::FixedNull => {
                                let encoding = FixedNull::<Int32Type, i64, _>::new(
                                    PrimitiveArray::from(
                                        (0..num_rows as i32).collect::<Vec<i32>>(),
                                    ),
                                    ByteTrimmer {},
                                );

                                group.bench_with_input(
                                    BenchmarkId::from_parameter(format!(
                                        "{:?}_{:?}_i32",
                                        num_rows, chunk
                                    )),
                                    &input,
                                    |b, input| {
                                        b.iter(|| {
                                            // do work
                                            let _ = encoding.sum(&input);
                                        });
                                    },
                                );
                            }
                        }
                    }
                    PhysicalType::I16 => {
                        group
                            .throughput(Throughput::Bytes((input.len() * size_of::<i64>()) as u64));

                        match enc_type {
                            EncType::Fixed => {
                                let encoding = Fixed::<i16, i64, _>::new(
                                    (0..num_rows as i16).collect::<Vec<i16>>(),
                                    ByteTrimmer {},
                                );

                                group.bench_with_input(
                                    BenchmarkId::from_parameter(format!(
                                        "{:?}_{:?}_i16",
                                        num_rows, chunk
                                    )),
                                    &input,
                                    |b, input| {
                                        b.iter(|| {
                                            // do work
                                            let _ = encoding.sum(&input);
                                        });
                                    },
                                );
                            }
                            EncType::FixedNull => {
                                let encoding = FixedNull::<Int16Type, i64, _>::new(
                                    PrimitiveArray::from(
                                        (0..num_rows as i16).collect::<Vec<i16>>(),
                                    ),
                                    ByteTrimmer {},
                                );

                                group.bench_with_input(
                                    BenchmarkId::from_parameter(format!(
                                        "{:?}_{:?}_i16",
                                        num_rows, chunk
                                    )),
                                    &input,
                                    |b, input| {
                                        b.iter(|| {
                                            // do work
                                            let _ = encoding.sum(&input);
                                        });
                                    },
                                );
                            }
                        }
                    }
                }
            }
        }
    }
    group.finish();
}

// results in about 50% rows being requested.
fn gen_even_chunk(rows: usize) -> Vec<u32> {
    (0..rows as u32).filter(|x| x % 2 == 0).collect()
}

// generate small sequences of 3 rows periodically. This leads to about 34% of
// rows being requested.
fn gen_many_small_chunk(rows: usize) -> Vec<u32> {
    let mut input = vec![];
    let mut emit_chunk = false;
    let mut chunk_size = 0;

    for i in 0..rows as u32 {
        if i % 9 == 0 {
            emit_chunk = true;
        }

        if emit_chunk {
            input.push(i);
            chunk_size += 1;
        }

        if chunk_size == 3 {
            chunk_size = 0;
            emit_chunk = false;
        }
    }

    input
}

// generate random 10% sequence.
fn gen_random_10_percent(rows: usize) -> Vec<u32> {
    let mut rnd = thread_rng();
    let mut input = vec![];

    for i in 0..rows as u32 {
        if rnd.gen::<f64>() < 0.1 {
            input.push(i);
        }
    }

    input
}

criterion_group!(benches, encoding_sum,);
criterion_main!(benches);
