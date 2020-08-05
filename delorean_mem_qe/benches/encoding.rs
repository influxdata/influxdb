use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

const BATCH_SIZES: [usize; 5] = [10, 100, 1_000, 10_000, 100_000];
const CARDINALITIES: [usize; 4] = [1, 5, 10, 100];

fn encoding_drle_row_ids_sorted(c: &mut Criterion) {
    benchmark_row_ids(
        c,
        "encoding_drle_row_ids_sorted",
        &BATCH_SIZES,
        &CARDINALITIES,
    );
}

fn benchmark_row_ids(
    c: &mut Criterion,
    benchmark_group_name: &str,
    batch_sizes: &[usize],
    cardinalities: &[usize],
) {
    let mut group = c.benchmark_group(benchmark_group_name);
    for &batch_size in batch_sizes {
        for &cardinality in cardinalities {
            let mut input = delorean_mem_qe::encoding::DictionaryRLE::new();
            let values = batch_size / cardinality;
            for i in 0..cardinality {
                input.push_additional(i.to_string().as_str(), values as u64);
            }
            group.throughput(Throughput::Bytes(batch_size as u64));

            group.bench_with_input(
                BenchmarkId::from_parameter(format!("{:?}_{:?}", batch_size, cardinality)),
                &input,
                |b, input| {
                    b.iter(|| {
                        // do work
                        for i in 0..cardinality {
                            let ids = input
                                .row_ids(i.to_string().as_str())
                                .collect::<Vec<usize>>();
                        }
                    });
                },
            );
        }
    }
    group.finish();
}

fn encoding_drle_row_ids_sorted_roaring(c: &mut Criterion) {
    benchmark_row_ids_roaring(
        c,
        "encoding_drle_row_ids_sorted_roaring",
        &BATCH_SIZES,
        &CARDINALITIES,
    );
}

fn benchmark_row_ids_roaring(
    c: &mut Criterion,
    benchmark_group_name: &str,
    batch_sizes: &[usize],
    cardinalities: &[usize],
) {
    let mut group = c.benchmark_group(benchmark_group_name);
    for &batch_size in batch_sizes {
        for &cardinality in cardinalities {
            let mut input = delorean_mem_qe::encoding::DictionaryRLE::new();
            let values = batch_size / cardinality;
            for i in 0..cardinality {
                input.push_additional(i.to_string().as_str(), values as u64);
            }
            group.throughput(Throughput::Bytes(batch_size as u64));

            group.bench_with_input(
                BenchmarkId::from_parameter(format!("{:?}_{:?}", batch_size, cardinality)),
                &input,
                |b, input| {
                    b.iter(|| {
                        // do work
                        for i in 0..cardinality {
                            let ids = input.row_ids_roaring(i.to_string().as_str());
                        }
                    });
                },
            );
        }
    }
    group.finish();
}

criterion_group!(
    benches,
    encoding_drle_row_ids_sorted,
    encoding_drle_row_ids_sorted_roaring
);
criterion_main!(benches);
