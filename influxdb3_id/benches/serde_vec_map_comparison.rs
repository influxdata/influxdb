use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use indexmap::IndexMap;
use influxdb3_id::{SerdeVecMap, TableId};
use rustc_hash::FxHasher;
use std::hash::BuildHasherDefault;

type FxBuildHasher = BuildHasherDefault<FxHasher>;

// Helper to create FxHash-based SerdeVecMap (our optimized version)
fn create_fx_serde_map(size: usize) -> SerdeVecMap<TableId, String> {
    let mut map = IndexMap::with_capacity_and_hasher(size, FxBuildHasher::default());
    for i in 0..size as u32 {
        map.insert(TableId::from(i), format!("table_{}", i));
    }
    SerdeVecMap::from(map)
}

// Helper to create default hasher IndexMap (for comparison)
fn create_default_index_map(size: usize) -> IndexMap<TableId, String> {
    let mut map = IndexMap::with_capacity(size);
    for i in 0..size as u32 {
        map.insert(TableId::from(i), format!("table_{}", i));
    }
    map
}

fn benchmark_lookups(c: &mut Criterion) {
    let mut group = c.benchmark_group("lookups");

    for size in [10, 100, 1000, 10000].iter() {
        // Benchmark FxHash SerdeVecMap
        let fx_map = create_fx_serde_map(*size);
        group.bench_with_input(
            BenchmarkId::new("serde_vec_map_fxhash", size),
            size,
            |b, &size| {
                b.iter(|| {
                    let mut sum = 0u32;
                    for i in 0..size as u32 {
                        if let Some(val) = fx_map.get(&TableId::from(i)) {
                            sum += val.len() as u32;
                        }
                    }
                    black_box(sum);
                })
            },
        );

        // Benchmark default IndexMap for comparison
        let default_map = create_default_index_map(*size);
        group.bench_with_input(
            BenchmarkId::new("index_map_default", size),
            size,
            |b, &size| {
                b.iter(|| {
                    let mut sum = 0u32;
                    for i in 0..size as u32 {
                        if let Some(val) = default_map.get(&TableId::from(i)) {
                            sum += val.len() as u32;
                        }
                    }
                    black_box(sum);
                })
            },
        );
    }

    group.finish();
}

fn benchmark_inserts(c: &mut Criterion) {
    let mut group = c.benchmark_group("inserts");

    for size in [10, 100, 1000, 10000].iter() {
        // Benchmark FxHash SerdeVecMap inserts
        group.bench_with_input(
            BenchmarkId::new("serde_vec_map_fxhash", size),
            size,
            |b, &size| {
                b.iter_with_setup(
                    || SerdeVecMap::<TableId, String>::with_capacity(size),
                    |mut map| {
                        for i in 0..size as u32 {
                            map.insert(TableId::from(i), format!("table_{}", i));
                        }
                        black_box(map);
                    },
                )
            },
        );

        // Benchmark default IndexMap inserts
        group.bench_with_input(
            BenchmarkId::new("index_map_default", size),
            size,
            |b, &size| {
                b.iter_with_setup(
                    || IndexMap::<TableId, String>::with_capacity(size),
                    |mut map| {
                        for i in 0..size as u32 {
                            map.insert(TableId::from(i), format!("table_{}", i));
                        }
                        black_box(map);
                    },
                )
            },
        );
    }

    group.finish();
}

fn benchmark_iteration(c: &mut Criterion) {
    let mut group = c.benchmark_group("iteration");

    for size in [10, 100, 1000, 10000].iter() {
        // Benchmark FxHash SerdeVecMap iteration
        let fx_map = create_fx_serde_map(*size);
        group.bench_with_input(
            BenchmarkId::new("serde_vec_map_fxhash", size),
            size,
            |b, _| {
                b.iter(|| {
                    let mut sum = 0usize;
                    for (_id, val) in fx_map.iter() {
                        sum += val.len();
                    }
                    black_box(sum);
                })
            },
        );

        // Benchmark default IndexMap iteration
        let default_map = create_default_index_map(*size);
        group.bench_with_input(BenchmarkId::new("index_map_default", size), size, |b, _| {
            b.iter(|| {
                let mut sum = 0usize;
                for (_id, val) in default_map.iter() {
                    sum += val.len();
                }
                black_box(sum);
            })
        });
    }

    group.finish();
}

fn benchmark_mixed_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_workload");

    for size in [100, 1000].iter() {
        // Benchmark FxHash SerdeVecMap mixed operations
        group.bench_with_input(
            BenchmarkId::new("serde_vec_map_fxhash", size),
            size,
            |b, &size| {
                b.iter_with_setup(
                    || create_fx_serde_map(size / 2),
                    |mut map| {
                        // 70% lookups, 20% inserts, 10% removes
                        for i in 0..100 {
                            let op = i % 10;
                            let key = TableId::from((i * 7) % size as u32);

                            if op < 7 {
                                // Lookup
                                black_box(map.get(&key));
                            } else if op < 9 {
                                // Insert
                                map.insert(key, format!("new_table_{}", i));
                            } else {
                                // Remove
                                map.shift_remove(&key);
                            }
                        }
                        black_box(map);
                    },
                )
            },
        );
    }

    group.finish();
}

fn benchmark_serialization(c: &mut Criterion) {
    let mut group = c.benchmark_group("serialization");

    for size in [10, 100, 1000].iter() {
        let fx_map = create_fx_serde_map(*size);

        // Benchmark serialization
        group.bench_with_input(BenchmarkId::new("serialize", size), size, |b, _| {
            b.iter(|| {
                let json = serde_json::to_string(&fx_map).unwrap();
                black_box(json);
            })
        });

        // Benchmark deserialization
        let json = serde_json::to_string(&fx_map).unwrap();
        group.bench_with_input(BenchmarkId::new("deserialize", size), size, |b, _| {
            b.iter(|| {
                let map: SerdeVecMap<TableId, String> = serde_json::from_str(&json).unwrap();
                black_box(map);
            })
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    benchmark_lookups,
    benchmark_inserts,
    benchmark_iteration,
    benchmark_mixed_workload,
    benchmark_serialization
);
criterion_main!(benches);
