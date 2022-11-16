use std::{
    sync::{Arc, Barrier},
    time::Instant,
};

use criterion::{
    criterion_group, criterion_main, measurement::WallTime, BenchmarkGroup, Criterion, Throughput,
};
use data_types::NamespaceName;
use mutable_batch::MutableBatch;
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use sharder::{JumpHash, RoundRobin, Sharder};

fn get_random_string(length: usize) -> String {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(length)
        .map(char::from)
        .collect()
}

fn sharder_benchmarks(c: &mut Criterion) {
    benchmark_impl(c, "jumphash", |num_buckets| {
        JumpHash::new((0..num_buckets).map(Arc::new))
    });

    benchmark_impl(c, "round_robin", |num_buckets| {
        RoundRobin::new((0..num_buckets).map(Arc::new))
    });
}

fn benchmark_impl<T, F>(c: &mut Criterion, name: &str, init: F)
where
    T: Sharder<MutableBatch>,
    F: Fn(usize) -> T,
{
    let mut group = c.benchmark_group(name);

    // benchmark sharder with fixed table name and namespace, with varying number of buckets
    benchmark_scenario(
        &mut group,
        "basic 1k buckets",
        "table",
        &NamespaceName::try_from("namespace").unwrap(),
        init(1_000),
    );
    benchmark_scenario(
        &mut group,
        "basic 10k buckets",
        "table",
        &NamespaceName::try_from("namespace").unwrap(),
        init(10_000),
    );
    benchmark_scenario(
        &mut group,
        "basic 100k buckets",
        "table",
        &NamespaceName::try_from("namespace").unwrap(),
        init(100_000),
    );
    benchmark_scenario(
        &mut group,
        "basic 1M buckets",
        "table",
        &NamespaceName::try_from("namespace").unwrap(),
        init(1_000_000),
    );

    // benchmark sharder with random table name and namespace of length 16
    benchmark_scenario(
        &mut group,
        "random with key-length 16",
        get_random_string(16).as_str(),
        &NamespaceName::try_from(get_random_string(16)).unwrap(),
        init(10_000),
    );

    // benchmark sharder with random table name and namespace of length 32
    benchmark_scenario(
        &mut group,
        "random with key-length 32",
        get_random_string(32).as_str(),
        &NamespaceName::try_from(get_random_string(32)).unwrap(),
        init(10_000),
    );

    // benchmark sharder with random table name and namespace of length 64
    benchmark_scenario(
        &mut group,
        "random with key-length 64",
        get_random_string(64).as_str(),
        &NamespaceName::try_from(get_random_string(64)).unwrap(),
        init(10_000),
    );

    group.finish();
}

fn benchmark_scenario<T>(
    group: &mut BenchmarkGroup<WallTime>,
    bench_name: &str,
    table: &str,
    namespace: &NamespaceName<'_>,
    sharder: T,
) where
    T: Sharder<MutableBatch>,
{
    let batch = MutableBatch::default();

    group.throughput(Throughput::Elements(1));
    group.bench_function(bench_name, |b| {
        b.iter(|| {
            sharder.shard(table, namespace, &batch);
        });
    });

    const N_THREADS: usize = 10;

    // Run the same test with N contending threads.
    //
    // Note that this includes going through pointer indirection for each shard
    // op due to the Arc.
    let sharder = Arc::new(sharder);
    group.bench_function(format!("{bench_name}_{N_THREADS}_threads"), |b| {
        b.iter_custom(|iters| {
            let sharder = Arc::clone(&sharder);
            std::thread::scope(|s| {
                let barrier = Arc::new(Barrier::new(N_THREADS));
                // Spawn N-1 threads that wait for the last thread to spawn
                for _ in 0..(N_THREADS - 1) {
                    let sharder = Arc::clone(&sharder);
                    let barrier = Arc::clone(&barrier);
                    s.spawn(move || {
                        let batch = MutableBatch::default();
                        barrier.wait();
                        for _ in 0..iters {
                            sharder.shard(table, namespace, &batch);
                        }
                    });
                }
                // Spawn the Nth thread that performs the same sharding ops, but
                // measures the duration of time taken.
                s.spawn(move || {
                    let batch = MutableBatch::default();
                    barrier.wait();
                    let start = Instant::now();
                    for _ in 0..iters {
                        sharder.shard(table, namespace, &batch);
                    }
                    start.elapsed()
                })
                .join()
                .unwrap()
            })
        });
    });
}

criterion_group!(benches, sharder_benchmarks);
criterion_main!(benches);
