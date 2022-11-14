use std::sync::Arc;

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

fn sharder_benchmarks(mut c: &mut Criterion) {
    benchmark_impl(&mut c, "jumphash", |num_buckets| {
        JumpHash::new((0..num_buckets).map(Arc::new))
    });

    benchmark_impl(&mut c, "round_robin", |num_buckets| {
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
}

criterion_group!(benches, sharder_benchmarks);
criterion_main!(benches);
