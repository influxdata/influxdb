use std::{iter, sync::Arc};

use criterion::{
    criterion_group, criterion_main, measurement::WallTime, BatchSize, BenchmarkGroup, Criterion,
    Throughput,
};
use data_types::{NamespaceId, NamespaceName, NamespaceSchema};
use hashbrown::HashMap;
use iox_catalog::{interface::Catalog, mem::MemCatalog};
use mutable_batch::MutableBatch;
use once_cell::sync::Lazy;
use router::{
    dml_handlers::{DmlHandler, SchemaValidator},
    namespace_cache::{MemoryNamespaceCache, NamespaceCache, ReadThroughCache, ShardedCache},
};
use schema::Projection;
use tokio::runtime::Runtime;

static NAMESPACE: Lazy<NamespaceName<'static>> = Lazy::new(|| "bananas".try_into().unwrap());

fn runtime() -> Runtime {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
}

fn schema_validator_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("schema_validator");

    bench(&mut group, 1, 1);

    bench(&mut group, 1, 100);
    bench(&mut group, 1, 10000);

    bench(&mut group, 100, 1);
    bench(&mut group, 10000, 1);

    group.finish();
}

fn bench(group: &mut BenchmarkGroup<WallTime>, tables: usize, columns_per_table: usize) {
    let metrics = Arc::new(metric::Registry::default());

    let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metrics)));
    let ns_cache = Arc::new(ReadThroughCache::new(
        ShardedCache::new(iter::repeat_with(MemoryNamespaceCache::default).take(10)),
        Arc::clone(&catalog),
    ));
    let validator = SchemaValidator::new(catalog, Arc::clone(&ns_cache), &metrics);

    let namespace_schema = NamespaceSchema {
        id: NamespaceId::new(42),
        tables: Default::default(),
        max_columns_per_table: 42,
        max_tables: 42,
        retention_period_ns: None,
        partition_template: Default::default(),
    };
    ns_cache.put_schema(NAMESPACE.clone(), namespace_schema);

    for i in 0..65_000 {
        let write = lp_to_writes(format!("{}{}", i + 10_000_000, generate_lp(1, 1)).as_str());
        let namespace_schema = runtime().block_on(ns_cache.get_schema(&NAMESPACE)).unwrap();
        let _ = runtime().block_on(validator.write(&NAMESPACE, namespace_schema, write, None));
    }

    let write = lp_to_writes(&generate_lp(tables, columns_per_table));
    let column_count = write
        .values()
        .fold(0, |acc, b| acc + b.schema(Projection::All).unwrap().len());

    group.throughput(Throughput::Elements(column_count as _));
    group.bench_function(format!("{tables}x{columns_per_table}"), |b| {
        b.to_async(runtime()).iter_batched(
            || {
                (
                    write.clone(),
                    futures::executor::block_on(ns_cache.get_schema(&NAMESPACE)).unwrap(),
                )
            },
            |(write, namespace_schema)| validator.write(&NAMESPACE, namespace_schema, write, None),
            BatchSize::SmallInput,
        );
    });
}

fn generate_lp(tables: usize, columns_per_table: usize) -> String {
    (0..tables)
        .map(|i| {
            let cols = (0..columns_per_table)
                .map(|i| format!("val{i}=42i"))
                .collect::<Vec<_>>()
                .join(",");

            format!("table{i},tag=A {cols}")
        })
        .collect::<Vec<_>>()
        .join("\n")
}

// Parse `lp` into a table-keyed MutableBatch map.
fn lp_to_writes(lp: &str) -> HashMap<String, MutableBatch> {
    let (writes, _) = mutable_batch_lp::lines_to_batches_stats(lp, 42)
        .expect("failed to build test writes from LP");
    writes
}

criterion_group!(benches, schema_validator_benchmarks);
criterion_main!(benches);
