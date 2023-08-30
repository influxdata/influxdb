use std::{collections::BTreeMap, iter, sync::Arc};

use criterion::{
    criterion_group, criterion_main, measurement::WallTime, BatchSize, BenchmarkGroup, Criterion,
    Throughput,
};
use data_types::{
    partition_template::{NamespacePartitionTemplateOverride, TablePartitionTemplateOverride},
    ColumnId, ColumnSchema, NamespaceId, NamespaceName, NamespaceSchema, TableId, TableSchema,
};
use iox_catalog::{interface::Catalog, mem::MemCatalog};
use once_cell::sync::Lazy;
use router::{
    gossip::anti_entropy::mst::{actor::AntiEntropyActor, merkle::MerkleTree},
    namespace_cache::{MemoryNamespaceCache, NamespaceCache, ReadThroughCache, ShardedCache},
};

static ARBITRARY_NAMESPACE: Lazy<NamespaceName<'static>> =
    Lazy::new(|| "bananas".try_into().unwrap());

fn init_ns_cache(
    rt: &tokio::runtime::Runtime,
    initial_schema: impl IntoIterator<Item = (NamespaceName<'static>, NamespaceSchema)>,
) -> impl NamespaceCache {
    let metrics = Arc::new(metric::Registry::default());

    let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metrics)));
    let cache = Arc::new(ShardedCache::new(
        iter::repeat_with(|| Arc::new(MemoryNamespaceCache::default())).take(10),
    ));

    let (actor, handle) = AntiEntropyActor::new(Arc::clone(&cache));
    rt.spawn(actor.run());

    let cache = MerkleTree::new(cache, handle);
    let cache = Arc::new(ReadThroughCache::new(cache, Arc::clone(&catalog)));

    for (name, schema) in initial_schema {
        cache.put_schema(name, schema);
    }

    cache
}

fn namespace_schema_cache_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("namespace_schema_cache_add_new_tables_with_columns");

    for i in [1, 10, 100] {
        bench_add_new_tables_with_columns(&mut group, i, 100);
    }

    group.finish();

    let mut group = c.benchmark_group("namespace_schema_cache_add_columns_to_existing_table");

    for i in [1, 10, 50] {
        bench_add_columns_to_existing_table(&mut group, i, 1);
        bench_add_columns_to_existing_table(&mut group, i, 10);
        bench_add_columns_to_existing_table(&mut group, i, 100);
    }

    group.finish();
}

fn bench_add_new_tables_with_columns(
    group: &mut BenchmarkGroup<WallTime>,
    tables: usize,
    columns_per_table: usize,
) {
    const INITIAL_TABLE_COUNT: usize = 1;
    let initial_schema = generate_namespace_schema(INITIAL_TABLE_COUNT, columns_per_table);
    let schema_update = generate_namespace_schema(INITIAL_TABLE_COUNT + tables, columns_per_table);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .build()
        .unwrap();

    group.throughput(Throughput::Elements((tables * columns_per_table) as _));
    group.bench_function(format!("{tables}x{columns_per_table}"), |b| {
        b.iter_batched(
            || {
                (
                    init_ns_cache(&rt, [(ARBITRARY_NAMESPACE.clone(), initial_schema.clone())]),
                    ARBITRARY_NAMESPACE.clone(),
                    schema_update.clone(),
                )
            },
            |(ns_cache, namespace_name, schema_update)| {
                ns_cache.put_schema(namespace_name, schema_update)
            },
            BatchSize::NumIterations(1),
        );
    });
}

fn bench_add_columns_to_existing_table(
    group: &mut BenchmarkGroup<WallTime>,
    initial_column_count: usize,
    add_new_columns: usize,
) {
    let initial_schema = generate_namespace_schema(1, initial_column_count);
    let schema_update = generate_namespace_schema(1, initial_column_count + add_new_columns);

    let rt = tokio::runtime::Runtime::new().unwrap();

    group.throughput(Throughput::Elements(add_new_columns as _));
    group.bench_function(format!("{initial_column_count}+{add_new_columns}"), |b| {
        b.iter_batched(
            || {
                (
                    init_ns_cache(&rt, [(ARBITRARY_NAMESPACE.clone(), initial_schema.clone())]),
                    ARBITRARY_NAMESPACE.clone(),
                    schema_update.clone(),
                )
            },
            |(ns_cache, namespace_name, schema_update)| {
                ns_cache.put_schema(namespace_name, schema_update)
            },
            BatchSize::NumIterations(1),
        );
    });
}

/// Deterministically generate an arbitrary [`NamespaceSchema`] with number of
/// `tables` each with a number of `columns_per_table`. An increasing number of
/// `tables` supersets the schema for a constant `columns_per_table` and vice
/// versa.
fn generate_namespace_schema(tables: usize, columns_per_table: usize) -> NamespaceSchema {
    let partition_template = NamespacePartitionTemplateOverride::default();
    NamespaceSchema {
        id: NamespaceId::new(42),
        tables: (0..tables)
            .map(|i| {
                let schema = TableSchema {
                    id: TableId::new(i as _),
                    columns: (0..columns_per_table)
                        .map(|j| {
                            (
                                format!("column{j}"),
                                ColumnSchema {
                                    id: ColumnId::new(j as _),
                                    column_type: data_types::ColumnType::U64,
                                },
                            )
                        })
                        .collect::<BTreeMap<_, _>>()
                        .into(),
                    partition_template: TablePartitionTemplateOverride::try_new(
                        None,
                        &partition_template,
                    )
                    .expect("should be able to use namespace partition template"),
                };
                (format!("table{i}"), schema)
            })
            .collect::<BTreeMap<_, _>>(),
        max_columns_per_table: usize::MAX,
        max_tables: usize::MAX,
        retention_period_ns: None,
        partition_template,
    }
}

criterion_group!(benches, namespace_schema_cache_benchmarks);
criterion_main!(benches);
