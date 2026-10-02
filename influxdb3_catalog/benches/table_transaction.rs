//! Benchmarks for per-write-request table transaction costs.
//!
//! `read_only` measures transaction setup over an existing table — the
//! steady-state write path, which shares the committed `TableDefinition`
//! via `Arc` and must not clone it. `first_mutation` adds one new column,
//! deliberately paying the copy-on-write clone every iteration so the
//! O(columns) clone cost of the schema-change path stays measured.

use std::hint::black_box;
use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use influxdb3_catalog::catalog::{Catalog, InfluxFieldType};
use iox_time::{MockProvider, Time, TimeProvider};
use object_store::{ObjectStore, memory::InMemory};

const COLUMN_COUNTS: [usize; 3] = [10, 100, 1000];

fn catalog_with_wide_table(rt: &tokio::runtime::Runtime, columns: usize) -> Arc<Catalog> {
    rt.block_on(async {
        let catalog = Catalog::new(
            "bench",
            Arc::new(InMemory::new()) as Arc<dyn ObjectStore>,
            Arc::new(MockProvider::new(Time::from_timestamp_nanos(0))) as Arc<dyn TimeProvider>,
            Default::default(),
        )
        .await
        .unwrap();
        catalog.create_database("db").await.unwrap();
        let mut txn = catalog.begin_database_transaction("db").unwrap();
        let tx = txn.table_tx_or_create("wide").unwrap();
        tx.tag_or_create("t0").unwrap();
        for i in 0..columns {
            tx.field_or_create(&format!("f{i}"), InfluxFieldType::Float)
                .unwrap();
        }
        catalog.commit(txn).await.unwrap().unwrap_success();
        catalog
    })
}

fn bench_table_transaction(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut group = c.benchmark_group("table_transaction");
    for columns in COLUMN_COUNTS {
        let catalog = catalog_with_wide_table(&rt, columns);
        group.bench_with_input(BenchmarkId::new("read_only", columns), &columns, |b, _| {
            b.iter(|| {
                let mut txn = catalog.begin_database_transaction("db").unwrap();
                let tx = txn.existing_table_tx("wide").unwrap();
                black_box(tx.num_columns())
            })
        });
        group.bench_with_input(
            BenchmarkId::new("first_mutation", columns),
            &columns,
            |b, _| {
                b.iter(|| {
                    let mut txn = catalog.begin_database_transaction("db").unwrap();
                    let tx = txn.table_tx_or_create("wide").unwrap();
                    black_box(tx.tag_or_create("t_new").unwrap().id)
                })
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_table_transaction);
criterion_main!(benches);
