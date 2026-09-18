//! Write-buffer table buffer: insert cost and the per-query cost of producing
//! batches, across overwrite mixes. The query-path number is what a point
//! lookup against buffered data pays before DataFusion sees a row, so it is
//! the one to watch when changing how superseded rows are handled.
//!
//! Scenarios (100k unique points, 3 fields incl. a string, one tag):
//! - `none`:    no overwrites (append-only tables; the fast path)
//! - `average`: 10% of points overwritten once
//! - `heavy`:   every point overwritten once (50% of rows superseded)
//! - `worst`:   every point overwritten three times (75% superseded)

use std::hint::black_box;
use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use influxdb3_catalog::catalog::{Catalog, TableDefinition};
use influxdb3_types::DatabaseName;
use influxdb3_wal::Row;
use influxdb3_write::write_buffer::table_buffer::TableBuffer;
use influxdb3_write::write_buffer::validator::WriteValidator;
use influxdb3_write::{ChunkFilter, Precision};
use iox_time::{MockProvider, Time};
use object_store::memory::InMemory;

const POINTS: usize = 100_000;
const DB: &str = "bench-db";

struct Workload {
    name: &'static str,
    /// Batches of rows in ingest order: the first holds the unique points,
    /// each later one rewrites a slice of them.
    batches: Vec<Vec<Row>>,
    table_def: Arc<TableDefinition>,
}

fn lp(points: impl Iterator<Item = usize>, version: usize) -> String {
    let mut s = String::with_capacity(POINTS * 64);
    for i in points {
        // Same tag set and timestamp per point; field values change per version.
        s.push_str(&format!(
            "tbl,sensor=s-{i} f={}.5,i={}i,s=\"v{version}-{i}\" 1000\n",
            i % 1000,
            i + version
        ));
    }
    s
}

async fn rows(catalog: &Arc<Catalog>, lp: &str, ingest_sec: i64) -> Vec<Row> {
    let db = DatabaseName::try_from(DB).unwrap();
    WriteValidator::initialize(db, Arc::clone(catalog))
        .unwrap()
        .v1_parse_lines_and_catalog_updates(
            lp,
            false,
            Time::from_timestamp_nanos(ingest_sec * 1_000_000_000),
            Precision::Nanosecond,
        )
        .unwrap()
        .commit_catalog_changes()
        .await
        .map(|r| r.unwrap_success())
        .unwrap()
        .into_inner()
        .to_rows()
}

async fn workload(name: &'static str, overwrite_fraction: f64, passes: usize) -> Workload {
    let catalog = Catalog::new(
        "bench-node",
        Arc::new(InMemory::new()),
        Arc::new(MockProvider::new(Time::from_timestamp_nanos(0))),
        Default::default(),
    )
    .await
    .unwrap();
    let mut batches = vec![rows(&catalog, &lp(0..POINTS, 0), 0).await];
    let overwritten = (POINTS as f64 * overwrite_fraction) as usize;
    for pass in 1..=passes {
        batches.push(rows(&catalog, &lp(0..overwritten, pass), pass as i64).await);
    }
    let table_def = catalog
        .db_schema(DB)
        .unwrap()
        .table_definition("tbl")
        .unwrap();
    Workload {
        name,
        batches,
        table_def,
    }
}

fn buffer(w: &Workload) -> TableBuffer {
    let mut tb = TableBuffer::new();
    for b in &w.batches {
        tb.buffer_chunk(0, b);
    }
    tb
}

fn bench(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let workloads = rt.block_on(async {
        vec![
            workload("none", 0.0, 0).await,
            workload("average", 0.1, 1).await,
            workload("heavy", 1.0, 1).await,
            workload("worst", 1.0, 3).await,
        ]
    });

    // Insert: the whole workload buffered from scratch.
    let mut g = c.benchmark_group("table_buffer/insert");
    for w in &workloads {
        let rows: usize = w.batches.iter().map(Vec::len).sum();
        g.throughput(Throughput::Elements(rows as u64));
        g.bench_with_input(BenchmarkId::from_parameter(w.name), w, |b, w| {
            b.iter(|| black_box(buffer(w)))
        });
    }
    g.finish();

    // Query path: producing the batches a query scans.
    let mut g = c.benchmark_group("table_buffer/record_batches");
    for w in &workloads {
        let tb = buffer(w);
        let filter = ChunkFilter::new(&w.table_def, &[]).unwrap();
        g.throughput(Throughput::Elements(POINTS as u64));
        g.bench_with_input(BenchmarkId::from_parameter(w.name), &tb, |b, tb| {
            b.iter(|| {
                black_box(
                    tb.partitioned_record_batches(Arc::clone(&w.table_def), &filter)
                        .unwrap(),
                )
            })
        });
    }
    g.finish();

    // Persist path: the snapshot batch of the chunk.
    let mut g = c.benchmark_group("table_buffer/snapshot");
    for w in &workloads {
        g.throughput(Throughput::Elements(POINTS as u64));
        g.bench_with_input(BenchmarkId::from_parameter(w.name), w, |b, w| {
            b.iter_batched(
                || buffer(w),
                |mut tb| black_box(tb.snapshot(Arc::clone(&w.table_def), i64::MAX)),
                criterion::BatchSize::LargeInput,
            )
        });
    }
    g.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
