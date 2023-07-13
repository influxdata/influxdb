use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use data_types::{NamespaceId, PartitionKey, TableId};
use ingester::IngesterRpcInterface;
use ingester_query_grpc::influxdata::iox::ingester::v1::IngesterQueryRequest;
use ingester_test_ctx::{TestContext, TestContextBuilder};
use std::{fmt::Write, sync::Arc, time::Instant};
use tokio::sync::Barrier;

const TEST_NAMESPACE: &str = "bananas";
const PARTITION_KEY: &str = "platanos";

fn generate_table_data(rows: usize, cols: usize) -> String {
    let mut buf = String::new();
    for i in 0..rows {
        write!(&mut buf, "bananas ").unwrap();
        for j in 0..(cols - 1) {
            write!(&mut buf, "v{j}={i}{j},").unwrap();
        }
        writeln!(&mut buf, "v{cols}={i}{cols} 42").unwrap(); // One timestamp -> one partition
    }

    buf
}

/// Return an initialised and pre-warmed ingester instance backed by a catalog
/// correctly populated to accept writes of `lp`.
async fn init(
    lp: impl AsRef<str>,
) -> (TestContext<impl IngesterRpcInterface>, NamespaceId, TableId) {
    let lp = lp.as_ref();

    let mut ctx = TestContextBuilder::default()
        // Don't stop ingest during benchmarks
        .with_max_persist_queue_depth(10_000_000)
        .with_persist_hot_partition_cost(10_000_000_000)
        .build()
        .await;

    // Ensure the namespace exists in the catalog.
    let ns = ctx.ensure_namespace(TEST_NAMESPACE, None).await;

    // Write the test data
    ctx.write_lp(TEST_NAMESPACE, lp, PartitionKey::from(PARTITION_KEY), 42)
        .await;

    let table_id = ctx.table_id(TEST_NAMESPACE, "bananas").await;

    (ctx, ns.id, table_id)
}

fn bench_query(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to initialise tokio runtime for benchmark");

    for (rows, cols) in [(100_000, 10), (100_000, 100), (100_000, 200)] {
        run_projection_bench("no projection", rows, cols, vec![], &runtime, c);
        run_projection_bench(
            "project 1 column",
            rows,
            cols,
            vec!["time".to_string()],
            &runtime,
            c,
        );
    }
}

fn run_projection_bench(
    name: &str,
    rows: usize,
    cols: usize,
    projection: Vec<String>,
    runtime: &tokio::runtime::Runtime,
    c: &mut Criterion,
) {
    let lp = generate_table_data(rows, cols);
    let (ctx, namespace_id, table_id) = runtime.block_on(init(lp));

    let mut group = c.benchmark_group("projection");
    group.throughput(Throughput::Elements(1)); // Queries per second
    group.bench_function(
        BenchmarkId::new(name, format!("rows_{rows}_cols{cols}")),
        |b| {
            let ctx = &ctx;
            let projection = &projection;
            b.to_async(runtime).iter(|| async move {
                ctx.query(IngesterQueryRequest {
                    namespace_id: namespace_id.get(),
                    table_id: table_id.get(),
                    columns: projection.clone(),
                    predicate: None,
                })
                .await
                .expect("query request failed");
            });
        },
    );
}

/// Number of queries to send per reader, per iteration.
const CONCURRENT_QUERY_BATCH_SIZE: usize = 20;

// Benchmark scalability of the read path as more readers are added when
// querying partitions with varying amounts of data.
//
// The ingester "process" is running in the same threadpool as the benchmark
// loop, so this isn't super clean.
fn bench_query_concurrent(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to initialise tokio runtime for benchmark");

    for readers in [1, 10, 100] {
        for buf_size_lines in [1000, 50_000] {
            run_concurrent_bench(readers, buf_size_lines, &runtime, c);
        }
    }

    run_concurrent_bench(1, 100_000, &runtime, c);
    run_concurrent_bench(10, 100_000, &runtime, c);
}

async fn do_queries(ctx: &TestContext<impl IngesterRpcInterface>, query: &IngesterQueryRequest) {
    for _ in 0..CONCURRENT_QUERY_BATCH_SIZE {
        ctx.query(query.clone())
            .await
            .expect("query request failed");
    }
}

fn run_concurrent_bench(
    concurrent_readers: usize,
    buf_size_lines: usize,
    runtime: &tokio::runtime::Runtime,
    c: &mut Criterion,
) {
    const COLUMN_COUNT: usize = 10;

    let lp = generate_table_data(buf_size_lines, COLUMN_COUNT);
    let (ctx, namespace_id, table_id) = runtime.block_on(init(lp));

    let query = Arc::new(IngesterQueryRequest {
        namespace_id: namespace_id.get(),
        table_id: table_id.get(),
        columns: vec![],
        predicate: None,
    });

    let ctx = Arc::new(ctx);

    let mut group = c.benchmark_group("concurrent_query");
    group.throughput(Throughput::Elements(CONCURRENT_QUERY_BATCH_SIZE as _)); // Queries per second
    group.bench_function(
        format!("readers_{concurrent_readers}/buffered_{buf_size_lines}x{COLUMN_COUNT}"),
        |b| {
            b.to_async(runtime).iter_custom(|iters| {
                let query = Arc::clone(&query);
                let ctx = Arc::clone(&ctx);
                async move {
                    // Sync point to ensure all readers start at approximately the same
                    // time.
                    let barrier = Arc::new(Barrier::new(concurrent_readers));

                    // Spawn N-1 readers that'll be adding the concurrent workload, but
                    // not measured.
                    for _ in 0..(concurrent_readers - 1) {
                        let barrier = Arc::clone(&barrier);
                        let query = Arc::clone(&query);
                        let ctx = Arc::clone(&ctx);
                        tokio::spawn(async move {
                            barrier.wait().await;
                            for _ in 0..iters {
                                do_queries(&ctx, &query).await;
                            }
                        });
                    }

                    // And measure the last reader.
                    barrier.wait().await;
                    let start = Instant::now();
                    for _ in 0..iters {
                        do_queries(&ctx, &query).await;
                    }
                    start.elapsed()
                }
            });
        },
    );
}

criterion_group!(benches, bench_query, bench_query_concurrent);
criterion_main!(benches);
