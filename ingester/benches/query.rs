use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use data_types::{NamespaceId, PartitionKey, TableId};
use ingester::IngesterRpcInterface;
use ingester_query_grpc::influxdata::iox::ingester::v1::IngesterQueryRequest;
use ingester_test_ctx::{TestContext, TestContextBuilder};
use std::fmt::Write;

const TEST_NAMESPACE: &str = "bananas";
const PARTITION_KEY: &str = "platanos";

fn generate_table_data(rows: usize, cols: usize) -> String {
    let mut buf = String::new();
    for i in 0..rows {
        write!(&mut buf, "bananas ").unwrap();
        for j in 0..(cols - 1) {
            write!(&mut buf, "v{j}={i}{j},").unwrap();
        }
        writeln!(&mut buf, "v{cols}={i}{cols} 42{i}").unwrap();
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
        run_bench("no projection", rows, cols, vec![], &runtime, c);
        run_bench(
            "project 1 column",
            rows,
            cols,
            vec!["time".to_string()],
            &runtime,
            c,
        );
    }
}

fn run_bench(
    name: &str,
    rows: usize,
    cols: usize,
    projection: Vec<String>,
    runtime: &tokio::runtime::Runtime,
    c: &mut Criterion,
) {
    let lp = generate_table_data(rows, cols);
    let (ctx, namespace_id, table_id) = runtime.block_on(init(lp));

    let mut group = c.benchmark_group("query");
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

criterion_group!(benches, bench_query);
criterion_main!(benches);
