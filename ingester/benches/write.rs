use std::sync::Arc;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use data_types::{PartitionKey, SequenceNumber};
use futures::{stream::FuturesUnordered, StreamExt};
use generated_types::influxdata::{
    iox::ingester::v1::write_service_server::WriteService, pbdata::v1::DatabaseBatch,
};
use influxdb_iox_client::ingester::generated_types::WriteRequest;
use ingester::internal_implementation_details::{
    encode::encode_write_op,
    write::{
        PartitionedData as PayloadPartitionedData, TableData as PayloadTableData, WriteOperation,
    },
};
use ingester::IngesterRpcInterface;
use ingester_test_ctx::{TestContext, TestContextBuilder};
use mutable_batch_lp::lines_to_batches;

const TEST_NAMESPACE: &str = "bananas";
const PARTITION_KEY: &str = "platanos";

/// Return an initialised and pre-warmed ingester instance backed by a catalog
/// correctly populated to accept writes of `lp`.
async fn init(lp: impl AsRef<str>) -> (TestContext<impl IngesterRpcInterface>, DatabaseBatch) {
    let lp = lp.as_ref();

    let mut ctx = TestContextBuilder::default()
        // Don't stop ingest during benchmarks
        .with_max_persist_queue_depth(10_000_000)
        .with_persist_hot_partition_cost(10_000_000_000)
        .build()
        .await;

    // Ensure the namespace exists in the catalog.
    let ns = ctx.ensure_namespace(TEST_NAMESPACE, None, None).await;

    // Perform a write to drive table / schema population in the catalog.
    ctx.write_lp(
        TEST_NAMESPACE,
        lp,
        PartitionKey::from(PARTITION_KEY),
        42,
        None,
    )
    .await;

    // Construct the write request once, and reuse it for each iteration.
    let batches = lines_to_batches(lp, 0).unwrap();

    // Build the TableId -> Batch map, resolving the tables IDs from the catalog
    // in the process.
    let batches_by_ids = batches
        .into_iter()
        .map(|(table_name, batch)| {
            let catalog = Arc::clone(&ctx.catalog());
            async move {
                let id = catalog
                    .repositories()
                    .await
                    .tables()
                    .get_by_namespace_and_name(ns.id, table_name.as_str())
                    .await
                    .unwrap()
                    .expect("table should exist because of the write_lp above")
                    .id;

                (
                    id,
                    PayloadTableData::new(
                        id,
                        PayloadPartitionedData::new(SequenceNumber::new(42), batch),
                    ),
                )
            }
        })
        .collect::<FuturesUnordered<_>>()
        .collect::<hashbrown::HashMap<_, _>>()
        .await;

    let op = WriteOperation::new(
        ns.id,
        batches_by_ids,
        PartitionKey::from(PARTITION_KEY),
        None,
    );

    (ctx, encode_write_op(ns.id, &op))
}

/// Benchmark writes containing varying volumes of line protocol.
///
/// This is definitely a more "macro" benchmark than micro, as it covers the
/// entire ingester write process (RPC request handler, RPC message
/// deserialisation, WAL commit, buffering write, RPC response, etc) but does
/// not include transport overhead (measuring only the processing time, not
/// including the network read time).
///
/// Note that this benchmark covers the single threaded / uncontended case - as
/// the number of parallel writes increases, so does the lock contention on the
/// underlying buffer tree.
fn bench_write(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to initialise tokio runtime for benchmark");

    {
        let (ctx, op) = runtime.block_on(init("bananas greatness=\"unbounded\" 42"));
        let rpc = ctx.rpc();

        let mut group = c.benchmark_group("single row");
        group.throughput(Throughput::Elements(1));
        group.bench_function("write", |b| {
            b.to_async(&runtime).iter(|| {
                let op = op.clone();
                async move {
                    rpc.write_service()
                        .write(tonic::Request::new(WriteRequest { payload: Some(op) }))
                        .await
                        .unwrap();
                }
            });
        });
    }

    {
        let lp = std::fs::read_to_string("../test_fixtures/lineproto/metrics.lp").unwrap();
        let line_count = lp.lines().count() as u64;

        let (ctx, op) = runtime.block_on(init(lp));
        let rpc = ctx.rpc();

        let mut group = c.benchmark_group("batched");
        group.throughput(Throughput::Elements(line_count));
        group.bench_function(BenchmarkId::new("write", line_count), |b| {
            b.to_async(&runtime).iter(|| {
                let op = op.clone();
                async move {
                    rpc.write_service()
                        .write(tonic::Request::new(WriteRequest { payload: Some(op) }))
                        .await
                        .unwrap();
                }
            });
        });
    }
}

criterion_group!(benches, bench_write);
criterion_main!(benches);
