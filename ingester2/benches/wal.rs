use std::{iter, sync::Arc};

use async_trait::async_trait;
use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use data_types::{NamespaceId, PartitionKey, TableId};
use dml::{DmlMeta, DmlOperation, DmlWrite};
use generated_types::influxdata::{
    iox::wal::v1::sequenced_wal_op::Op as WalOp, pbdata::v1::DatabaseBatch,
};
use ingester2::{
    buffer_tree::benches::PartitionData,
    dml_sink::{DmlError, DmlSink},
};
use mutable_batch_pb::encode::encode_write;
use wal::SequencedWalOp;

const NAMESPACE_ID: NamespaceId = NamespaceId::new(42);

/// A function to initialise the state of the WAL directory prior to executing
/// the replay code, returning the path to the initialised WAL directory.
async fn init() -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("failed to get temporary WAL directory");

    let wal = wal::Wal::new(dir.path())
        .await
        .expect("failed to initialise WAL to write");

    // Write a single line of LP to the WAL
    wal.write_op(SequencedWalOp {
        sequence_number: 42,
        op: WalOp::Write(lp_to_writes("bananas,tag1=A,tag2=B val=42i 1")),
    })
    .changed()
    .await
    .expect("flush fail");

    dir
}

fn wal_replay_bench(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to initialise tokio runtime for benchmark");

    let mut group = c.benchmark_group("replay");

    group.throughput(Throughput::Elements(1));
    group.bench_function("1 write", |b| {
        b.to_async(&runtime).iter_batched(
            // Call the init func to set up the WAL state.
            || futures::executor::block_on(init()),
            // Call the code to measure
            |dir| async move {
                // Initialise the WAL using the pre-populated path.
                let wal = wal::Wal::new(dir.path())
                    .await
                    .expect("failed to initialise WAL for replay");

                // Pass all writes into a NOP that discards them with no
                // overhead.
                let sink = NopSink::default();

                let persist = ingester2::persist::queue::benches::MockPersistQueue::default();

                // Replay the wal into the NOP.
                ingester2::benches::replay(&wal, &sink, Arc::new(persist))
                    .await
                    .expect("WAL replay error");
            },
            // Use the WAL for one test invocation only, and re-create a new one
            // for the next iteration.
            BatchSize::PerIteration,
        );
    });
}

/// Parse `lp` into a [`DatabaseBatch`] instance.
fn lp_to_writes(lp: &str) -> DatabaseBatch {
    let (writes, _) = mutable_batch_lp::lines_to_batches_stats(lp, 42)
        .expect("failed to build test writes from LP");

    // Add some made-up table IDs to the entries.
    let writes = writes
        .into_iter()
        .enumerate()
        .map(|(i, (_name, data))| (TableId::new(i as _), data))
        .collect();

    // Build the DmlWrite
    let op = DmlWrite::new(
        NAMESPACE_ID,
        writes,
        PartitionKey::from("ignored"),
        DmlMeta::unsequenced(None),
    );

    // And return it as a DatabaseBatch
    encode_write(NAMESPACE_ID.get(), &op)
}

/// A no-op [`DmlSink`] implementation.
#[derive(Debug, Default)]
struct NopSink;

#[async_trait]
impl DmlSink for NopSink {
    type Error = DmlError;
    async fn apply(&self, _op: DmlOperation) -> Result<(), DmlError> {
        // It does nothing!
        Ok(())
    }
}

impl ingester2::wal::benches::PartitionIter for NopSink {
    fn partition_iter(
        &self,
    ) -> Box<dyn Iterator<Item = std::sync::Arc<parking_lot::Mutex<PartitionData>>> + Send> {
        Box::new(iter::empty())
    }
}

criterion_group!(benches, wal_replay_bench);
criterion_main!(benches);
