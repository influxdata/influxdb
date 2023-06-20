use async_trait::async_trait;
use data_types::TableId;
use generated_types::influxdata::iox::wal::v1::sequenced_wal_op::Op;
use mutable_batch_pb::encode::encode_write;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::watch::Receiver;
use wal::{SequencedWalOp, WriteResult};

use crate::{
    cancellation_safe::CancellationSafe,
    dml_payload::IngestOp,
    dml_sink::{DmlError, DmlSink},
};

use super::traits::WalAppender;

/// [`DELEGATE_APPLY_TIMEOUT`] defines how long the inner [`DmlSink`] is given
/// to complete the write [`DmlSink::apply()`] call.
///
/// If this limit weren't enforced, a write that does not make progress would
/// consume resources forever. Instead, a reasonable duration of time is given
/// to attempt the write before an error is returned to the caller.
///
/// In practice, this limit SHOULD only ever be reached as a symptom of a larger
/// problem (catalog unavailable, etc) preventing a write from making progress.
const DELEGATE_APPLY_TIMEOUT: Duration = Duration::from_secs(15);

/// A [`DmlSink`] decorator that ensures any [`DmlOperation`] is committed to
/// the write-ahead log before passing the operation to the inner [`DmlSink`].
#[derive(Debug)]
pub(crate) struct WalSink<T, W = wal::Wal> {
    /// The inner chain of [`DmlSink`] that a [`DmlOperation`] is passed to once
    /// committed to the write-ahead log.
    inner: T,

    /// The write-ahead log implementation.
    wal: W,
}

impl<T, W> WalSink<T, W> {
    /// Initialise a new [`WalSink`] that appends [`DmlOperation`] to `W` and
    /// on success, passes the op through to `T`.
    pub(crate) fn new(inner: T, wal: W) -> Self {
        Self { inner, wal }
    }
}

#[async_trait]
impl<T, W> DmlSink for WalSink<T, W>
where
    T: DmlSink + Clone + 'static,
    W: WalAppender + 'static,
{
    type Error = DmlError;

    async fn apply(&self, op: IngestOp) -> Result<(), Self::Error> {
        // Append the operation to the WAL
        let mut write_result = self.wal.append(&op);

        // Pass it to the inner handler while we wait for the write to be made
        // durable.
        //
        // Ensure that this future is always driven to completion now that the
        // WAL entry is being committed, otherwise they'll diverge. At the same
        // time, do not allow the spawned task to live forever, consuming
        // resources without making progress - instead shed load after a
        // reasonable duration of time (DELEGATE_APPLY_TIMEOUT) has passed,
        // before returning a write error (if the caller is still listening).
        //
        // If this buffer apply fails, the entry remains in the WAL and will be
        // attempted again during WAL replay after a crash. If this can never
        // succeed, this can cause a crash loop (unlikely) - see:
        //
        //  https://github.com/influxdata/influxdb_iox/issues/7111
        //
        let inner = self.inner.clone();
        CancellationSafe::new(async move {
            let res = tokio::time::timeout(DELEGATE_APPLY_TIMEOUT, inner.apply(op))
                .await
                .map_err(|_| DmlError::ApplyTimeout)?;

            res.map_err(Into::into)
        })
        .await?;

        // Wait for the write to be durable before returning to the user
        write_result
            .changed()
            .await
            .expect("unable to get WAL write result");

        let res = write_result.borrow();
        match res.as_ref().expect("WAL should always return result") {
            WriteResult::Ok(_) => Ok(()),
            WriteResult::Err(ref e) => Err(DmlError::Wal(e.to_string())),
        }
    }
}

impl WalAppender for Arc<wal::Wal> {
    fn append(&self, op: &IngestOp) -> Receiver<Option<WriteResult>> {
        let namespace_id = op.namespace();

        let (wal_op, partition_sequence_numbers) = match op {
            IngestOp::Write(w) => {
                let partition_sequence_numbers = w
                    .tables()
                    .map(|(table_id, data)| {
                        (
                            *table_id,
                            data.partitioned_data().sequence_number().get() as u64,
                        )
                    })
                    .collect::<HashMap<TableId, u64>>();
                let dml_write = w.into();
                (
                    Op::Write(encode_write(namespace_id.get(), &dml_write)),
                    partition_sequence_numbers,
                )
            }
        };

        self.write_op(SequencedWalOp {
            sequence_number: *partition_sequence_numbers
                .values()
                .next()
                .expect("tried to append unsequenced WAL operation"),
            table_write_sequence_numbers: partition_sequence_numbers,
            op: wal_op,
        })
    }
}

#[cfg(test)]
mod tests {
    use core::{future::Future, marker::Send, pin::Pin};
    use std::{future, sync::Arc};

    use assert_matches::assert_matches;
    use data_types::{NamespaceId, PartitionKey, SequenceNumber, TableId};
    use generated_types::influxdata::pbdata::v1::DatabaseBatch;
    use mutable_batch_lp::lines_to_batches;
    use mutable_batch_pb::encode::encode_batch;
    use wal::Wal;

    use crate::{
        dml_payload::{PartitionedData, TableData, WriteOperation},
        dml_sink::mock_sink::MockDmlSink,
        test_util::make_write_op,
    };

    use super::*;

    const TABLE_ID: TableId = TableId::new(44);
    const TABLE_NAME: &str = "bananas";
    const NAMESPACE_NAME: &str = "platanos";
    const NAMESPACE_ID: NamespaceId = NamespaceId::new(42);

    #[tokio::test]
    async fn test_append() {
        let dir = tempfile::tempdir().unwrap();

        const SECOND_TABLE_ID: TableId = TableId::new(45);
        const SECOND_TABLE_NAME: &str = "banani";
        // Generate a test op containing writes for multiple tables that will
        // be appended and read back
        let mut tables_by_name = lines_to_batches(
            "bananas,region=Madrid temp=35 4242424242\n\
             banani,region=Iceland temp=25 7676767676",
            0,
        )
        .expect("invalid line proto");
        let op = WriteOperation::new(
            NAMESPACE_ID,
            [
                (
                    TABLE_ID,
                    TableData::new(
                        TABLE_ID,
                        PartitionedData::new(
                            SequenceNumber::new(42),
                            tables_by_name
                                .remove(TABLE_NAME)
                                .expect("table does not exist in LP"),
                        ),
                    ),
                ),
                (
                    SECOND_TABLE_ID,
                    TableData::new(
                        SECOND_TABLE_ID,
                        PartitionedData::new(
                            SequenceNumber::new(42),
                            tables_by_name
                                .remove(SECOND_TABLE_NAME)
                                .expect("second table does not exist in LP"),
                        ),
                    ),
                ),
            ]
            .into_iter()
            .collect(),
            PartitionKey::from("p1"),
            None,
        );

        // The write portion of this test.
        {
            let inner = Arc::new(MockDmlSink::default().with_apply_return(vec![Ok(())]));
            let wal = Wal::new(dir.path())
                .await
                .expect("failed to initialise WAL");

            let wal_sink = WalSink::new(Arc::clone(&inner), wal);

            // Apply the op through the decorator
            wal_sink
                .apply(IngestOp::Write(op.clone()))
                .await
                .expect("wal should not error");

            // Assert the mock inner sink saw the call
            assert_eq!(inner.get_calls().len(), 1);
        }

        // Read the op back
        let wal = Wal::new(dir.path())
            .await
            .expect("failed to initialise WAL");

        // Identify the segment file
        let files = wal.closed_segments();
        let file = assert_matches!(&*files, [f] => f, "expected 1 file");

        // Open a reader
        let ops: Vec<SequencedWalOp> = wal
            .reader_for_segment(file.id())
            .expect("failed to obtain reader for WAL segment")
            .flat_map(|batch| batch.expect("failed to read WAL op batch"))
            .collect();

        // Extract the op payload read from the WAL
        let read_op = assert_matches!(&*ops, [op] => op, "expected 1 DML operation");
        assert_eq!(read_op.sequence_number, 42);
        assert_eq!(
            read_op.table_write_sequence_numbers,
            [(TABLE_ID, 42), (SECOND_TABLE_ID, 42)]
                .into_iter()
                .collect::<std::collections::HashMap<TableId, u64>>()
        );
        let payload =
            assert_matches!(&read_op.op, Op::Write(w) => w, "expected DML write WAL entry");

        // The payload should match the serialised form of the "op" originally
        // wrote above.
        let want = encode_write_op(NAMESPACE_ID, &op);

        assert_eq!(want, *payload);
    }

    /// A [`DmlSink`] implementation that hangs forever and never completes.
    #[derive(Debug, Default, Clone)]
    struct BlockingDmlSink;

    impl DmlSink for BlockingDmlSink {
        type Error = DmlError;

        fn apply<'life0, 'async_trait>(
            &'life0 self,
            _op: IngestOp,
        ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'async_trait>>
        where
            'life0: 'async_trait,
            Self: 'async_trait,
        {
            Box::pin(future::pending())
        }
    }

    #[tokio::test]
    async fn test_timeout() {
        let dir = tempfile::tempdir().unwrap();

        // Generate the test op
        let op = make_write_op(
            &PartitionKey::from("p1"),
            NAMESPACE_ID,
            TABLE_NAME,
            TABLE_ID,
            42,
            r#"bananas,region=Madrid temp=35 4242424242"#,
        );

        let wal = Wal::new(dir.path())
            .await
            .expect("failed to initialise WAL");

        let wal_sink = WalSink::new(BlockingDmlSink::default(), wal);

        // Allow tokio to automatically advance time past the timeout duration,
        // when all threads are blocked on await points.
        //
        // This allows the test to drive the timeout logic without actually
        // waiting for the timeout duration in the test.
        tokio::time::pause();

        let start = tokio::time::Instant::now();

        // Apply the op through the decorator, which should time out
        let err = wal_sink
            .apply(IngestOp::Write(op.clone()))
            .await
            .expect_err("write should time out");

        assert_matches!(err, DmlError::ApplyTimeout);

        // Ensure that "time" advanced at least the timeout amount of time
        // before erroring.
        let duration = tokio::time::Instant::now().duration_since(start);
        assert!(duration > DELEGATE_APPLY_TIMEOUT);
    }

    // TODO(savage): Move to a better location
    fn encode_write_op(namespace_id: NamespaceId, op: &WriteOperation) -> DatabaseBatch {
        DatabaseBatch {
            database_id: namespace_id.get(),
            partition_key: op.partition_key().to_string(),
            table_batches: op
                .tables()
                .map(|(table_id, batch)| {
                    encode_batch(table_id.get(), batch.partitioned_data().data())
                })
                .collect(),
        }
    }
}
