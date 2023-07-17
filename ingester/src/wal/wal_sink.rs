use async_trait::async_trait;
use data_types::{sequence_number_set::SequenceNumberSet, TableId};
use generated_types::influxdata::iox::wal::v1::sequenced_wal_op::Op;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::watch::Receiver;
use wal::{SequencedWalOp, WriteResult};

use crate::{
    cancellation_safe::CancellationSafe,
    dml_payload::{encode::encode_write_op, IngestOp},
    dml_sink::{DmlError, DmlSink},
};

use super::{
    reference_tracker::WalReferenceHandle,
    traits::{UnbufferedWriteNotifier, WalAppender},
};

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

/// A [`DmlSink`] decorator that ensures any [`IngestOp`] is committed to
/// the write-ahead log before passing the operation to the inner [`DmlSink`].
#[derive(Debug)]
pub(crate) struct WalSink<T, W = wal::Wal, N = WalReferenceHandle> {
    /// The inner chain of [`DmlSink`] that a [`IngestOp`] is passed to once
    /// committed to the write-ahead log.
    inner: T,

    /// The write-ahead log implementation.
    wal: W,

    /// A notifier handle to report the sequence numbers of writes that enter
    /// the write-ahead log but fail to buffer.
    notifier_handle: N,
}

impl<T, W, N> WalSink<T, W, N> {
    /// Initialise a new [`WalSink`] that appends [`IngestOp`] to `W` and
    /// on success, passes the op through to `T`, using `N` to keep `W` informed
    /// of writes that fail to buffer.
    pub(crate) fn new(inner: T, wal: W, notifier_handle: N) -> Self {
        Self {
            inner,
            wal,
            notifier_handle,
        }
    }
}

#[async_trait]
impl<T, W, N> DmlSink for WalSink<T, W, N>
where
    T: DmlSink + Clone + 'static,
    W: WalAppender + 'static,
    N: UnbufferedWriteNotifier + 'static,
{
    type Error = DmlError;

    async fn apply(&self, op: IngestOp) -> Result<(), Self::Error> {
        // Append the operation to the WAL
        let mut write_result = self.wal.append(&op);

        let set = op.sequence_number_set();

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
        let inner_result = CancellationSafe::new(async move {
            let res = tokio::time::timeout(DELEGATE_APPLY_TIMEOUT, inner.apply(op))
                .await
                .map_err(|_| DmlError::ApplyTimeout)?;

            res.map_err(Into::into)
        })
        .await;
        if inner_result.is_err() {
            self.notifier_handle.notify_failed_write_buffer(set).await;
        }
        inner_result?;

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
                        (*table_id, data.partitioned_data().sequence_number().get())
                    })
                    .collect::<HashMap<TableId, u64>>();
                (
                    Op::Write(encode_write_op(namespace_id, w)),
                    partition_sequence_numbers,
                )
            }
        };

        self.write_op(SequencedWalOp {
            table_write_sequence_numbers: partition_sequence_numbers,
            op: wal_op,
        })
    }
}

#[async_trait]
impl UnbufferedWriteNotifier for WalReferenceHandle {
    async fn notify_failed_write_buffer(&self, set: SequenceNumberSet) {
        self.enqueue_unbuffered_write(set).await;
    }
}

#[cfg(test)]
pub(crate) mod mock {
    use parking_lot::Mutex;

    use super::*;

    #[derive(Debug, Default)]
    pub(crate) struct MockUnbufferedWriteNotifier {
        calls: Mutex<Vec<SequenceNumberSet>>,
    }

    impl MockUnbufferedWriteNotifier {
        pub(crate) fn calls(&self) -> Vec<SequenceNumberSet> {
            self.calls.lock().clone()
        }
    }

    #[async_trait]
    impl UnbufferedWriteNotifier for Arc<MockUnbufferedWriteNotifier> {
        async fn notify_failed_write_buffer(&self, set: SequenceNumberSet) {
            self.calls.lock().push(set);
        }
    }
}

#[cfg(test)]
mod tests {
    use core::{future::Future, marker::Send, pin::Pin};
    use std::{future, sync::Arc};

    use assert_matches::assert_matches;
    use data_types::{SequenceNumber, TableId};
    use lazy_static::lazy_static;
    use wal::Wal;

    use super::*;
    use crate::{
        dml_sink::mock_sink::MockDmlSink,
        test_util::{
            make_multi_table_write_op, ARBITRARY_NAMESPACE_ID, ARBITRARY_PARTITION_KEY,
            ARBITRARY_TABLE_ID, ARBITRARY_TABLE_NAME,
        },
    };

    lazy_static! {
        static ref ALTERNATIVE_TABLE_NAME: &'static str = "arán";
        static ref ALTERNATIVE_TABLE_ID: TableId = TableId::new(ARBITRARY_TABLE_ID.get() + 1);
    }

    #[tokio::test]
    async fn test_append() {
        let dir = tempfile::tempdir().unwrap();

        // Generate a test op containing writes for multiple tables that will
        // be appended and read back
        let op = make_multi_table_write_op(
            &ARBITRARY_PARTITION_KEY,
            ARBITRARY_NAMESPACE_ID,
            [
                (
                    ARBITRARY_TABLE_NAME.to_string().as_str(),
                    ARBITRARY_TABLE_ID,
                    SequenceNumber::new(42),
                ),
                (
                    &ALTERNATIVE_TABLE_NAME,
                    *ALTERNATIVE_TABLE_ID,
                    SequenceNumber::new(43),
                ),
            ]
            .into_iter(),
            &format!(
                r#"{},region=Madrid temp=35,climate="dry" 4242424242
                {},region=Belfast temp=14,climate="wet" 4242424242"#,
                &*ARBITRARY_TABLE_NAME, &*ALTERNATIVE_TABLE_NAME,
            ),
        );

        // The write portion of this test.
        {
            let inner = Arc::new(MockDmlSink::default().with_apply_return(vec![Ok(())]));
            let wal = Wal::new(dir.path())
                .await
                .expect("failed to initialise WAL");
            let notifier_handle = Arc::new(mock::MockUnbufferedWriteNotifier::default());

            let wal_sink = WalSink::new(Arc::clone(&inner), wal, Arc::clone(&notifier_handle));

            // Apply the op through the decorator
            wal_sink
                .apply(IngestOp::Write(op.clone()))
                .await
                .expect("wal should not error");

            // Assert the mock inner sink saw the call and that no unbuffered
            // write notification was sent
            assert_eq!(inner.get_calls().len(), 1);
            assert_eq!(notifier_handle.calls().len(), 0);
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
        assert_eq!(
            read_op.table_write_sequence_numbers,
            [(ARBITRARY_TABLE_ID, 42), (*ALTERNATIVE_TABLE_ID, 43)]
                .into_iter()
                .collect::<std::collections::HashMap<TableId, u64>>()
        );
        let payload =
            assert_matches!(&read_op.op, Op::Write(w) => w, "expected DML write WAL entry");

        // The payload should match the serialised form of the "op" originally
        // wrote above.
        let want = encode_write_op(ARBITRARY_NAMESPACE_ID, &op);

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
        let op = make_multi_table_write_op(
            &ARBITRARY_PARTITION_KEY,
            ARBITRARY_NAMESPACE_ID,
            [
                (
                    ARBITRARY_TABLE_NAME.to_string().as_str(),
                    ARBITRARY_TABLE_ID,
                    SequenceNumber::new(42),
                ),
                (
                    &ALTERNATIVE_TABLE_NAME,
                    *ALTERNATIVE_TABLE_ID,
                    SequenceNumber::new(43),
                ),
            ]
            .into_iter(),
            &format!(
                r#"{},region=Madrid temp=35,climate="dry" 4242424242
                {},region=Belfast temp=14,climate="wet" 4242424242"#,
                &*ARBITRARY_TABLE_NAME, &*ALTERNATIVE_TABLE_NAME,
            ),
        );

        let wal = Wal::new(dir.path())
            .await
            .expect("failed to initialise WAL");
        let notifier_handle = Arc::new(mock::MockUnbufferedWriteNotifier::default());

        let wal_sink = WalSink::new(BlockingDmlSink, wal, Arc::clone(&notifier_handle));

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

        // Assert that an unbuffered write notification was sent for the
        // correct [`SequenceNumberSet`]
        assert_eq!(
            notifier_handle.calls(),
            [SequenceNumberSet::from_iter([
                SequenceNumber::new(42),
                SequenceNumber::new(43)
            ])]
            .into_iter()
            .collect::<Vec<_>>()
        );
    }
}
