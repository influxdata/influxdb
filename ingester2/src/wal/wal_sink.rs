use async_trait::async_trait;
use dml::DmlOperation;
use generated_types::influxdata::iox::wal::v1::sequenced_wal_op::Op;
use mutable_batch_pb::encode::encode_write;
use std::sync::Arc;
use tokio::sync::watch::Receiver;
use wal::{SequencedWalOp, WriteResult};

use crate::dml_sink::{DmlError, DmlSink};

use super::traits::WalAppender;

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
    T: DmlSink,
    W: WalAppender + 'static,
{
    type Error = DmlError;

    async fn apply(&self, op: DmlOperation) -> Result<(), Self::Error> {
        // TODO: cancellation safety
        //
        // See https://github.com/influxdata/influxdb_iox/issues/6281.
        //
        // Once an item is in the WAL, it should be passed into the inner
        // DmlSink so that is becomes readable - failing to do this means writes
        // will randomly appear after replaying the WAL.
        //
        // This can happen If the caller stops polling just after the WAL commit
        // future completes and before the inner DmlSink call returns Ready.

        // Append the operation to the WAL
        let mut write_result = self.wal.append(&op);

        // Pass it to the inner handler while we wait for the write to be made durable
        self.inner.apply(op).await.map_err(Into::into)?;

        // wait for the write to be durable before returning
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
    fn append(&self, op: &DmlOperation) -> Receiver<Option<WriteResult>> {
        let sequence_number = op
            .meta()
            .sequence()
            .expect("committing unsequenced dml operation to wal")
            .sequence_number
            .get() as u64;

        let namespace_id = op.namespace_id();

        let wal_op = match op {
            DmlOperation::Write(w) => Op::Write(encode_write(namespace_id.get(), w)),
            DmlOperation::Delete(_) => unreachable!(),
        };

        self.write_op(SequencedWalOp {
            sequence_number,
            op: wal_op,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert_matches::assert_matches;
    use data_types::{NamespaceId, PartitionKey, TableId};
    use wal::Wal;

    use crate::{dml_sink::mock_sink::MockDmlSink, test_util::make_write_op};

    use super::*;

    const TABLE_ID: TableId = TableId::new(44);
    const TABLE_NAME: &str = "bananas";
    const NAMESPACE_NAME: &str = "platanos";
    const NAMESPACE_ID: NamespaceId = NamespaceId::new(42);

    #[tokio::test]
    async fn test_append() {
        let dir = tempfile::tempdir().unwrap();

        // Generate the test op that will be appended and read back
        let op = make_write_op(
            &PartitionKey::from("p1"),
            NAMESPACE_ID,
            TABLE_NAME,
            TABLE_ID,
            42,
            r#"bananas,region=Madrid temp=35 4242424242"#,
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
                .apply(DmlOperation::Write(op.clone()))
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
        let mut reader = wal
            .reader_for_segment(file.id())
            .expect("failed to obtain reader");

        // Obtain all the ops in the file
        let mut ops = Vec::new();
        while let Ok(Some(mut batch)) = reader.next_batch() {
            ops.append(&mut batch);
        }

        // Extract the op payload read from the WAL
        let read_op = assert_matches!(&*ops, [op] => op, "expected 1 DML operation");
        assert_eq!(read_op.sequence_number, 42);
        let payload =
            assert_matches!(&read_op.op, Op::Write(w) => w, "expected DML write WAL entry");

        // The payload should match the serialised form of the "op" originally
        // wrote above.
        let want = encode_write(NAMESPACE_ID.get(), &op);

        assert_eq!(want, *payload);
    }
}
