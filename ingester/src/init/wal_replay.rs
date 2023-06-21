use data_types::{NamespaceId, PartitionKey, SequenceNumber, TableId};
use generated_types::influxdata::iox::wal::v1::sequenced_wal_op::Op;
use metric::U64Counter;
use mutable_batch_pb::decode::decode_database_batch;
use observability_deps::tracing::*;
use std::time::Instant;
use thiserror::Error;
use wal::{SequencedWalOp, Wal};

use crate::{
    dml_payload::{IngestOp, PartitionedData, TableData, WriteOperation},
    dml_sink::{DmlError, DmlSink},
    partition_iter::PartitionIter,
    persist::{drain_buffer::persist_partitions, queue::PersistQueue},
};

/// Errors returned when replaying the write-ahead log.
#[derive(Debug, Error)]
pub enum WalReplayError {
    /// An error initialising a segment file reader.
    #[error("failed to open wal segment for replay: {0}")]
    OpenSegment(wal::Error),

    /// An error when attempting to read an entry from the WAL.
    #[error("failed to read wal entry: {0}")]
    ReadEntry(wal::Error),

    /// An error converting the WAL entry into a [`IngestOp`].
    #[error("failed converting wal entry to ingest operation: {0}")]
    MapToDml(#[from] mutable_batch_pb::decode::Error),

    /// A failure to apply a [`IngestOp`] from the WAL to the in-memory
    /// [`BufferTree`].
    ///
    /// [`BufferTree`]: crate::buffer_tree::BufferTree
    #[error("failed to apply op: {0}")]
    Apply(#[from] DmlError),
}

// TODO: tolerate WAL replay errors
//
// https://github.com/influxdata/influxdb_iox/issues/6283

/// Replay all the entries in `wal` to `sink`, returning the maximum observed
/// [`SequenceNumber`].
pub async fn replay<T, P>(
    wal: &Wal,
    sink: &T,
    persist: P,
    metrics: &metric::Registry,
) -> Result<Option<SequenceNumber>, WalReplayError>
where
    T: DmlSink + PartitionIter,
    P: PersistQueue + Clone,
{
    // Read the set of files to replay.
    //
    // The WAL yields files ordered from oldest to newest, ensuring the ordering
    // of this replay is correct.
    let files = wal.closed_segments();
    if files.is_empty() {
        info!("no wal replay files found");
        return Ok(None);
    }

    // Initialise metrics to track the progress of the WAL replay.
    //
    // The file count tracks the number of WAL files that have started
    // replaying, as opposed to finished replaying - this gives us the ability
    // to monitor WAL replays that hang or otherwise go wrong.
    let file_count_metric = metrics
        .register_metric::<U64Counter>(
            "ingester_wal_replay_files_started",
            "Number of WAL files that have started to be replayed",
        )
        .recorder(&[]);
    let op_count_metric = metrics
        .register_metric::<U64Counter>(
            "ingester_wal_replay_ops",
            "Number of operations successfully replayed from the WAL",
        )
        .recorder(&[]);

    let n_files = files.len();
    info!(n_files, "found wal files for replay");

    // Replay each file, keeping track of the last observed sequence number.
    //
    // Applying writes to the buffer can only happen monotonically and this is
    // enforced within the buffer.
    let mut max_sequence = None;
    for (index, file) in files.into_iter().enumerate() {
        // Map 0-based iter index to 1 based file count
        let file_number = index + 1;

        file_count_metric.inc(1);

        // Read the segment
        let reader = wal
            .reader_for_segment(file.id())
            .map_err(WalReplayError::OpenSegment)?;

        // Emit a log entry so progress can be tracked (and a problematic file
        // be identified should an explosion happen during replay).
        info!(
            file_number,
            n_files,
            file_id = %file.id(),
            size = file.size(),
            "replaying wal file"
        );

        // Replay this segment file
        match replay_file(reader, sink, &op_count_metric).await? {
            v @ Some(_) => max_sequence = max_sequence.max(v),
            None => {
                // This file was empty and should be deleted.
                warn!(
                    file_number,
                    n_files,
                    file_id = %file.id(),
                    size = file.size(),
                    "dropping empty wal segment",
                );

                // TODO(test): empty WAL replay

                // A failure to delete an empty file should not prevent WAL
                // replay from continuing.
                if let Err(error) = wal.delete(file.id()).await {
                    error!(
                        file_number,
                        n_files,
                        file_id = %file.id(),
                        size = file.size(),
                        %error,
                        "error dropping empty wal segment",
                    );
                }

                continue;
            }
        };

        info!(
            file_number,
            n_files,
            file_id = %file.id(),
            size = file.size(),
            "persisting wal segment data"
        );

        // Persist all the data that was replayed from the WAL segment.
        persist_partitions(sink.partition_iter(), &persist).await;

        // Drop the newly persisted data - it should not be replayed.
        wal.delete(file.id())
            .await
            .expect("failed to drop wal segment");

        info!(
            file_number,
            n_files,
            file_id = %file.id(),
            size = file.size(),
            "dropped persisted wal segment"
        );
    }

    info!(
        max_sequence_number = ?max_sequence,
        "wal replay complete"
    );

    Ok(max_sequence)
}

/// Replay the entries in `file`, applying them to `buffer`. Returns the highest
/// sequence number observed in the file, or [`None`] if the file was empty.
async fn replay_file<T>(
    file: wal::ClosedSegmentFileReader,
    sink: &T,
    op_count_metric: &U64Counter,
) -> Result<Option<SequenceNumber>, WalReplayError>
where
    T: DmlSink,
{
    let mut max_sequence = None;
    let start = Instant::now();

    for batch in file {
        let ops = batch.map_err(WalReplayError::ReadEntry)?;

        for op in ops {
            let SequencedWalOp {
                sequence_number,
                table_write_sequence_numbers: _, // TODO(savage): Use sequence numbers assigned per-partition
                op,
            } = op;

            let sequence_number = SequenceNumber::new(
                i64::try_from(sequence_number).expect("sequence number overflow"),
            );

            max_sequence = max_sequence.max(Some(sequence_number));

            let op = match op {
                Op::Write(w) => w,
                Op::Delete(_) => unreachable!(),
                Op::Persist(_) => unreachable!(),
            };

            debug!(?op, sequence_number = sequence_number.get(), "apply wal op");

            // Reconstruct the ingest operation
            let batches = decode_database_batch(&op)?;
            let namespace_id = NamespaceId::new(op.database_id);
            let partition_key = PartitionKey::from(op.partition_key);

            let op = WriteOperation::new(
                namespace_id,
                batches
                    .into_iter()
                    .map(|(k, v)| {
                        let table_id = TableId::new(k);
                        (
                            table_id,
                            // TODO(savage): Use table-partitioned sequence
                            // numbers here
                            TableData::new(table_id, PartitionedData::new(sequence_number, v)),
                        )
                    })
                    .collect(),
                partition_key,
                // TODO: A tracing context should be added for WAL replay.
                None,
            );

            // Apply the operation to the provided DML sink
            sink.apply(IngestOp::Write(op))
                .await
                .map_err(Into::<DmlError>::into)?;

            op_count_metric.inc(1);
        }
    }

    // This file is complete, return the last observed sequence
    // number.
    debug!("wal file replayed in {:?}", start.elapsed());
    Ok(max_sequence)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert_matches::assert_matches;
    use async_trait::async_trait;
    use metric::{Attributes, Metric};
    use parking_lot::Mutex;
    use wal::Wal;

    use crate::{
        buffer_tree::partition::PartitionData,
        dml_payload::IngestOp,
        dml_sink::mock_sink::MockDmlSink,
        persist::queue::mock::MockPersistQueue,
        test_util::{
            assert_dml_writes_eq, make_write_op, PartitionDataBuilder, ARBITRARY_NAMESPACE_ID,
            ARBITRARY_PARTITION_ID, ARBITRARY_PARTITION_KEY, ARBITRARY_TABLE_ID,
            ARBITRARY_TABLE_NAME,
        },
        wal::wal_sink::WalSink,
    };

    use super::*;

    #[derive(Debug)]
    struct MockIter {
        sink: MockDmlSink,
        partitions: Vec<Arc<Mutex<PartitionData>>>,
    }

    impl PartitionIter for MockIter {
        fn partition_iter(&self) -> Box<dyn Iterator<Item = Arc<Mutex<PartitionData>>> + Send> {
            Box::new(self.partitions.clone().into_iter())
        }
    }

    #[async_trait]
    impl DmlSink for MockIter {
        type Error = <MockDmlSink as DmlSink>::Error;

        async fn apply(&self, op: IngestOp) -> Result<(), Self::Error> {
            self.sink.apply(op).await
        }
    }

    #[tokio::test]
    async fn test_replay() {
        let dir = tempfile::tempdir().unwrap();

        // Generate the test ops that will be appended and read back
        let op1 = make_write_op(
            &ARBITRARY_PARTITION_KEY,
            ARBITRARY_NAMESPACE_ID,
            &ARBITRARY_TABLE_NAME,
            ARBITRARY_TABLE_ID,
            24,
            &format!(
                r#"{},region=Madrid temp=35 4242424242"#,
                &*ARBITRARY_TABLE_NAME
            ),
            None,
        );
        let op2 = make_write_op(
            &ARBITRARY_PARTITION_KEY,
            ARBITRARY_NAMESPACE_ID,
            &ARBITRARY_TABLE_NAME,
            ARBITRARY_TABLE_ID,
            25,
            &format!(
                r#"{},region=Asturias temp=25 4242424242"#,
                &*ARBITRARY_TABLE_NAME
            ),
            None,
        );
        let op3 = make_write_op(
            &ARBITRARY_PARTITION_KEY,
            ARBITRARY_NAMESPACE_ID,
            &ARBITRARY_TABLE_NAME,
            ARBITRARY_TABLE_ID,
            42,
            // Overwrite op2
            &format!(
                r#"{},region=Asturias temp=15 4242424242"#,
                &*ARBITRARY_TABLE_NAME
            ),
            None,
        );

        // The write portion of this test.
        //
        // Write two ops, rotate the file, and write a third op.
        {
            let inner =
                Arc::new(MockDmlSink::default().with_apply_return(vec![Ok(()), Ok(()), Ok(())]));
            let wal = Wal::new(dir.path())
                .await
                .expect("failed to initialise WAL");

            let wal_sink = WalSink::new(Arc::clone(&inner), Arc::clone(&wal));

            // Apply the first op through the decorator
            wal_sink
                .apply(IngestOp::Write(op1.clone()))
                .await
                .expect("wal should not error");
            // And the second op
            wal_sink
                .apply(IngestOp::Write(op2.clone()))
                .await
                .expect("wal should not error");

            // Rotate the log file
            wal.rotate().expect("failed to rotate WAL file");

            // Write the third op
            wal_sink
                .apply(IngestOp::Write(op3.clone()))
                .await
                .expect("wal should not error");

            // Assert the mock inner sink saw the calls
            assert_eq!(inner.get_calls().len(), 3);
        }

        // Reinitialise the WAL
        let wal = Wal::new(dir.path())
            .await
            .expect("failed to initialise WAL");

        assert_eq!(wal.closed_segments().len(), 2);

        // Initialise the mock persist system
        let persist = Arc::new(MockPersistQueue::default());

        // Replay the results into a mock to capture the DmlWrites and returns
        // some dummy partitions when iterated over.
        let mock_sink = MockDmlSink::default().with_apply_return(vec![Ok(()), Ok(()), Ok(())]);
        let mut partition = PartitionDataBuilder::new().build();
        // Put at least one write into the buffer so it is a candidate for persistence
        partition
            .buffer_write(
                op1.tables()
                    .next()
                    .unwrap()
                    .1
                    .partitioned_data()
                    .data()
                    .clone(),
                SequenceNumber::new(1),
            )
            .unwrap();
        let mock_iter = MockIter {
            sink: mock_sink,
            partitions: vec![Arc::new(Mutex::new(partition))],
        };

        let metrics = metric::Registry::default();
        let max_sequence_number = replay(&wal, &mock_iter, Arc::clone(&persist), &metrics)
            .await
            .expect("failed to replay WAL");

        assert_eq!(max_sequence_number, Some(SequenceNumber::new(42)));

        // Assert the ops were pushed into the DmlSink exactly as generated.
        let ops = mock_iter.sink.get_calls();
        assert_matches!(
            &*ops,
            &[
                IngestOp::Write(ref w1),
                IngestOp::Write(ref w2),
                IngestOp::Write(ref w3)
            ] => {
                assert_dml_writes_eq(w1.clone(), op1);
                assert_dml_writes_eq(w2.clone(), op2);
                assert_dml_writes_eq(w3.clone(), op3);
            }
        );

        // Ensure all partitions were persisted
        let calls = persist.calls();
        assert_matches!(&*calls, [p] => {
            assert_eq!(p.lock().partition_id(), ARBITRARY_PARTITION_ID);
        });

        // Ensure there were no partition persist panics.
        Arc::try_unwrap(persist)
            .expect("should be no more refs")
            .join()
            .await;

        // Ensure the replayed segments were dropped
        let wal = Wal::new(dir.path())
            .await
            .expect("failed to initialise WAL");

        assert_eq!(wal.closed_segments().len(), 1);

        // Validate the expected metric values were populated.
        let files = metrics
            .get_instrument::<Metric<U64Counter>>("ingester_wal_replay_files_started")
            .expect("file counter not found")
            .get_observer(&Attributes::from([]))
            .expect("attributes not found")
            .fetch();
        assert_eq!(files, 2);
        let ops = metrics
            .get_instrument::<Metric<U64Counter>>("ingester_wal_replay_ops")
            .expect("file counter not found")
            .get_observer(&Attributes::from([]))
            .expect("attributes not found")
            .fetch();
        assert_eq!(ops, 3);
    }
}
