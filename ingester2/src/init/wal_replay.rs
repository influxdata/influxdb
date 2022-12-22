use data_types::{NamespaceId, PartitionKey, Sequence, SequenceNumber, TableId};
use dml::{DmlMeta, DmlOperation, DmlWrite};
use generated_types::influxdata::iox::wal::v1::sequenced_wal_op::Op;
use mutable_batch_pb::decode::decode_database_batch;
use observability_deps::tracing::*;
use std::time::Instant;
use thiserror::Error;
use wal::{SequencedWalOp, Wal};

use crate::{
    dml_sink::{DmlError, DmlSink},
    persist::{drain_buffer::persist_partitions, queue::PersistQueue},
    wal::rotate_task::PartitionIter,
    TRANSITION_SHARD_INDEX,
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

    /// An error converting the WAL entry into a [`DmlOperation`].
    #[error("failed converting wal entry to dml operation: {0}")]
    MapToDml(#[from] mutable_batch_pb::decode::Error),

    /// A failure to apply a [`DmlOperation`] from the WAL to the in-memory
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
        match replay_file(reader, sink).await? {
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
        persist_partitions(sink.partition_iter(), persist.clone()).await;

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
    mut file: wal::ClosedSegmentFileReader,
    sink: &T,
) -> Result<Option<SequenceNumber>, WalReplayError>
where
    T: DmlSink,
{
    let mut max_sequence = None;
    let start = Instant::now();

    loop {
        let ops = match file.next_batch() {
            Ok(Some(v)) => v,
            Ok(None) => {
                // This file is complete, return the last observed sequence
                // number.
                debug!("wal file replayed in {:?}", start.elapsed());
                return Ok(max_sequence);
            }
            Err(e) => return Err(WalReplayError::ReadEntry(e)),
        };

        for op in ops {
            let SequencedWalOp {
                sequence_number,
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

            // Reconstruct the DML operation
            let batches = decode_database_batch(&op)?;
            let namespace_id = NamespaceId::new(op.database_id);
            let partition_key = PartitionKey::from(op.partition_key);

            let op = DmlWrite::new(
                namespace_id,
                batches
                    .into_iter()
                    .map(|(k, v)| (TableId::new(k), v))
                    .collect(),
                partition_key,
                // The tracing context should be propagated over the RPC boundary.
                DmlMeta::sequenced(
                    Sequence {
                        shard_index: TRANSITION_SHARD_INDEX, // TODO: remove this from DmlMeta
                        sequence_number,
                    },
                    iox_time::Time::MAX, // TODO: remove this from DmlMeta
                    // TODO: A tracing context should be added for WAL replay.
                    None,
                    42, // TODO: remove this from DmlMeta
                ),
            );

            // Apply the operation to the provided DML sink
            sink.apply(DmlOperation::Write(op))
                .await
                .map_err(Into::<DmlError>::into)?;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use assert_matches::assert_matches;
    use async_trait::async_trait;
    use data_types::{NamespaceId, PartitionId, PartitionKey, ShardId, TableId};
    use parking_lot::Mutex;
    use wal::Wal;

    use crate::{
        buffer_tree::partition::{PartitionData, SortKeyState},
        deferred_load::DeferredLoad,
        dml_sink::mock_sink::MockDmlSink,
        persist::queue::mock::MockPersistQueue,
        test_util::{assert_dml_writes_eq, make_write_op},
        wal::wal_sink::WalSink,
    };

    use super::*;

    const PARTITION_ID: PartitionId = PartitionId::new(42);
    const TABLE_ID: TableId = TableId::new(44);
    const TABLE_NAME: &str = "bananas";
    const NAMESPACE_NAME: &str = "platanos";
    const NAMESPACE_ID: NamespaceId = NamespaceId::new(42);

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

        async fn apply(&self, op: DmlOperation) -> Result<(), Self::Error> {
            self.sink.apply(op).await
        }
    }

    #[tokio::test]
    async fn test_replay() {
        let dir = tempfile::tempdir().unwrap();

        // Generate the test ops that will be appended and read back
        let op1 = make_write_op(
            &PartitionKey::from("p1"),
            NAMESPACE_ID,
            TABLE_NAME,
            TABLE_ID,
            24,
            r#"bananas,region=Madrid temp=35 4242424242"#,
        );
        let op2 = make_write_op(
            &PartitionKey::from("p1"),
            NAMESPACE_ID,
            TABLE_NAME,
            TABLE_ID,
            25,
            r#"bananas,region=Asturias temp=25 4242424242"#,
        );
        let op3 = make_write_op(
            &PartitionKey::from("p1"),
            NAMESPACE_ID,
            TABLE_NAME,
            TABLE_ID,
            42,
            r#"bananas,region=Asturias temp=15 4242424242"#, // Overwrite op2
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
                .apply(DmlOperation::Write(op1.clone()))
                .await
                .expect("wal should not error");
            // And the second op
            wal_sink
                .apply(DmlOperation::Write(op2.clone()))
                .await
                .expect("wal should not error");

            // Rotate the log file
            wal.rotate().expect("failed to rotate WAL file");

            // Write the third op
            wal_sink
                .apply(DmlOperation::Write(op3.clone()))
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
        let mut partition = PartitionData::new(
            PARTITION_ID,
            PartitionKey::from("bananas"),
            NAMESPACE_ID,
            Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                NAMESPACE_NAME.into()
            })),
            TABLE_ID,
            Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                TABLE_NAME.into()
            })),
            SortKeyState::Provided(None),
            ShardId::new(1234),
        );
        // Put at least one write into the buffer so it is a candidate for persistence
        partition
            .buffer_write(
                op1.tables().next().unwrap().1.clone(),
                SequenceNumber::new(1),
            )
            .unwrap();
        let mock_iter = MockIter {
            sink: mock_sink,
            partitions: vec![Arc::new(Mutex::new(partition))],
        };

        let max_sequence_number = replay(&wal, &mock_iter, Arc::clone(&persist))
            .await
            .expect("failed to replay WAL");

        assert_eq!(max_sequence_number, Some(SequenceNumber::new(42)));

        // Assert the ops were pushed into the DmlSink exactly as generated.
        let ops = mock_iter.sink.get_calls();
        assert_matches!(&*ops, &[DmlOperation::Write(ref w1),DmlOperation::Write(ref w2),DmlOperation::Write(ref w3)] => {
            assert_dml_writes_eq(w1.clone(), op1);
            assert_dml_writes_eq(w2.clone(), op2);
            assert_dml_writes_eq(w3.clone(), op3);
        });

        // Ensure all partitions were persisted
        let calls = persist.calls();
        assert_matches!(&*calls, [p] => {
            assert_eq!(p.lock().partition_id(), PARTITION_ID);
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
    }
}
