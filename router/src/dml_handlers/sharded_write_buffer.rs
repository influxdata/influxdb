//! Logic to shard writes/deletes and push them into a write buffer shard.

use std::{
    fmt::{Debug, Display},
    sync::Arc,
};

use async_trait::async_trait;
use data_types::{DeletePredicate, NamespaceId, NamespaceName, NonEmptyString, TableId};
use dml::{DmlDelete, DmlMeta, DmlOperation, DmlWrite};
use futures::{stream::FuturesUnordered, StreamExt};
use hashbrown::HashMap;
use mutable_batch::MutableBatch;
use observability_deps::tracing::*;
use sharder::Sharder;
use thiserror::Error;
use trace::ctx::SpanContext;
use write_buffer::core::WriteBufferError;

use super::Partitioned;
use crate::{dml_handlers::DmlHandler, shard::Shard};

/// Errors occurring while writing to one or more write buffer shards.
#[derive(Debug, Error)]
pub enum ShardError {
    /// An error occurred when writing to one or more shards.
    ///
    /// This error indicates a partial write may have occurred if `successes >
    /// 0`.
    #[error("{} shards failed pushing to write buffer ({} shards successful): [{}]", .errs.len(), .successes, join_strings(.errs))]
    WriteBufferErrors {
        /// The number of successful shard writes.
        successes: usize,
        /// The errors returned by the failed shard writes.
        errs: Vec<WriteBufferError>,
    },
}

/// Helper function to turn the set of `T` into strings and join them with `;`.
///
/// Useful to join an array of errors for display purposes.
fn join_strings<T>(s: &[T]) -> String
where
    T: Display,
{
    s.iter()
        .map(|e| e.to_string())
        .collect::<Vec<String>>()
        .join("; ")
}

/// A [`ShardedWriteBuffer`] combines a [`Shard`] with a [`Sharder`], using
/// the latter to split writes (and deletes) up into per-shard [`DmlOperation`]
/// instances and dispatching them to the write buffer.
///
/// Writes are batched per-shard, producing one op per shard, per write. For a
/// single write, all shards are wrote to in parallel.
///
/// The buffering / async return behaviour of the methods on this type are
/// defined by the behaviour of the underlying [write buffer] implementation.
///
/// Operations that require writing to multiple shards may experience partial
/// failures - the op may be successfully wrote to one shard, while failing to
/// write to another shard. Users are expected to retry the partially failed
/// operation to converge the system. The order of writes across multiple shards
/// is non-deterministic.
///
/// [write buffer]: write_buffer::core::WriteBufferWriting
#[derive(Debug)]
pub struct ShardedWriteBuffer<S> {
    sharder: S,
}

impl<S> ShardedWriteBuffer<S> {
    /// Construct a [`ShardedWriteBuffer`] using the specified [`Sharder`]
    /// implementation.
    pub fn new(sharder: S) -> Self {
        Self { sharder }
    }
}

#[async_trait]
impl<S> DmlHandler for ShardedWriteBuffer<S>
where
    S: Sharder<MutableBatch, Item = Arc<Shard>> + Sharder<DeletePredicate, Item = Vec<Arc<Shard>>>,
{
    type WriteError = ShardError;
    type DeleteError = ShardError;

    type WriteInput = Partitioned<HashMap<TableId, (String, MutableBatch)>>;
    type WriteOutput = Vec<DmlMeta>;

    /// Shard `writes` and dispatch the resultant DML operations.
    async fn write(
        &self,
        namespace: &NamespaceName<'static>,
        namespace_id: NamespaceId,
        writes: Self::WriteInput,
        span_ctx: Option<SpanContext>,
    ) -> Result<Self::WriteOutput, ShardError> {
        // Extract the partition key & DML writes.
        let (partition_key, writes) = writes.into_parts();

        // Sets of maps collated by destination shard for batching/merging of
        // shard data.
        let mut collated: HashMap<_, HashMap<TableId, MutableBatch>> = HashMap::new();

        // Shard each entry in `writes` and collate them into one DML operation
        // per shard to maximise the size of each write, and therefore increase
        // the effectiveness of compression of ops in the write buffer.
        for (table_id, (table_name, batch)) in writes.into_iter() {
            let shard = self.sharder.shard(&table_name, namespace, &batch);

            let existing = collated
                .entry(Arc::clone(&shard))
                .or_default()
                .insert(table_id, batch);
            assert!(existing.is_none());
        }

        let iter = collated.into_iter().map(|(shard, batch)| {
            let dml = DmlWrite::new(
                namespace_id,
                batch,
                partition_key.clone(),
                DmlMeta::unsequenced(span_ctx.clone()),
            );

            trace!(
                %partition_key,
                kafka_partition=%shard.shard_index(),
                tables=%dml.table_count(),
                %namespace,
                %namespace_id,
                approx_size=%dml.size(),
                "routing writes to shard"
            );

            (shard, DmlOperation::from(dml))
        });

        parallel_enqueue(iter).await
    }

    /// Shard `predicate` and dispatch it to the appropriate shard.
    async fn delete(
        &self,
        namespace: &NamespaceName<'static>,
        namespace_id: NamespaceId,
        table_name: &str,
        predicate: &DeletePredicate,
        span_ctx: Option<SpanContext>,
    ) -> Result<(), ShardError> {
        let predicate = predicate.clone();
        let shards = self.sharder.shard(table_name, namespace, &predicate);

        let dml = DmlDelete::new(
            namespace_id,
            predicate,
            NonEmptyString::new(table_name),
            DmlMeta::unsequenced(span_ctx),
        );

        let iter = shards.into_iter().map(|s| {
            trace!(
                shard_index=%s.shard_index(),
                %table_name,
                %namespace,
                %namespace_id,
                "routing delete to shard"
            );

            (s, DmlOperation::from(dml.clone()))
        });

        // TODO: return shard metadata
        parallel_enqueue(iter).await?;

        Ok(())
    }
}

/// Enumerates all items in the iterator, maps each to a future that dispatches
/// the [`DmlOperation`] to its paired [`Shard`], executes all the futures
/// in parallel and gathers any errors.
///
/// Returns a list of the sequences that were written.
async fn parallel_enqueue<T>(v: T) -> Result<Vec<DmlMeta>, ShardError>
where
    T: Iterator<Item = (Arc<Shard>, DmlOperation)> + Send,
{
    let mut successes = vec![];
    let mut errs = vec![];

    v.map(|(shard, op)| async move {
        tokio::spawn(async move { shard.enqueue(op).await })
            .await
            .expect("shard enqueue panic")
    })
    // Use FuturesUnordered so the futures can run in parallel
    .collect::<FuturesUnordered<_>>()
    .collect::<Vec<_>>()
    .await
    // Sort the result into successes/failures upon completion
    .into_iter()
    .for_each(|v| match v {
        Ok(meta) => successes.push(meta),
        Err(e) => errs.push(e),
    });

    match errs.len() {
        0 => Ok(successes),
        _n => Err(ShardError::WriteBufferErrors {
            successes: successes.len(),
            errs,
        }),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert_matches::assert_matches;
    use data_types::{ShardIndex, TimestampRange};
    use sharder::mock::{MockSharder, MockSharderCall, MockSharderPayload};
    use write_buffer::mock::{MockBufferForWriting, MockBufferSharedState};

    use super::*;
    use crate::dml_handlers::DmlHandler;

    // Parse `lp` into a table-keyed MutableBatch map.
    fn lp_to_writes(lp: &str) -> Partitioned<HashMap<TableId, (String, MutableBatch)>> {
        let (writes, _) = mutable_batch_lp::lines_to_batches_stats(lp, 42)
            .expect("failed to build test writes from LP");

        let writes = writes
            .into_iter()
            .enumerate()
            .map(|(i, (name, data))| (TableId::new(i as _), (name, data)))
            .collect();
        Partitioned::new("key".into(), writes)
    }

    // Init a mock write buffer with the given number of shards.
    fn init_write_buffer(n_shards: u32) -> MockBufferForWriting {
        let time = iox_time::MockProvider::new(
            iox_time::Time::from_timestamp_millis(668563200000).unwrap(),
        );
        MockBufferForWriting::new(
            MockBufferSharedState::empty_with_n_shards(
                n_shards.try_into().expect("cannot have 0 shards"),
            ),
            None,
            Arc::new(time),
        )
        .expect("failed to init mock write buffer")
    }

    #[tokio::test]
    async fn test_shard_writes_collated() {
        let write_buffer = init_write_buffer(1);
        let write_buffer_state = write_buffer.state();

        let writes = lp_to_writes(
            "\
                bananas,tag1=A,tag2=B val=42i 123456\n\
                bananas,tag1=A,tag2=B val=42i 456789\n\
                platanos,tag1=A,tag2=B value=42i 123456\n\
                another,tag1=A,tag2=B value=42i 123456\n\
            ",
        );

        // Configure the sharder to return shards containing the mock write
        // buffer.
        let shard = Arc::new(Shard::new(
            ShardIndex::new(0),
            Arc::new(write_buffer),
            &Default::default(),
        ));
        let sharder = Arc::new(MockSharder::default().with_return([
            Arc::clone(&shard),
            Arc::clone(&shard),
            Arc::clone(&shard),
        ]));

        let w = ShardedWriteBuffer::new(Arc::clone(&sharder));

        // Call the ShardedWriteBuffer and drive the test
        let ns = NamespaceName::new("bananas").unwrap();
        w.write(&ns, NamespaceId::new(42), writes, None)
            .await
            .expect("write failed");

        // Assert the sharder saw all the tables
        let calls = sharder.calls();
        assert_eq!(calls.len(), 3);
        assert!(calls.iter().any(|v| v.table_name == "bananas"));
        assert!(calls.iter().any(|v| v.table_name == "platanos"));
        assert!(calls.iter().any(|v| v.table_name == "another"));

        // All writes were dispatched to the same shard, which should observe
        // one op containing all writes lines (asserting that all the writes for
        // one shard are collated into one op).
        let mut got = write_buffer_state.get_messages(shard.shard_index());
        assert_eq!(got.len(), 1);
        let got = got
            .pop()
            .unwrap()
            .expect("write should have been successful");
        assert_matches!(got, DmlOperation::Write(w) => {
            assert_eq!(w.table_count(), 3);
        });
    }

    #[tokio::test]
    async fn test_multiple_shard_writes() {
        let writes = lp_to_writes(
            "\
                bananas,tag1=A,tag2=B val=42i 123456\n\
                platanos,tag1=A,tag2=B value=42i 123456\n\
                another,tag1=A,tag2=B value=42i 123456\n\
                table,tag1=A,tag2=B val=42i 456789\n\
            ",
        );

        // Configure the first shard to write to one write buffer
        let write_buffer1 = init_write_buffer(1);
        let write_buffer1_state = write_buffer1.state();
        let shard1 = Arc::new(Shard::new(
            ShardIndex::new(0),
            Arc::new(write_buffer1),
            &Default::default(),
        ));

        // Configure the second shard to write to a different write buffer in
        // order to see which buffer saw what write.
        let write_buffer2 = init_write_buffer(2);
        let write_buffer2_state = write_buffer2.state();
        let shard2 = Arc::new(Shard::new(
            ShardIndex::new(1),
            Arc::new(write_buffer2),
            &Default::default(),
        ));

        let sharder = Arc::new(MockSharder::default().with_return([
            // 4 tables, 3 mapped to the first shard
            Arc::clone(&shard1),
            Arc::clone(&shard1),
            Arc::clone(&shard1),
            // And one mapped to the second shard
            Arc::clone(&shard2),
        ]));

        let w = ShardedWriteBuffer::new(Arc::clone(&sharder));

        // Call the ShardedWriteBuffer and drive the test
        let ns = NamespaceName::new("bananas").unwrap();
        w.write(&ns, NamespaceId::new(42), writes, None)
            .await
            .expect("write failed");

        // Assert the sharder saw all the tables
        let calls = sharder.calls();
        assert_eq!(calls.len(), 4);
        assert!(calls
            .iter()
            .any(|v| v.table_name == "bananas" && v.payload.mutable_batch().rows() == 1));
        assert!(calls
            .iter()
            .any(|v| v.table_name == "platanos" && v.payload.mutable_batch().rows() == 1));
        assert!(calls
            .iter()
            .any(|v| v.table_name == "another" && v.payload.mutable_batch().rows() == 1));
        assert!(calls
            .iter()
            .any(|v| v.table_name == "table" && v.payload.mutable_batch().rows() == 1));

        // The write buffer for shard 1 should observe 1 write containing 3 rows.
        let mut got = write_buffer1_state.get_messages(shard1.shard_index());
        assert_eq!(got.len(), 1);
        let got = got
            .pop()
            .unwrap()
            .expect("write should have been successful");
        assert_matches!(got, DmlOperation::Write(w) => {
            assert_eq!(w.table_count(), 3);
        });

        // The second shard should observe 1 write containing 1 row.
        let mut got = write_buffer2_state.get_messages(shard2.shard_index());
        assert_eq!(got.len(), 1);
        let got = got
            .pop()
            .unwrap()
            .expect("write should have been successful");
        assert_matches!(got, DmlOperation::Write(w) => {
            assert_eq!(w.table_count(), 1);
        });
    }

    #[tokio::test]
    async fn test_write_partial_success() {
        let writes = lp_to_writes(
            "\
                bananas,tag1=A,tag2=B val=42i 123456\n\
                platanos,tag1=A,tag2=B value=42i 123456\n\
            ",
        );

        // Configure the first shard to write to one write buffer
        let write_buffer1 = init_write_buffer(1);
        let write_buffer1_state = write_buffer1.state();
        let shard1 = Arc::new(Shard::new(
            ShardIndex::new(0),
            Arc::new(write_buffer1),
            &Default::default(),
        ));

        // Configure the second shard to write to a write buffer that always fails
        let write_buffer2 = init_write_buffer(1);
        // Non-existent shard index to trigger an error.
        let shard2 = Arc::new(Shard::new(
            ShardIndex::new(13),
            Arc::new(write_buffer2),
            &Default::default(),
        ));

        let sharder = Arc::new(
            MockSharder::default().with_return([Arc::clone(&shard1), Arc::clone(&shard2)]),
        );

        let w = ShardedWriteBuffer::new(Arc::clone(&sharder));

        // Call the ShardedWriteBuffer and drive the test
        let ns = NamespaceName::new("bananas").unwrap();
        let err = w
            .write(&ns, NamespaceId::new(42), writes, None)
            .await
            .expect_err("write should return a failure");
        assert_matches!(err, ShardError::WriteBufferErrors{successes, errs} => {
            assert_eq!(errs.len(), 1);
            assert_eq!(successes, 1);
        });

        // The write buffer for shard 1 should observe 1 write independent of
        // the second, erroring shard.
        let got = write_buffer1_state.get_messages(shard1.shard_index());
        assert_eq!(got.len(), 1);
    }

    #[tokio::test]
    async fn test_shard_delete() {
        const TABLE: &str = "bananas";

        let write_buffer = init_write_buffer(1);
        let write_buffer_state = write_buffer.state();

        let predicate = DeletePredicate {
            range: TimestampRange::new(1, 2),
            exprs: vec![],
        };

        // Configure the sharder to return shards containing the mock write
        // buffer.
        let shard = Arc::new(Shard::new(
            ShardIndex::new(0),
            Arc::new(write_buffer),
            &Default::default(),
        ));
        let sharder = Arc::new(MockSharder::default().with_return([Arc::clone(&shard)]));

        let w = ShardedWriteBuffer::new(Arc::clone(&sharder));

        // Call the ShardedWriteBuffer and drive the test
        let ns = NamespaceName::new("namespace").unwrap();
        w.delete(&ns, NamespaceId::new(42), TABLE, &predicate, None)
            .await
            .expect("delete failed");

        // Assert the sharder saw all the tables
        let calls = sharder.calls();
        assert_matches!(calls.as_slice(), [MockSharderCall{table_name, ..}] => {
            assert_eq!(table_name, TABLE);
        });

        // All writes were dispatched to the same shard, which should observe
        // one op containing all writes lines (asserting that all the writes for
        // one shard are collated into one op).
        let mut got = write_buffer_state.get_messages(shard.shard_index());
        assert_eq!(got.len(), 1);
        let got = got
            .pop()
            .unwrap()
            .expect("write should have been successful");
        assert_matches!(got, DmlOperation::Delete(d) => {
            assert_eq!(d.table_name(), Some(TABLE));
            assert_eq!(d.namespace_id().get(), 42);
            assert_eq!(*d.predicate(), predicate);
        });
    }

    #[tokio::test]
    async fn test_shard_delete_no_table() {
        let write_buffer = init_write_buffer(1);
        let write_buffer_state = write_buffer.state();

        let predicate = DeletePredicate {
            range: TimestampRange::new(1, 2),
            exprs: vec![],
        };

        // Configure the sharder to return shards containing the mock write
        // buffer.
        let shard = Arc::new(Shard::new(
            ShardIndex::new(0),
            Arc::new(write_buffer),
            &Default::default(),
        ));
        let sharder = Arc::new(MockSharder::default().with_return([Arc::clone(&shard)]));

        let w = ShardedWriteBuffer::new(Arc::clone(&sharder));

        // Call the ShardedWriteBuffer and drive the test
        let ns = NamespaceName::new("namespace").unwrap();
        w.delete(&ns, NamespaceId::new(42), "", &predicate, None)
            .await
            .expect("delete failed");

        // Assert the table name was captured as empty.
        let calls = sharder.calls();
        assert_matches!(calls.as_slice(), [MockSharderCall{table_name, payload, ..}] => {
            assert_eq!(table_name, "");
            assert_matches!(payload, MockSharderPayload::DeletePredicate(..));
        });

        // All writes were dispatched to the same shard, which should observe
        // one op containing all writes lines (asserting that all the writes for
        // one shard are collated into one op).
        //
        // The table name should be None as it was specified as an empty string.
        let mut got = write_buffer_state.get_messages(shard.shard_index());
        assert_eq!(got.len(), 1);
        let got = got
            .pop()
            .unwrap()
            .expect("write should have been successful");
        assert_matches!(got, DmlOperation::Delete(d) => {
            assert_eq!(d.table_name(), None);
            assert_eq!(*d.predicate(), predicate);
        });
    }

    #[derive(Debug)]
    struct MultiDeleteSharder(Vec<Arc<Shard>>);

    impl Sharder<DeletePredicate> for MultiDeleteSharder {
        type Item = Vec<Arc<Shard>>;

        fn shard(
            &self,
            _table: &str,
            _namespace: &NamespaceName<'_>,
            _payload: &DeletePredicate,
        ) -> Self::Item {
            self.0.clone()
        }
    }

    impl Sharder<MutableBatch> for MultiDeleteSharder {
        type Item = Arc<Shard>;

        fn shard(
            &self,
            _table: &str,
            _namespace: &NamespaceName<'_>,
            _payload: &MutableBatch,
        ) -> Self::Item {
            unreachable!()
        }
    }

    #[tokio::test]
    async fn test_shard_delete_multiple_shards() {
        const TABLE: &str = "bananas";

        let predicate = DeletePredicate {
            range: TimestampRange::new(1, 2),
            exprs: vec![],
        };

        // Configure the first shard to write to one write buffer
        let write_buffer1 = init_write_buffer(1);
        let write_buffer1_state = write_buffer1.state();
        let shard1 = Arc::new(Shard::new(
            ShardIndex::new(0),
            Arc::new(write_buffer1),
            &Default::default(),
        ));

        // Configure the second shard to write to another write buffer
        let write_buffer2 = init_write_buffer(1);
        let write_buffer2_state = write_buffer2.state();
        let shard2 = Arc::new(Shard::new(
            ShardIndex::new(0),
            Arc::new(write_buffer2),
            &Default::default(),
        ));

        let sharder = MultiDeleteSharder(vec![Arc::clone(&shard1), Arc::clone(&shard2)]);

        let w = ShardedWriteBuffer::new(sharder);

        // Call the ShardedWriteBuffer and drive the test
        let ns = NamespaceName::new("namespace").unwrap();
        w.delete(&ns, NamespaceId::new(42), TABLE, &predicate, None)
            .await
            .expect("delete failed");

        // The write buffer for shard 1 should observe the delete
        let mut got = write_buffer1_state.get_messages(shard1.shard_index());
        assert_eq!(got.len(), 1);
        let got = got
            .pop()
            .unwrap()
            .expect("write should have been successful");
        assert_matches!(got, DmlOperation::Delete(_));

        // The second shard should observe the delete as well
        let mut got = write_buffer2_state.get_messages(shard2.shard_index());
        assert_eq!(got.len(), 1);
        let got = got
            .pop()
            .unwrap()
            .expect("write should have been successful");
        assert_matches!(got, DmlOperation::Delete(_));
    }

    #[tokio::test]
    async fn test_shard_delete_multiple_shards_partial_failure() {
        const TABLE: &str = "bananas";

        let predicate = DeletePredicate {
            range: TimestampRange::new(1, 2),
            exprs: vec![],
        };

        // Configure the first shard to write to one write buffer
        let write_buffer1 = init_write_buffer(1);
        let write_buffer1_state = write_buffer1.state();
        let shard1 = Arc::new(Shard::new(
            ShardIndex::new(0),
            Arc::new(write_buffer1),
            &Default::default(),
        ));

        // Configure the second shard to write to a write buffer that always fails
        let write_buffer2 = init_write_buffer(1);
        // Non-existent shard index to trigger an error.
        let shard2 = Arc::new(Shard::new(
            ShardIndex::new(13),
            Arc::new(write_buffer2),
            &Default::default(),
        ));

        let sharder = MultiDeleteSharder(vec![Arc::clone(&shard1), Arc::clone(&shard2)]);

        let w = ShardedWriteBuffer::new(sharder);

        // Call the ShardedWriteBuffer and drive the test
        let ns = NamespaceName::new("namespace").unwrap();
        let err = w
            .delete(&ns, NamespaceId::new(42), TABLE, &predicate, None)
            .await
            .expect_err("delete should fail");
        assert_matches!(err, ShardError::WriteBufferErrors{successes, errs} => {
            assert_eq!(errs.len(), 1);
            assert_eq!(successes, 1);
        });

        // The write buffer for shard 1 will still observer the delete.
        let got = write_buffer1_state.get_messages(shard1.shard_index());
        assert_eq!(got.len(), 1);
    }
}
