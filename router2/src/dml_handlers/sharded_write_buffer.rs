//! Logic to shard writes/deletes and push them into a write buffer sequencer.

use super::Partitioned;
use crate::{dml_handlers::DmlHandler, sequencer::Sequencer, sharder::Sharder};
use async_trait::async_trait;
use data_types2::{DatabaseName, DeletePredicate, NonEmptyString};
use dml::{DmlDelete, DmlMeta, DmlOperation, DmlWrite};
use futures::{stream::FuturesUnordered, StreamExt};
use hashbrown::HashMap;
use mutable_batch::MutableBatch;
use observability_deps::tracing::*;
use std::{
    fmt::{Debug, Display},
    sync::Arc,
};
use thiserror::Error;
use trace::ctx::SpanContext;
use write_buffer::core::WriteBufferError;

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

/// A [`ShardedWriteBuffer`] combines a [`Sequencer`] with a [`Sharder`], using
/// the latter to split writes (and deletes) up into per-shard [`DmlOperation`]
/// instances and dispatching them to the write buffer.
///
/// Writes are batched per-shard, producing one op per shard, per write. For a
/// single write, all shards are wrote to in parallel.
///
/// The buffering / async return behaviour of the methods on this type are
/// defined by the behaviour of the underlying [write buffer] implementation.
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
    S: Sharder<MutableBatch, Item = Arc<Sequencer>>
        + Sharder<DeletePredicate, Item = Arc<Sequencer>>,
{
    type WriteError = ShardError;
    type DeleteError = ShardError;

    type WriteInput = Partitioned<HashMap<String, MutableBatch>>;
    type WriteOutput = Vec<DmlMeta>;

    /// Shard `writes` and dispatch the resultant DML operations.
    async fn write(
        &self,
        namespace: &DatabaseName<'static>,
        writes: Self::WriteInput,
        span_ctx: Option<SpanContext>,
    ) -> Result<Self::WriteOutput, ShardError> {
        let mut collated: HashMap<_, HashMap<String, MutableBatch>> = HashMap::new();

        // Extract the partition key & DML writes.
        let (partition_key, writes) = writes.into_parts();

        // Shard each entry in `writes` and collate them into one DML operation
        // per shard to maximise the size of each write, and therefore increase
        // the effectiveness of compression of ops in the write buffer.
        for (table, batch) in writes.into_iter() {
            let sequencer = Arc::clone(self.sharder.shard(&table, namespace, &batch));

            let existing = collated
                .entry(sequencer)
                .or_default()
                .insert(table, batch.clone());

            assert!(existing.is_none());
        }

        let iter = collated.into_iter().map(|(sequencer, batch)| {
            let dml = DmlWrite::new(namespace, batch, DmlMeta::unsequenced(span_ctx.clone()));

            trace!(
                %partition_key,
                sequencer_id=%sequencer.id(),
                tables=%dml.table_count(),
                %namespace,
                approx_size=%dml.size(),
                "routing writes to shard"
            );

            (sequencer, DmlOperation::from(dml))
        });

        parallel_enqueue(iter).await
    }

    /// Shard `predicate` and dispatch it to the appropriate shard.
    async fn delete(
        &self,
        namespace: &DatabaseName<'static>,
        table_name: &str,
        predicate: &DeletePredicate,
        span_ctx: Option<SpanContext>,
    ) -> Result<(), ShardError> {
        let predicate = predicate.clone();
        let sequencer = self.sharder.shard(table_name, namespace, &predicate);

        trace!(sequencer_id=%sequencer.id(), %table_name, %namespace, "routing delete to shard");

        let dml = DmlDelete::new(
            namespace,
            predicate,
            NonEmptyString::new(table_name),
            DmlMeta::unsequenced(span_ctx),
        );

        sequencer
            .enqueue(DmlOperation::from(dml))
            .await
            .map_err(|e| ShardError::WriteBufferErrors {
                successes: 0,
                errs: vec![e],
            })?;

        Ok(())
    }
}

/// Enumerates all items in the iterator, maps each to a future that dispatches
/// the [`DmlOperation`] to its paired [`Sequencer`], executes all the futures
/// in parallel and gathers any errors.
///
/// Returns a list of the sequences that were written
async fn parallel_enqueue<T>(v: T) -> Result<Vec<DmlMeta>, ShardError>
where
    T: Iterator<Item = (Arc<Sequencer>, DmlOperation)> + Send,
{
    let mut successes = vec![];
    let mut errs = vec![];

    v.map(|(sequencer, op)| async move {
        tokio::spawn(async move { sequencer.enqueue(op).await })
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
    use super::*;
    use crate::{
        dml_handlers::DmlHandler,
        sharder::mock::{MockSharder, MockSharderCall},
    };
    use assert_matches::assert_matches;
    use data_types2::TimestampRange;
    use std::sync::Arc;
    use write_buffer::mock::{MockBufferForWriting, MockBufferSharedState};

    // Parse `lp` into a table-keyed MutableBatch map.
    fn lp_to_writes(lp: &str) -> Partitioned<HashMap<String, MutableBatch>> {
        let (writes, _) = mutable_batch_lp::lines_to_batches_stats(lp, 42)
            .expect("failed to build test writes from LP");
        Partitioned::new("key".to_owned(), writes)
    }

    // Init a mock write buffer with the given number of sequencers.
    fn init_write_buffer(n_sequencers: u32) -> MockBufferForWriting {
        let time = time::MockProvider::new(time::Time::from_timestamp_millis(668563200000));
        MockBufferForWriting::new(
            MockBufferSharedState::empty_with_n_sequencers(
                n_sequencers.try_into().expect("cannot have 0 sequencers"),
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
        let shard = Arc::new(Sequencer::new(
            0,
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
        let ns = DatabaseName::new("bananas").unwrap();
        w.write(&ns, writes, None).await.expect("write failed");

        // Assert the sharder saw all the tables
        let calls = sharder.calls();
        assert_eq!(calls.len(), 3);
        assert!(calls.iter().any(|v| v.table_name == "bananas"));
        assert!(calls.iter().any(|v| v.table_name == "platanos"));
        assert!(calls.iter().any(|v| v.table_name == "another"));

        // All writes were dispatched to the same shard, which should observe
        // one op containing all writes lines (asserting that all the writes for
        // one shard are collated into one op).
        let mut got = write_buffer_state.get_messages(shard.id() as _);
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
        let shard1 = Arc::new(Sequencer::new(
            0,
            Arc::new(write_buffer1),
            &Default::default(),
        ));

        // Configure the second shard to write to a different write buffer in
        // order to see which buffer saw what write.
        let write_buffer2 = init_write_buffer(2);
        let write_buffer2_state = write_buffer2.state();
        let shard2 = Arc::new(Sequencer::new(
            1,
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
        let ns = DatabaseName::new("bananas").unwrap();
        w.write(&ns, writes, None).await.expect("write failed");

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
        let mut got = write_buffer1_state.get_messages(shard1.id() as _);
        assert_eq!(got.len(), 1);
        let got = got
            .pop()
            .unwrap()
            .expect("write should have been successful");
        assert_matches!(got, DmlOperation::Write(w) => {
            assert_eq!(w.table_count(), 3);
        });

        // The second shard should observe 1 write containing 1 row.
        let mut got = write_buffer2_state.get_messages(shard2.id() as _);
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
        let shard1 = Arc::new(Sequencer::new(
            0,
            Arc::new(write_buffer1),
            &Default::default(),
        ));

        // Configure the second shard to write to a write buffer that always fails
        let write_buffer2 = init_write_buffer(1);
        // Non-existant sequencer ID to trigger an error.
        let shard2 = Arc::new(Sequencer::new(
            13,
            Arc::new(write_buffer2),
            &Default::default(),
        ));

        let sharder = Arc::new(
            MockSharder::default().with_return([Arc::clone(&shard1), Arc::clone(&shard2)]),
        );

        let w = ShardedWriteBuffer::new(Arc::clone(&sharder));

        // Call the ShardedWriteBuffer and drive the test
        let ns = DatabaseName::new("bananas").unwrap();
        let err = w
            .write(&ns, writes, None)
            .await
            .expect_err("write should return a failure");
        assert_matches!(err, ShardError::WriteBufferErrors{successes, errs} => {
            assert_eq!(errs.len(), 1);
            assert_eq!(successes, 1);
        });

        // The write buffer for shard 1 should observe 1 write independent of
        // the second, erroring shard.
        let got = write_buffer1_state.get_messages(shard1.id() as _);
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
        let shard = Arc::new(Sequencer::new(
            0,
            Arc::new(write_buffer),
            &Default::default(),
        ));
        let sharder = Arc::new(MockSharder::default().with_return([Arc::clone(&shard)]));

        let w = ShardedWriteBuffer::new(Arc::clone(&sharder));

        // Call the ShardedWriteBuffer and drive the test
        let ns = DatabaseName::new("namespace").unwrap();
        w.delete(&ns, TABLE, &predicate, None)
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
        let mut got = write_buffer_state.get_messages(shard.id() as _);
        assert_eq!(got.len(), 1);
        let got = got
            .pop()
            .unwrap()
            .expect("write should have been successful");
        assert_matches!(got, DmlOperation::Delete(d) => {
            assert_eq!(d.table_name(), Some(TABLE));
            assert_eq!(*d.predicate(), predicate);
        });
    }
}
