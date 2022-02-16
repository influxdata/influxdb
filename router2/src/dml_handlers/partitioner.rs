use async_trait::async_trait;
use data_types::{
    database_rules::PartitionTemplate, delete_predicate::DeletePredicate, DatabaseName,
};
use futures::stream::{FuturesUnordered, TryStreamExt};
use hashbrown::HashMap;
use mutable_batch::{MutableBatch, PartitionWrite, WritePayload};
use observability_deps::tracing::*;
use thiserror::Error;
use trace::ctx::SpanContext;

use super::{DmlError, DmlHandler};

/// An error raised by the [`Partitioner`] handler.
#[derive(Debug, Error)]
pub enum PartitionError {
    /// Failed to write to the partitioned table batch.
    #[error("error batching into partitioned write: {0}")]
    BatchWrite(#[from] mutable_batch::Error),

    /// The inner DML handler returned an error.
    #[error(transparent)]
    Inner(Box<DmlError>),
}

/// A decorator of `T`, tagging it with the partition key derived from it.
#[derive(Debug, PartialEq, Clone)]
pub struct Partitioned<T> {
    key: String,
    payload: T,
}

impl<T> Partitioned<T> {
    /// Wrap `payload` with a partition `key`.
    pub fn new(key: String, payload: T) -> Self {
        Self { key, payload }
    }

    /// Get a reference to the partition payload.
    pub fn payload(&self) -> &T {
        &self.payload
    }

    /// Unwrap `Self` returning the inner payload `T` and the partition key.
    pub fn into_parts(self) -> (String, T) {
        (self.key, self.payload)
    }
}

/// A [`DmlHandler`] implementation that splits per-table [`MutableBatch`] into
/// partitioned per-table [`MutableBatch`] instances according to a configured
/// [`PartitionTemplate`]. Deletes pass through unmodified.
///
/// Each partition is passed through to the inner DML handler (or chain of
/// handlers) concurrently, aborting if an error occurs. This may allow a
/// partial write to be observable down-stream of the [`Partitioner`] if at
/// least one partitioned write succeeds and at least one partitioned write
/// fails. When a partial write occurs, the handler returns an error describing
/// the failure.
#[derive(Debug)]
pub struct Partitioner<D> {
    partition_template: PartitionTemplate,
    inner: D,
}

impl<D> Partitioner<D> {
    /// Initialise a new [`Partitioner`], splitting writes according to the
    /// specified [`PartitionTemplate`] before calling `inner`.
    pub fn new(inner: D, partition_template: PartitionTemplate) -> Self {
        Self {
            partition_template,
            inner,
        }
    }
}

#[async_trait]
impl<D> DmlHandler for Partitioner<D>
where
    D: DmlHandler<WriteInput = Partitioned<HashMap<String, MutableBatch>>>,
{
    type WriteError = PartitionError;
    type DeleteError = D::DeleteError;

    type WriteInput = HashMap<String, MutableBatch>;

    /// Partition the per-table [`MutableBatch`] and call the inner handler with
    /// each partition.
    async fn write(
        &self,
        namespace: DatabaseName<'static>,
        batch: Self::WriteInput,
        span_ctx: Option<SpanContext>,
    ) -> Result<(), Self::WriteError> {
        // A collection of partition-keyed, per-table MutableBatch instances.
        let mut partitions: HashMap<_, HashMap<_, MutableBatch>> = HashMap::default();

        for (table_name, batch) in batch {
            // Partition the table batch according to the configured partition
            // template and write it into the partition-keyed map.
            for (partition_key, partition_payload) in
                PartitionWrite::partition(&table_name, &batch, &self.partition_template)
            {
                let partition = partitions.entry(partition_key).or_default();
                let table_batch = partition
                    .raw_entry_mut()
                    .from_key(&table_name)
                    .or_insert_with(|| (table_name.to_owned(), MutableBatch::default()));

                partition_payload.write_to_batch(table_batch.1)?;
            }
        }

        partitions
            .into_iter()
            .map(|(key, batch)| {
                let p = Partitioned {
                    key,
                    payload: batch,
                };

                let namespace = namespace.clone();
                let span_ctx = span_ctx.clone();
                async move { self.inner.write(namespace, p, span_ctx).await }
            })
            .collect::<FuturesUnordered<_>>()
            .try_for_each(|_| async move {
                trace!("partitioned write complete");
                Ok(())
            })
            .await
            .map_err(|e| PartitionError::Inner(Box::new(e.into())))
    }

    /// Pass the delete request through unmodified to the next handler.
    async fn delete<'a>(
        &self,
        namespace: DatabaseName<'static>,
        table_name: impl Into<String> + Send + Sync + 'a,
        predicate: DeletePredicate,
        span_ctx: Option<SpanContext>,
    ) -> Result<(), Self::DeleteError> {
        self.inner
            .delete(namespace, table_name, predicate, span_ctx)
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert_matches::assert_matches;
    use data_types::database_rules::TemplatePart;
    use lazy_static::lazy_static;
    use time::Time;

    use crate::dml_handlers::mock::{MockDmlHandler, MockDmlHandlerCall};

    use super::*;

    lazy_static! {
        /// A static default time to use in tests (1971-05-02 UTC).
        static ref DEFAULT_TIME: Time = Time::from_timestamp_nanos(42000000000000000);
    }

    // Generate a test case that partitions "lp" and calls a mock inner DML
    // handler for each partition, which returns the values specified in
    // "inner_write_returns".
    //
    // Assert the partition-to-table mapping in "want_writes" and assert the
    // handler write() return value in "want_handler_ret".
    macro_rules! test_write {
        (
            $name:ident,
            lp = $lp:expr,
            inner_write_returns = $inner_write_returns:expr,
            want_writes = [$($want_writes:tt)*], // "partition key" => ["mapped", "tables"] or [unchecked] to skip assert
            want_handler_ret = $($want_handler_ret:tt)+
        ) => {
            paste::paste! {
                #[tokio::test]
                async fn [<test_write_ $name>]() {
                    use pretty_assertions::assert_eq;

                    let partition_template = PartitionTemplate {
                        parts: vec![TemplatePart::TimeFormat("%Y-%m-%d".to_owned())],
                    };

                    let inner = Arc::new(MockDmlHandler::default().with_write_return($inner_write_returns));
                    let partitioner = Partitioner::new(Arc::clone(&inner), partition_template);
                    let ns = DatabaseName::new("bananas").expect("valid db name");

                    let (writes, _) = mutable_batch_lp::lines_to_batches_stats($lp, DEFAULT_TIME.timestamp_nanos()).expect("failed to parse test LP");

                    let handler_ret = partitioner.write(ns.clone(), writes, None).await;
                    assert_matches!(handler_ret, $($want_handler_ret)+);

                    // Collect writes into a <partition_key, table_names> map.
                    let calls = inner.calls().into_iter().map(|v| match v {
                        MockDmlHandlerCall::Write { namespace, write_input, .. } => {
                            assert_eq!(namespace, *ns);

                            // Extract the table names for comparison
                            let mut tables = write_input
                                .payload
                                .keys()
                                .cloned()
                                .collect::<Vec<String>>();

                            tables.sort();

                            (write_input.key.clone(), tables)
                        },
                        MockDmlHandlerCall::Delete { .. } => unreachable!("mock should not observe deletes"),
                    })
                    .collect::<HashMap<String, _>>();

                    test_write!(@assert_writes, calls, $($want_writes)*);
                }
            }
        };

        // Generate a NOP that doesn't assert the writes if "unchecked" is
        // specified.
        //
        // This is useful for tests that cause non-deterministic partial writes.
        (@assert_writes, $got:ident, unchecked) => { let _x = $got; };

        // Generate a block of code that validates tokens in the form of:
        //
        //      key => ["table", "names"]
        //
        // Matches the partition key / tables names observed by the mock.
        (@assert_writes, $got:ident, $($partition_key:expr => $want_tables:expr, )*) => {
            // Construct the desired writes, keyed by partition key
            #[allow(unused_mut)]
            let mut want_writes: HashMap<String, _> = Default::default();
            $(
                let mut want: Vec<String> = $want_tables.into_iter().map(|t| t.to_string()).collect();
                want.sort();
                want_writes.insert($partition_key.to_string(), want);
            )*

            assert_eq!(want_writes, $got);
        };
    }

    test_write!(
        single_partition_ok,
        lp = "\
            bananas,tag1=A,tag2=B val=42i 1\n\
            platanos,tag1=A,tag2=B value=42i 2\n\
            another,tag1=A,tag2=B value=42i 3\n\
            bananas,tag1=A,tag2=B val=42i 2\n\
            table,tag1=A,tag2=B val=42i 1\n\
        ",
        inner_write_returns = [Ok(())],
        want_writes = [
            "1970-01-01" => ["bananas", "platanos", "another", "table"],
        ],
        want_handler_ret = Ok(())
    );

    test_write!(
        single_partition_err,
        lp = "\
            bananas,tag1=A,tag2=B val=42i 1\n\
            platanos,tag1=A,tag2=B value=42i 2\n\
            another,tag1=A,tag2=B value=42i 3\n\
            bananas,tag1=A,tag2=B val=42i 2\n\
            table,tag1=A,tag2=B val=42i 1\n\
        ",
        inner_write_returns = [Err(DmlError::DatabaseNotFound("missing".to_owned()))],
        want_writes = [
            // Attempted write recorded by the mock
            "1970-01-01" => ["bananas", "platanos", "another", "table"],
        ],
        want_handler_ret = Err(PartitionError::Inner(e)) => {
            assert_matches!(*e, DmlError::DatabaseNotFound(_));
        }
    );

    test_write!(
        multiple_partitions_ok,
        lp = "\
            bananas,tag1=A,tag2=B val=42i 1\n\
            platanos,tag1=A,tag2=B value=42i 1465839830100400200\n\
            another,tag1=A,tag2=B value=42i 1465839830100400200\n\
            bananas,tag1=A,tag2=B val=42i 2\n\
            table,tag1=A,tag2=B val=42i 1644347270670952000\n\
        ",
        inner_write_returns = [Ok(()), Ok(()), Ok(())],
        want_writes = [
            "1970-01-01" => ["bananas"],
            "2016-06-13" => ["platanos", "another"],
            "2022-02-08" => ["table"],
        ],
        want_handler_ret = Ok(())
    );

    test_write!(
        multiple_partitions_total_err,
        lp = "\
            bananas,tag1=A,tag2=B val=42i 1\n\
            platanos,tag1=A,tag2=B value=42i 1465839830100400200\n\
            another,tag1=A,tag2=B value=42i 1465839830100400200\n\
            bananas,tag1=A,tag2=B val=42i 2\n\
            table,tag1=A,tag2=B val=42i 1644347270670952000\n\
        ",
        inner_write_returns = [
            Err(DmlError::DatabaseNotFound("missing".to_owned())),
            Err(DmlError::DatabaseNotFound("missing".to_owned())),
            Err(DmlError::DatabaseNotFound("missing".to_owned())),
        ],
        want_writes = [unchecked],
        want_handler_ret = Err(PartitionError::Inner(e)) => {
            assert_matches!(*e, DmlError::DatabaseNotFound(_));
        }
    );

    test_write!(
        multiple_partitions_partial_err,
        lp = "\
            bananas,tag1=A,tag2=B val=42i 1\n\
            platanos,tag1=A,tag2=B value=42i 1465839830100400200\n\
            another,tag1=A,tag2=B value=42i 1465839830100400200\n\
            bananas,tag1=A,tag2=B val=42i 2\n\
            table,tag1=A,tag2=B val=42i 1644347270670952000\n\
        ",
        inner_write_returns = [
            Err(DmlError::DatabaseNotFound("missing".to_owned())),
            Ok(()),
            Ok(()),
        ],
        want_writes = [unchecked],
        want_handler_ret = Err(PartitionError::Inner(e)) => {
            assert_matches!(*e, DmlError::DatabaseNotFound(_));
        }
    );

    test_write!(
        no_specified_timestamp,
        lp = "\
            bananas,tag1=A,tag2=B val=42i\n\
            platanos,tag1=A,tag2=B value=42i\n\
            another,tag1=A,tag2=B value=42i\n\
            bananas,tag1=A,tag2=B val=42i\n\
            table,tag1=A,tag2=B val=42i\n\
        ",
        inner_write_returns = [Ok(())],
        want_writes = [
            "1971-05-02" => ["bananas", "platanos", "another", "table"],
        ],
        want_handler_ret = Ok(())
    );
}
