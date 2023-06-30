//! Table level data buffer structures.

pub(crate) mod name_resolver;

use std::{fmt::Debug, sync::Arc};

use async_trait::async_trait;
use data_types::{NamespaceId, PartitionKey, SequenceNumber, TableId};
use mutable_batch::MutableBatch;
use parking_lot::Mutex;
use predicate::Predicate;
use schema::Projection;
use trace::span::{Span, SpanRecorder};

use super::{
    namespace::NamespaceName,
    partition::{resolver::PartitionProvider, PartitionData},
    post_write::PostWriteObserver,
};
use crate::{
    arcmap::ArcMap,
    deferred_load::DeferredLoad,
    query::{
        partition_response::PartitionResponse, response::PartitionStream, QueryError, QueryExec,
    },
};

/// The string name / identifier of a Table.
///
/// A reference-counted, cheap clone-able string.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct TableName(Arc<str>);

impl<T> From<T> for TableName
where
    T: AsRef<str>,
{
    fn from(v: T) -> Self {
        Self(Arc::from(v.as_ref()))
    }
}

impl From<TableName> for Arc<str> {
    fn from(v: TableName) -> Self {
        v.0
    }
}

impl std::fmt::Display for TableName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl std::ops::Deref for TableName {
    type Target = Arc<str>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl PartialEq<str> for TableName {
    fn eq(&self, other: &str) -> bool {
        &*self.0 == other
    }
}

/// Data of a Table in a given Namesapce
#[derive(Debug)]
pub(crate) struct TableData<O> {
    table_id: TableId,
    table_name: Arc<DeferredLoad<TableName>>,

    /// The catalog ID of the namespace this table is being populated from.
    namespace_id: NamespaceId,
    namespace_name: Arc<DeferredLoad<NamespaceName>>,

    /// An abstract constructor of [`PartitionData`] instances for a given
    /// `(key, table)` tuple.
    partition_provider: Arc<dyn PartitionProvider>,

    // Map of partition key to its data
    partition_data: ArcMap<PartitionKey, Mutex<PartitionData>>,

    post_write_observer: Arc<O>,
}

impl<O> TableData<O> {
    /// Initialize new table buffer identified by [`TableId`] in the catalog.
    ///
    /// The partition provider is used to instantiate a [`PartitionData`]
    /// instance when this [`TableData`] instance observes an op for a partition
    /// for the first time.
    pub(super) fn new(
        table_id: TableId,
        table_name: Arc<DeferredLoad<TableName>>,
        namespace_id: NamespaceId,
        namespace_name: Arc<DeferredLoad<NamespaceName>>,
        partition_provider: Arc<dyn PartitionProvider>,
        post_write_observer: Arc<O>,
    ) -> Self {
        Self {
            table_id,
            table_name,
            namespace_id,
            namespace_name,
            partition_data: Default::default(),
            partition_provider,
            post_write_observer,
        }
    }

    /// Return a mutable reference to all partitions buffered for this table.
    ///
    /// # Ordering
    ///
    /// The order of [`PartitionData`] in the iterator is arbitrary and should
    /// not be relied upon.
    ///
    /// # Snapshot
    ///
    /// The set of [`PartitionData`] returned is an atomic / point-in-time
    /// snapshot of the set of [`PartitionData`] at the time this function is
    /// invoked, but the data within them may change as they continue to buffer
    /// DML operations.
    pub(crate) fn partitions(&self) -> Vec<Arc<Mutex<PartitionData>>> {
        self.partition_data.values()
    }

    /// Returns the table ID for this partition.
    pub(crate) fn table_id(&self) -> TableId {
        self.table_id
    }

    /// Returns the name of this table.
    pub(crate) fn table_name(&self) -> &Arc<DeferredLoad<TableName>> {
        &self.table_name
    }

    /// Return the [`NamespaceId`] this table is a part of.
    pub(crate) fn namespace_id(&self) -> NamespaceId {
        self.namespace_id
    }
}

impl<O> TableData<O>
where
    O: PostWriteObserver,
{
    // buffers the table write and returns true if the lifecycle manager indicates that
    // ingest should be paused.
    pub(super) async fn buffer_table_write(
        &self,
        sequence_number: SequenceNumber,
        batch: MutableBatch,
        partition_key: PartitionKey,
    ) -> Result<(), mutable_batch::Error> {
        let p = self.partition_data.get(&partition_key);
        let partition_data = match p {
            Some(p) => p,
            None => {
                let p = self
                    .partition_provider
                    .get_partition(
                        partition_key.clone(),
                        self.namespace_id,
                        Arc::clone(&self.namespace_name),
                        self.table_id,
                        Arc::clone(&self.table_name),
                    )
                    .await;
                // Add the partition to the map.
                //
                // This MAY return a different instance than `p` if another
                // thread has already initialised the partition.
                self.partition_data.get_or_insert_with(&partition_key, || p)
            }
        };

        // Obtain the partition lock.
        let mut p = partition_data.lock();

        // Enqueue the write, returning any error.
        p.buffer_write(batch, sequence_number)?;

        // If successful, allow the observer to inspect the partition.
        self.post_write_observer
            .observe(Arc::clone(&partition_data), p);

        Ok(())
    }
}

#[async_trait]
impl<O> QueryExec for TableData<O>
where
    O: Send + Sync + Debug,
{
    type Response = PartitionStream;

    async fn query_exec(
        &self,
        namespace_id: NamespaceId,
        table_id: TableId,
        columns: Vec<String>,
        span: Option<Span>,
        _predicate: Option<Predicate>,
    ) -> Result<Self::Response, QueryError> {
        assert_eq!(self.table_id, table_id, "buffer tree index inconsistency");
        assert_eq!(
            self.namespace_id, namespace_id,
            "buffer tree index inconsistency"
        );

        // Gather the partition data from all of the partitions in this table.
        let span = SpanRecorder::new(span);
        let partitions = self.partitions().into_iter().map(move |p| {
            let mut span = span.child("partition read");

            let (id, hash_id, completed_persistence_count, data) = {
                let mut p = p.lock();
                (
                    p.partition_id(),
                    p.partition_hash_id().cloned(),
                    p.completed_persistence_count(),
                    p.get_query_data(),
                )
            };

            let ret = match data {
                Some(data) => {
                    // TODO(savage): Apply predicate here through the projection?
                    assert_eq!(id, data.partition_id());

                    // Project the data if necessary
                    let columns = columns.iter().map(String::as_str).collect::<Vec<_>>();
                    let selection = if columns.is_empty() {
                        Projection::All
                    } else {
                        Projection::Some(columns.as_ref())
                    };

                    let data = data.project_selection(selection).into_iter().collect();
                    PartitionResponse::new(data, id, hash_id, completed_persistence_count)
                }
                None => PartitionResponse::new(vec![], id, hash_id, completed_persistence_count),
            };

            span.ok("read partition data");
            ret
        });

        Ok(PartitionStream::new(futures::stream::iter(partitions)))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use mutable_batch_lp::lines_to_batches;

    use super::*;
    use crate::{
        buffer_tree::{
            partition::resolver::mock::MockPartitionProvider,
            post_write::mock::MockPostWriteObserver,
        },
        test_util::{
            defer_namespace_name_1_sec, defer_table_name_1_sec, PartitionDataBuilder,
            ARBITRARY_NAMESPACE_ID, ARBITRARY_PARTITION_KEY, ARBITRARY_TABLE_ID,
            ARBITRARY_TABLE_NAME,
        },
    };

    #[tokio::test]
    async fn test_partition_init() {
        // Configure the mock partition provider to return a partition for a table ID.
        let partition_provider = Arc::new(
            MockPartitionProvider::default().with_partition(PartitionDataBuilder::new().build()),
        );

        let table = TableData::new(
            ARBITRARY_TABLE_ID,
            defer_table_name_1_sec(),
            ARBITRARY_NAMESPACE_ID,
            defer_namespace_name_1_sec(),
            partition_provider,
            Arc::new(MockPostWriteObserver::default()),
        );

        let batch = lines_to_batches(
            &format!(r#"{},bat=man value=24 42"#, &*ARBITRARY_TABLE_NAME),
            0,
        )
        .unwrap()
        .remove(&***ARBITRARY_TABLE_NAME)
        .unwrap();

        // Assert the table does not contain the test partition
        assert!(table.partition_data.get(&ARBITRARY_PARTITION_KEY).is_none());

        // Write some test data
        table
            .buffer_table_write(
                SequenceNumber::new(42),
                batch,
                ARBITRARY_PARTITION_KEY.clone(),
            )
            .await
            .expect("buffer op should succeed");

        // Referencing the partition should succeed
        assert!(table.partition_data.get(&ARBITRARY_PARTITION_KEY).is_some());
    }
}
