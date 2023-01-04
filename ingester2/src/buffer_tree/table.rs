//! Table level data buffer structures.

pub(crate) mod name_resolver;

use std::{fmt::Debug, sync::Arc};

use async_trait::async_trait;
use data_types::{NamespaceId, PartitionId, PartitionKey, SequenceNumber, ShardId, TableId};
use datafusion_util::MemoryStream;
use mutable_batch::MutableBatch;
use parking_lot::{Mutex, RwLock};
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

/// A double-referenced map where [`PartitionData`] can be looked up by
/// [`PartitionKey`], or ID.
#[derive(Debug, Default)]
struct DoubleRef {
    by_key: ArcMap<PartitionKey, Mutex<PartitionData>>,
    by_id: ArcMap<PartitionId, Mutex<PartitionData>>,
}

impl DoubleRef {
    /// Try to insert the provided [`PartitionData`].
    ///
    /// Note that the partition MAY have been inserted concurrently, and the
    /// returned [`PartitionData`] MAY be a different instance for the same
    /// underlying partition.
    fn try_insert(&mut self, ns: PartitionData) -> Arc<Mutex<PartitionData>> {
        let id = ns.partition_id();
        let key = ns.partition_key().clone();

        let ns = Arc::new(Mutex::new(ns));
        self.by_key.get_or_insert_with(&key, || Arc::clone(&ns));
        self.by_id.get_or_insert_with(&id, || ns)
    }

    fn by_key(&self, key: &PartitionKey) -> Option<Arc<Mutex<PartitionData>>> {
        self.by_key.get(key)
    }

    fn by_id(&self, id: PartitionId) -> Option<Arc<Mutex<PartitionData>>> {
        self.by_id.get(&id)
    }
}

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

/// Data of a Table in a given Namesapce that belongs to a given Shard
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
    partition_data: RwLock<DoubleRef>,

    post_write_observer: Arc<O>,
    transition_shard_id: ShardId,
}

impl<O> TableData<O> {
    /// Initialize new table buffer identified by [`TableId`] in the catalog.
    ///
    /// Optionally the given tombstone max [`SequenceNumber`] identifies the
    /// inclusive upper bound of tombstones associated with this table. Any data
    /// greater than this value is guaranteed to not (yet) have a delete
    /// tombstone that must be resolved.
    ///
    /// The partition provider is used to instantiate a [`PartitionData`]
    /// instance when this [`TableData`] instance observes an op for a partition
    /// for the first time.
    pub(super) fn new(
        table_id: TableId,
        table_name: DeferredLoad<TableName>,
        namespace_id: NamespaceId,
        namespace_name: Arc<DeferredLoad<NamespaceName>>,
        partition_provider: Arc<dyn PartitionProvider>,
        post_write_observer: Arc<O>,
        transition_shard_id: ShardId,
    ) -> Self {
        Self {
            table_id,
            table_name: Arc::new(table_name),
            namespace_id,
            namespace_name,
            partition_data: Default::default(),
            partition_provider,
            post_write_observer,
            transition_shard_id,
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
        self.partition_data.read().by_key.values()
    }

    /// Return the [`PartitionData`] for the specified ID.
    pub(crate) fn partition(&self, partition_id: PartitionId) -> Option<Arc<Mutex<PartitionData>>> {
        self.partition_data.read().by_id(partition_id)
    }

    /// Return the [`PartitionData`] for the specified partition key.
    pub(crate) fn get_partition_by_key(
        &self,
        partition_key: &PartitionKey,
    ) -> Option<Arc<Mutex<PartitionData>>> {
        self.partition_data.read().by_key(partition_key)
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
        let p = self.partition_data.read().by_key(&partition_key);
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
                        self.transition_shard_id,
                    )
                    .await;
                // Add the double-referenced partition to the map.
                //
                // This MAY return a different instance than `p` if another
                // thread has already initialised the partition.
                self.partition_data.write().try_insert(p)
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
    ) -> Result<Self::Response, QueryError> {
        assert_eq!(self.table_id, table_id, "buffer tree index inconsistency");
        assert_eq!(
            self.namespace_id, namespace_id,
            "buffer tree index inconsistency"
        );

        // Gather the partition data from all of the partitions in this table.
        let partitions = self.partitions().into_iter().map(move |p| {
            let mut span = SpanRecorder::new(span.clone().map(|s| s.child("partition read")));

            let (id, completed_persistence_count, data) = {
                let mut p = p.lock();
                (
                    p.partition_id(),
                    p.completed_persistence_count(),
                    p.get_query_data(),
                )
            };

            let ret = match data {
                Some(data) => {
                    assert_eq!(id, data.partition_id());

                    // Project the data if necessary
                    let columns = columns.iter().map(String::as_str).collect::<Vec<_>>();
                    let selection = if columns.is_empty() {
                        Projection::All
                    } else {
                        Projection::Some(columns.as_ref())
                    };

                    let data = Box::pin(MemoryStream::new(
                        data.project_selection(selection).into_iter().collect(),
                    ));
                    PartitionResponse::new(Some(data), id, completed_persistence_count)
                }
                None => PartitionResponse::new(None, id, completed_persistence_count),
            };

            span.ok("read partition data");
            ret
        });

        Ok(PartitionStream::new(futures::stream::iter(partitions)))
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use data_types::PartitionId;
    use mutable_batch_lp::lines_to_batches;

    use super::*;
    use crate::buffer_tree::{
        partition::{resolver::mock::MockPartitionProvider, PartitionData, SortKeyState},
        post_write::mock::MockPostWriteObserver,
    };

    const TABLE_NAME: &str = "bananas";
    const TABLE_ID: TableId = TableId::new(44);
    const NAMESPACE_ID: NamespaceId = NamespaceId::new(42);
    const PARTITION_KEY: &str = "platanos";
    const PARTITION_ID: PartitionId = PartitionId::new(0);
    const TRANSITION_SHARD_ID: ShardId = ShardId::new(84);

    #[tokio::test]
    async fn test_partition_double_ref() {
        // Configure the mock partition provider to return a partition for this
        // table ID.
        let partition_provider = Arc::new(MockPartitionProvider::default().with_partition(
            PartitionData::new(
                PARTITION_ID,
                PARTITION_KEY.into(),
                NAMESPACE_ID,
                Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                    NamespaceName::from("platanos")
                })),
                TABLE_ID,
                Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                    TableName::from(TABLE_NAME)
                })),
                SortKeyState::Provided(None),
                TRANSITION_SHARD_ID,
            ),
        ));

        let table = TableData::new(
            TABLE_ID,
            DeferredLoad::new(Duration::from_secs(1), async {
                TableName::from(TABLE_NAME)
            }),
            NAMESPACE_ID,
            Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                NamespaceName::from("platanos")
            })),
            partition_provider,
            Arc::new(MockPostWriteObserver::default()),
            TRANSITION_SHARD_ID,
        );

        let batch = lines_to_batches(r#"bananas,bat=man value=24 42"#, 0)
            .unwrap()
            .remove(TABLE_NAME)
            .unwrap();

        // Assert the table does not contain the test partition
        assert!(table
            .partition_data
            .read()
            .by_key(&PARTITION_KEY.into())
            .is_none());
        assert!(table.partition_data.read().by_id(PARTITION_ID).is_none());

        // Write some test data
        table
            .buffer_table_write(SequenceNumber::new(42), batch, PARTITION_KEY.into())
            .await
            .expect("buffer op should succeed");

        // Referencing the partition should succeed
        assert!(table
            .partition_data
            .read()
            .by_key(&PARTITION_KEY.into())
            .is_some());
        assert!(table.partition_data.read().by_id(PARTITION_ID).is_some());
    }
}
