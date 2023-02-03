//! Table level data buffer structures.

pub(crate) mod name_resolver;

use std::sync::Arc;

use data_types::{NamespaceId, PartitionId, PartitionKey, SequenceNumber, ShardId, TableId};
use mutable_batch::MutableBatch;
use parking_lot::{Mutex, RwLock};
use write_summary::ShardProgress;

use super::partition::{resolver::PartitionProvider, BufferError, PartitionData};
use crate::{
    arcmap::ArcMap, data::DmlApplyAction, deferred_load::DeferredLoad, lifecycle::LifecycleHandle,
};

/// A double-referenced map where [`PartitionData`] can be looked up by
/// [`PartitionKey`], or ID.
#[derive(Debug, Default)]
struct DoubleRef {
    // TODO(4880): this can be removed when IDs are sent over the wire.
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
pub struct TableName(Arc<str>);

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
        self.0.fmt(f)
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
pub(crate) struct TableData {
    table_id: TableId,
    table_name: Arc<DeferredLoad<TableName>>,

    /// The catalog ID of the shard & namespace this table is being populated
    /// from.
    shard_id: ShardId,
    namespace_id: NamespaceId,

    /// An abstract constructor of [`PartitionData`] instances for a given
    /// `(key, shard, table)` triplet.
    partition_provider: Arc<dyn PartitionProvider>,

    // Map of partition key to its data
    partition_data: RwLock<DoubleRef>,
}

impl TableData {
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
        shard_id: ShardId,
        namespace_id: NamespaceId,
        partition_provider: Arc<dyn PartitionProvider>,
    ) -> Self {
        Self {
            table_id,
            table_name: Arc::new(table_name),
            shard_id,
            namespace_id,
            partition_data: Default::default(),
            partition_provider,
        }
    }

    // buffers the table write and returns true if the lifecycle manager indicates that
    // ingest should be paused.
    pub(super) async fn buffer_table_write(
        &self,
        sequence_number: SequenceNumber,
        batch: MutableBatch,
        partition_key: PartitionKey,
        lifecycle_handle: &dyn LifecycleHandle,
    ) -> Result<DmlApplyAction, crate::data::Error> {
        let p = self.partition_data.read().by_key(&partition_key);
        let partition_data = match p {
            Some(p) => p,
            None => {
                let p = self
                    .partition_provider
                    .get_partition(
                        partition_key.clone(),
                        self.shard_id,
                        self.namespace_id,
                        self.table_id,
                        Arc::clone(&self.table_name),
                    )
                    .await;
                // Add the double-referenced partition to the map.
                //
                // This MAY return a different instance than `p` if another
                // thread has already initialised the partition.
                self.partition_data.write().try_insert(p)
            }
        };

        let size = batch.size();
        let rows = batch.rows();
        let partition_id = {
            let mut p = partition_data.lock();
            match p.buffer_write(batch, sequence_number) {
                Ok(_) => p.partition_id(),
                Err(BufferError::SkipPersisted) => return Ok(DmlApplyAction::Skipped),
                Err(BufferError::BufferError(e)) => {
                    return Err(crate::data::Error::BufferWrite { source: e })
                }
            }
        };

        // Record the write as having been buffered.
        //
        // This should happen AFTER the write is applied, because buffering the
        // op may fail which would lead to a write being recorded, but not
        // applied.
        let should_pause = lifecycle_handle.log_write(
            partition_id,
            self.shard_id,
            self.namespace_id,
            self.table_id,
            sequence_number,
            size,
            rows,
        );

        Ok(DmlApplyAction::Applied(should_pause))
    }

    /// Return a mutable reference to all partitions buffered for this table.
    ///
    /// # Ordering
    ///
    /// The order of [`PartitionData`] in the iterator is arbitrary and should
    /// not be relied upon.
    pub(crate) fn partitions(&self) -> Vec<Arc<Mutex<PartitionData>>> {
        self.partition_data.read().by_key.values()
    }

    /// Return the [`PartitionData`] for the specified ID.
    #[allow(unused)]
    pub(crate) fn get_partition(
        &self,
        partition_id: PartitionId,
    ) -> Option<Arc<Mutex<PartitionData>>> {
        self.partition_data.read().by_id(partition_id)
    }

    /// Return the [`PartitionData`] for the specified partition key.
    #[cfg(test)]
    pub(crate) fn get_partition_by_key(
        &self,
        partition_key: &PartitionKey,
    ) -> Option<Arc<Mutex<PartitionData>>> {
        self.partition_data.read().by_key(partition_key)
    }

    /// Return progress from this Table
    pub(super) fn progress(&self) -> ShardProgress {
        self.partition_data
            .read()
            .by_key
            .values()
            .into_iter()
            .fold(Default::default(), |progress, p| {
                progress.combine(p.lock().progress())
            })
    }

    /// Returns the table ID for this partition.
    pub(crate) fn table_id(&self) -> TableId {
        self.table_id
    }

    /// Returns the name of this table.
    pub(crate) fn table_name(&self) -> &Arc<DeferredLoad<TableName>> {
        &self.table_name
    }

    /// Return the shard ID for this table.
    pub(crate) fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    /// Return the [`NamespaceId`] this table is a part of.
    pub(crate) fn namespace_id(&self) -> NamespaceId {
        self.namespace_id
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use assert_matches::assert_matches;
    use data_types::PartitionId;
    use mutable_batch::writer;
    use mutable_batch_lp::lines_to_batches;
    use schema::{InfluxColumnType, InfluxFieldType};

    use super::*;
    use crate::{
        buffer_tree::partition::{resolver::MockPartitionProvider, PartitionData, SortKeyState},
        data::Error,
        lifecycle::mock_handle::{MockLifecycleCall, MockLifecycleHandle},
    };

    const SHARD_ID: ShardId = ShardId::new(22);
    const TABLE_NAME: &str = "bananas";
    const TABLE_ID: TableId = TableId::new(44);
    const NAMESPACE_ID: NamespaceId = NamespaceId::new(42);
    const PARTITION_KEY: &str = "platanos";
    const PARTITION_ID: PartitionId = PartitionId::new(0);

    #[tokio::test]
    async fn test_partition_double_ref() {
        // Configure the mock partition provider to return a partition for this
        // table ID.
        let partition_provider = Arc::new(MockPartitionProvider::default().with_partition(
            PartitionData::new(
                PARTITION_ID,
                PARTITION_KEY.into(),
                SHARD_ID,
                NAMESPACE_ID,
                TABLE_ID,
                Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                    TableName::from(TABLE_NAME)
                })),
                SortKeyState::Provided(None),
                None,
            ),
        ));

        let table = TableData::new(
            TABLE_ID,
            DeferredLoad::new(Duration::from_secs(1), async {
                TableName::from(TABLE_NAME)
            }),
            SHARD_ID,
            NAMESPACE_ID,
            partition_provider,
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
        let action = table
            .buffer_table_write(
                SequenceNumber::new(42),
                batch,
                PARTITION_KEY.into(),
                &MockLifecycleHandle::default(),
            )
            .await
            .expect("buffer op should succeed");
        assert_matches!(action, DmlApplyAction::Applied(false));

        // Referencing the partition should succeed
        assert!(table
            .partition_data
            .read()
            .by_key(&PARTITION_KEY.into())
            .is_some());
        assert!(table.partition_data.read().by_id(PARTITION_ID).is_some());
    }

    #[tokio::test]
    async fn test_bad_write_memory_counting() {
        // Configure the mock partition provider to return a partition for this
        // table ID.
        let partition_provider = Arc::new(MockPartitionProvider::default().with_partition(
            PartitionData::new(
                PARTITION_ID,
                PARTITION_KEY.into(),
                SHARD_ID,
                NAMESPACE_ID,
                TABLE_ID,
                Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                    TableName::from(TABLE_NAME)
                })),
                SortKeyState::Provided(None),
                None,
            ),
        ));

        let table = TableData::new(
            TABLE_ID,
            DeferredLoad::new(Duration::from_secs(1), async {
                TableName::from(TABLE_NAME)
            }),
            SHARD_ID,
            NAMESPACE_ID,
            partition_provider,
        );

        let batch = lines_to_batches(r#"bananas,bat=man value=24 42"#, 0)
            .unwrap()
            .remove(TABLE_NAME)
            .unwrap();

        // Initialise the mock lifecycle handle and use it to inspect the calls
        // made to the lifecycle manager during buffering.
        let handle = MockLifecycleHandle::default();

        // Assert the table does not contain the test partition
        assert!(table
            .partition_data
            .read()
            .by_key(&PARTITION_KEY.into())
            .is_none());

        // Write some test data
        let action = table
            .buffer_table_write(
                SequenceNumber::new(42),
                batch,
                PARTITION_KEY.into(),
                &handle,
            )
            .await
            .expect("buffer op should succeed");
        assert_matches!(action, DmlApplyAction::Applied(false));

        // Referencing the partition should succeed
        assert!(table
            .partition_data
            .read()
            .by_key(&PARTITION_KEY.into())
            .is_some());

        // And the lifecycle handle was called with the expected values
        assert_eq!(
            handle.get_log_calls(),
            &[MockLifecycleCall {
                partition_id: PARTITION_ID,
                shard_id: SHARD_ID,
                namespace_id: NAMESPACE_ID,
                table_id: TABLE_ID,
                sequence_number: SequenceNumber::new(42),
                bytes_written: 1099,
                rows_written: 1,
            }]
        );

        // Attempt to buffer the second op that contains a type conflict - this
        // should return an error, and not make a call to the lifecycle handle
        // (as no data was buffered)
        //
        // Note the type of value was numeric previously, and here it is a string.
        let batch = lines_to_batches(r#"bananas,bat=man value="platanos" 42"#, 0)
            .unwrap()
            .remove(TABLE_NAME)
            .unwrap();

        let err = table
            .buffer_table_write(
                SequenceNumber::new(42),
                batch,
                PARTITION_KEY.into(),
                &handle,
            )
            .await
            .expect_err("type conflict should error");

        // The buffer op should return a column type error
        assert_matches!(
            err,
            Error::BufferWrite {
                source: mutable_batch::Error::WriterError {
                    source: writer::Error::TypeMismatch {
                        existing: InfluxColumnType::Field(InfluxFieldType::Float),
                        inserted: InfluxColumnType::Field(InfluxFieldType::String),
                        column: col_name,
                    }
                },
            } => { assert_eq!(col_name, "value") }
        );

        // And the lifecycle handle should not be called.
        //
        // It still contains the first call, so the desired length is 1
        // indicating no second call was made.
        assert_eq!(handle.get_log_calls().len(), 1);
    }
}
