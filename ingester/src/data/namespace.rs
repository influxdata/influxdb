//! Namespace level data buffer structures.

use std::{collections::HashMap, sync::Arc};

use data_types::{NamespaceId, PartitionKey, SequenceNumber, ShardId, TableId};
use dml::DmlOperation;
use iox_catalog::interface::Catalog;
use metric::U64Counter;
use observability_deps::tracing::warn;
use parking_lot::RwLock;
use snafu::ResultExt;
use write_summary::ShardProgress;

#[cfg(test)]
use super::triggers::TestTriggers;
use super::{
    partition::resolver::PartitionProvider,
    table::{TableData, TableName},
};
use crate::{data::DmlApplyAction, lifecycle::LifecycleHandle};

/// A double-referenced map where [`TableData`] can be looked up by name, or ID.
#[derive(Debug, Default)]
struct DoubleRef {
    // TODO(4880): this can be removed when IDs are sent over the wire.
    by_name: HashMap<TableName, Arc<tokio::sync::RwLock<TableData>>>,
    by_id: HashMap<TableId, Arc<tokio::sync::RwLock<TableData>>>,
}

impl DoubleRef {
    fn insert(&mut self, t: TableData) -> Arc<tokio::sync::RwLock<TableData>> {
        let name = t.table_name().clone();
        let id = t.table_id();

        let t = Arc::new(tokio::sync::RwLock::new(t));
        self.by_name.insert(name, Arc::clone(&t));
        self.by_id.insert(id, Arc::clone(&t));
        t
    }

    fn by_name(&self, name: &TableName) -> Option<Arc<tokio::sync::RwLock<TableData>>> {
        self.by_name.get(name).map(Arc::clone)
    }

    fn by_id(&self, id: TableId) -> Option<Arc<tokio::sync::RwLock<TableData>>> {
        self.by_id.get(&id).map(Arc::clone)
    }
}

/// The string name / identifier of a Namespace.
///
/// A reference-counted, cheap clone-able string.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct NamespaceName(Arc<str>);

impl<T> From<T> for NamespaceName
where
    T: AsRef<str>,
{
    fn from(v: T) -> Self {
        Self(Arc::from(v.as_ref()))
    }
}

impl std::ops::Deref for NamespaceName {
    type Target = Arc<str>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Display for NamespaceName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// Data of a Namespace that belongs to a given Shard
#[derive(Debug)]
pub(crate) struct NamespaceData {
    namespace_id: NamespaceId,
    namespace_name: NamespaceName,

    /// The catalog ID of the shard this namespace is being populated from.
    shard_id: ShardId,

    tables: RwLock<DoubleRef>,
    table_count: U64Counter,

    /// The resolver of `(shard_id, table_id, partition_key)` to
    /// [`PartitionData`].
    ///
    /// [`PartitionData`]: super::partition::PartitionData
    partition_provider: Arc<dyn PartitionProvider>,

    /// The sequence number being actively written, if any.
    ///
    /// This is used to know when a sequence number is only partially
    /// buffered for readability reporting. For example, in the
    /// following diagram a write for SequenceNumber 10 is only
    /// partially readable because it has been written into partitions
    /// A and B but not yet C. The max buffered number on each
    /// PartitionData is not sufficient to determine if the write is
    /// complete.
    ///
    /// ```text
    /// ╔═══════════════════════════════════════════════╗
    /// ║                                               ║   DML Operation (write)
    /// ║  ┏━━━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━━━━━┓  ║   SequenceNumber = 10
    /// ║  ┃ Data for C  ┃ Data for B  ┃ Data for A  ┃  ║
    /// ║  ┗━━━━━━━━━━━━━┻━━━━━━━━━━━━━┻━━━━━━━━━━━━━┛  ║
    /// ║         │             │             │         ║
    /// ╚═══════════════════════╬═════════════╬═════════╝
    ///           │             │             │           ┌──────────────────────────────────┐
    ///                         │             │           │           Partition A            │
    ///           │             │             └──────────▶│        max buffered = 10         │
    ///                         │                         └──────────────────────────────────┘
    ///           │             │
    ///                         │                         ┌──────────────────────────────────┐
    ///           │             │                         │           Partition B            │
    ///                         └────────────────────────▶│        max buffered = 10         │
    ///           │                                       └──────────────────────────────────┘
    ///
    ///           │
    ///                                                   ┌──────────────────────────────────┐
    ///           │                                       │           Partition C            │
    ///            ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ▶│         max buffered = 7         │
    ///                                                   └──────────────────────────────────┘
    ///           Write is partially buffered. It has been
    ///            written to Partitions A and B, but not
    ///                  yet written to Partition C
    ///                                                               PartitionData
    ///                                                       (Ingester state per partition)
    ///```
    buffering_sequence_number: RwLock<Option<SequenceNumber>>,

    /// Control the flow of ingest, for testing purposes
    #[cfg(test)]
    pub(crate) test_triggers: TestTriggers,
}

impl NamespaceData {
    /// Initialize new tables with default partition template of daily
    pub(super) fn new(
        namespace_id: NamespaceId,
        namespace_name: NamespaceName,
        shard_id: ShardId,
        partition_provider: Arc<dyn PartitionProvider>,
        metrics: &metric::Registry,
    ) -> Self {
        let table_count = metrics
            .register_metric::<U64Counter>(
                "ingester_tables_total",
                "Number of tables known to the ingester",
            )
            .recorder(&[]);

        Self {
            namespace_id,
            namespace_name,
            shard_id,
            tables: Default::default(),
            table_count,
            buffering_sequence_number: RwLock::new(None),
            partition_provider,
            #[cfg(test)]
            test_triggers: TestTriggers::new(),
        }
    }

    /// Buffer the operation in the cache, adding any new partitions or delete tombstones to the
    /// catalog. Returns true if ingest should be paused due to memory limits set in the passed
    /// lifecycle manager.
    pub(super) async fn buffer_operation(
        &self,
        dml_operation: DmlOperation,
        catalog: &Arc<dyn Catalog>,
        lifecycle_handle: &dyn LifecycleHandle,
    ) -> Result<DmlApplyAction, super::Error> {
        let sequence_number = dml_operation
            .meta()
            .sequence()
            .expect("must have sequence number")
            .sequence_number;

        // Note that this namespace is actively writing this sequence
        // number. Since there is no namespace wide lock held during a
        // write, this number is used to detect and update reported
        // progress during a write
        let _sequence_number_guard =
            ScopedSequenceNumber::new(sequence_number, &self.buffering_sequence_number);

        match dml_operation {
            DmlOperation::Write(write) => {
                let mut pause_writes = false;
                let mut all_skipped = true;

                // Extract the partition key derived by the router.
                let partition_key = write
                    .partition_key()
                    .expect("no partition key in dml write")
                    .clone();

                for (t, b) in write.into_tables() {
                    let t = TableName::from(t);
                    let table_data = match self.table_data(&t) {
                        Some(t) => t,
                        None => self.insert_table(&t, catalog).await?,
                    };

                    {
                        // lock scope
                        let mut table_data = table_data.write().await;
                        let action = table_data
                            .buffer_table_write(
                                sequence_number,
                                b,
                                partition_key.clone(),
                                lifecycle_handle,
                            )
                            .await?;
                        if let DmlApplyAction::Applied(should_pause) = action {
                            pause_writes = pause_writes || should_pause;
                            all_skipped = false;
                        }
                    }
                    #[cfg(test)]
                    self.test_triggers.on_write().await;
                }

                if all_skipped {
                    Ok(DmlApplyAction::Skipped)
                } else {
                    // at least some were applied
                    Ok(DmlApplyAction::Applied(pause_writes))
                }
            }
            DmlOperation::Delete(delete) => {
                // Deprecated delete support:
                // https://github.com/influxdata/influxdb_iox/issues/5825
                warn!(
                    shard_id=%self.shard_id,
                    namespace_name=%self.namespace_name,
                    namespace_id=%self.namespace_id,
                    table_name=?delete.table_name(),
                    sequence_number=?delete.meta().sequence(),
                    "discarding unsupported delete op"
                );

                Ok(DmlApplyAction::Applied(false))
            }
        }
    }

    /// Snapshots the mutable buffer for the partition, which clears it out and moves it over to
    /// snapshots. Then return a vec of the snapshots and the optional persisting batch.
    #[cfg(test)] // Only used in tests
    pub(crate) async fn snapshot(
        &self,
        table_name: &TableName,
        partition_key: &PartitionKey,
    ) -> Option<(
        Vec<Arc<super::partition::SnapshotBatch>>,
        Option<Arc<super::partition::PersistingBatch>>,
    )> {
        if let Some(t) = self.table_data(table_name) {
            let mut t = t.write().await;

            return t.get_partition_by_key_mut(partition_key).map(|p| {
                p.data
                    .generate_snapshot()
                    .expect("snapshot on mutable batch should never fail");
                (p.data.snapshots.to_vec(), p.data.persisting.clone())
            });
        }

        None
    }

    /// Snapshots the mutable buffer for the partition, which clears it out and then moves all
    /// snapshots over to a persisting batch, which is returned. If there is no data to snapshot
    /// or persist, None will be returned.
    #[cfg(test)] // Only used in tests
    pub(crate) async fn snapshot_to_persisting(
        &self,
        table_name: &TableName,
        partition_key: &PartitionKey,
    ) -> Option<Arc<super::partition::PersistingBatch>> {
        if let Some(table_data) = self.table_data(table_name) {
            let mut table_data = table_data.write().await;

            return table_data
                .get_partition_by_key_mut(partition_key)
                .and_then(|partition_data| partition_data.snapshot_to_persisting_batch());
        }

        None
    }

    /// Gets the buffered table data
    pub(crate) fn table_data(
        &self,
        table_name: &TableName,
    ) -> Option<Arc<tokio::sync::RwLock<TableData>>> {
        let t = self.tables.read();
        t.by_name(table_name)
    }

    /// Return the table data by ID.
    pub(crate) fn table_id(
        &self,
        table_id: TableId,
    ) -> Option<Arc<tokio::sync::RwLock<TableData>>> {
        let t = self.tables.read();
        t.by_id(table_id)
    }

    /// Inserts the table or returns it if it happens to be inserted by some other thread
    async fn insert_table(
        &self,
        table_name: &TableName,
        catalog: &Arc<dyn Catalog>,
    ) -> Result<Arc<tokio::sync::RwLock<TableData>>, super::Error> {
        let mut repos = catalog.repositories().await;

        let table_id = repos
            .tables()
            .get_by_namespace_and_name(self.namespace_id, table_name)
            .await
            .context(super::CatalogSnafu)?
            .ok_or_else(|| super::Error::TableNotFound {
                table_name: table_name.to_string(),
            })?
            .id;

        let mut t = self.tables.write();

        Ok(match t.by_name(table_name) {
            Some(v) => v,
            None => {
                self.table_count.inc(1);

                // Insert the table and then return a ref to it.
                t.insert(TableData::new(
                    table_id,
                    table_name.clone(),
                    self.shard_id,
                    self.namespace_id,
                    Arc::clone(&self.partition_provider),
                ))
            }
        })
    }

    /// Walks down the table and partition and clears the persisting batch. The sequence number is
    /// the max_sequence_number for the persisted parquet file, which should be kept in the table
    /// data buffer.
    pub(super) async fn mark_persisted(
        &self,
        table_name: &TableName,
        partition_key: &PartitionKey,
        sequence_number: SequenceNumber,
    ) {
        if let Some(t) = self.table_data(table_name) {
            let mut t = t.write().await;
            let partition = t.get_partition_by_key_mut(partition_key);

            if let Some(p) = partition {
                p.mark_persisted(sequence_number);
            }
        }
    }

    /// Return progress from this Namespace
    pub(super) async fn progress(&self) -> ShardProgress {
        let tables: Vec<_> = self.tables.read().by_id.values().map(Arc::clone).collect();

        // Consolidate progtress across partitions.
        let mut progress = ShardProgress::new()
            // Properly account for any sequence number that is
            // actively buffering and thus not yet completely
            // readable.
            .actively_buffering(*self.buffering_sequence_number.read());

        for table_data in tables {
            progress = progress.combine(table_data.read().await.progress())
        }
        progress
    }

    /// Return the [`NamespaceId`] this [`NamespaceData`] belongs to.
    pub(super) fn namespace_id(&self) -> NamespaceId {
        self.namespace_id
    }

    #[cfg(test)]
    pub(super) fn table_count(&self) -> &U64Counter {
        &self.table_count
    }

    /// Returns the [`NamespaceName`] for this namespace.
    pub(crate) fn namespace_name(&self) -> &NamespaceName {
        &self.namespace_name
    }
}

/// RAAI struct that sets buffering sequence number on creation and clears it on free
struct ScopedSequenceNumber<'a> {
    sequence_number: SequenceNumber,
    buffering_sequence_number: &'a RwLock<Option<SequenceNumber>>,
}

impl<'a> ScopedSequenceNumber<'a> {
    fn new(
        sequence_number: SequenceNumber,
        buffering_sequence_number: &'a RwLock<Option<SequenceNumber>>,
    ) -> Self {
        *buffering_sequence_number.write() = Some(sequence_number);

        Self {
            sequence_number,
            buffering_sequence_number,
        }
    }
}

impl<'a> Drop for ScopedSequenceNumber<'a> {
    fn drop(&mut self) {
        // clear write on drop
        let mut buffering_sequence_number = self.buffering_sequence_number.write();
        assert_eq!(
            *buffering_sequence_number,
            Some(self.sequence_number),
            "multiple operations are being buffered concurrently"
        );
        *buffering_sequence_number = None;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use data_types::{PartitionId, ShardIndex};
    use metric::{Attributes, Metric};

    use crate::{
        data::partition::{resolver::MockPartitionProvider, PartitionData, SortKeyState},
        lifecycle::mock_handle::MockLifecycleHandle,
        test_util::{make_write_op, populate_catalog},
    };

    use super::*;

    const SHARD_INDEX: ShardIndex = ShardIndex::new(24);
    const TABLE_NAME: &str = "bananas";
    const NAMESPACE_NAME: &str = "platanos";

    #[tokio::test]
    async fn test_namespace_double_ref() {
        let metrics = Arc::new(metric::Registry::default());
        let catalog: Arc<dyn Catalog> =
            Arc::new(iox_catalog::mem::MemCatalog::new(Arc::clone(&metrics)));

        // Populate the catalog with the shard / namespace / table
        let (shard_id, ns_id, table_id) =
            populate_catalog(&*catalog, SHARD_INDEX, NAMESPACE_NAME, TABLE_NAME).await;

        // Configure the mock partition provider to return a partition for this
        // table ID.
        let partition_provider = Arc::new(MockPartitionProvider::default().with_partition(
            PartitionData::new(
                PartitionId::new(0),
                PartitionKey::from("banana-split"),
                shard_id,
                ns_id,
                table_id,
                TABLE_NAME.into(),
                SortKeyState::Provided(None),
                None,
            ),
        ));

        let ns = NamespaceData::new(
            ns_id,
            NAMESPACE_NAME.into(),
            shard_id,
            partition_provider,
            &*metrics,
        );

        // Assert the namespace name was stored
        assert_eq!(ns.namespace_name().to_string(), NAMESPACE_NAME);

        // Assert the namespace does not contain the test data
        assert!(ns.table_data(&TABLE_NAME.into()).is_none());
        assert!(ns.table_id(table_id).is_none());

        // Write some test data
        ns.buffer_operation(
            DmlOperation::Write(make_write_op(
                &PartitionKey::from("banana-split"),
                SHARD_INDEX,
                NAMESPACE_NAME,
                0,
                r#"bananas,city=Medford day="sun",temp=55 22"#,
            )),
            &catalog,
            &MockLifecycleHandle::default(),
        )
        .await
        .expect("buffer op should succeed");

        // Both forms of referencing the table should succeed
        assert!(ns.table_data(&TABLE_NAME.into()).is_some());
        assert!(ns.table_id(table_id).is_some());

        // And the table counter metric should increase
        let tables = metrics
            .get_instrument::<Metric<U64Counter>>("ingester_tables_total")
            .expect("failed to read metric")
            .get_observer(&Attributes::from([]))
            .expect("failed to get observer")
            .fetch();
        assert_eq!(tables, 1);
    }
}
