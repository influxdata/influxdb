//! Namespace level data buffer structures.

pub(crate) mod name_resolver;

use std::sync::Arc;

use data_types::{NamespaceId, SequenceNumber, ShardId, TableId};
use dml::DmlOperation;
use metric::U64Counter;
use observability_deps::tracing::warn;
use parking_lot::RwLock;
use write_summary::ShardProgress;

use super::{
    partition::resolver::PartitionProvider,
    table::{name_resolver::TableNameProvider, TableData},
};
#[cfg(test)]
use crate::data::triggers::TestTriggers;
use crate::{
    arcmap::ArcMap, data::DmlApplyAction, deferred_load::DeferredLoad, lifecycle::LifecycleHandle,
};

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
    namespace_name: DeferredLoad<NamespaceName>,

    /// The catalog ID of the shard this namespace is being populated from.
    shard_id: ShardId,

    /// A set of tables this [`NamespaceData`] instance has processed
    /// [`DmlOperation`]'s for.
    ///
    /// The [`TableNameProvider`] acts as a [`DeferredLoad`] constructor to
    /// resolve the [`TableName`] for new [`TableData`] out of the hot path.
    ///
    /// [`TableName`]: crate::buffer_tree::table::TableName
    tables: ArcMap<TableId, TableData>,
    table_name_resolver: Arc<dyn TableNameProvider>,
    /// The count of tables initialised in this Ingester so far, across all
    /// shards / namespaces.
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
    // TODO(kafkaless): pub(super)
    pub(crate) fn new(
        namespace_id: NamespaceId,
        namespace_name: DeferredLoad<NamespaceName>,
        table_name_resolver: Arc<dyn TableNameProvider>,
        shard_id: ShardId,
        partition_provider: Arc<dyn PartitionProvider>,
        metrics: &metric::Registry,
    ) -> Self {
        let table_count = metrics
            .register_metric::<U64Counter>(
                "ingester_tables",
                "Number of tables known to the ingester",
            )
            .recorder(&[]);

        Self {
            namespace_id,
            namespace_name,
            shard_id,
            tables: Default::default(),
            table_name_resolver,
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
    pub(crate) async fn buffer_operation(
        &self,
        dml_operation: DmlOperation,
        lifecycle_handle: &dyn LifecycleHandle,
    ) -> Result<DmlApplyAction, crate::data::Error> {
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
                let partition_key = write.partition_key().clone();

                for (table_id, b) in write.into_tables() {
                    // Grab a reference to the table data, or insert a new
                    // TableData for it.
                    let table_data = self.tables.get_or_insert_with(&table_id, || {
                        self.table_count.inc(1);
                        Arc::new(TableData::new(
                            table_id,
                            self.table_name_resolver.for_table(table_id),
                            self.shard_id,
                            self.namespace_id,
                            Arc::clone(&self.partition_provider),
                        ))
                    });

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

    /// Return the table data by ID.
    pub(crate) fn table(&self, table_id: TableId) -> Option<Arc<TableData>> {
        self.tables.get(&table_id)
    }

    /// Return progress from this Namespace
    // TODO(kafkaless): pub(super)
    pub(crate) async fn progress(&self) -> ShardProgress {
        let tables: Vec<_> = self.tables.values();

        // Consolidate progress across partitions.
        let mut progress = ShardProgress::new()
            // Properly account for any sequence number that is
            // actively buffering and thus not yet completely
            // readable.
            .actively_buffering(*self.buffering_sequence_number.read());

        for table_data in tables {
            progress = progress.combine(table_data.progress())
        }
        progress
    }

    /// Return the [`NamespaceId`] this [`NamespaceData`] belongs to.
    pub(crate) fn namespace_id(&self) -> NamespaceId {
        self.namespace_id
    }

    #[cfg(test)]
    pub(super) fn table_count(&self) -> &U64Counter {
        &self.table_count
    }

    /// Returns the [`NamespaceName`] for this namespace.
    pub(crate) fn namespace_name(&self) -> &DeferredLoad<NamespaceName> {
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
    use std::{sync::Arc, time::Duration};

    use assert_matches::assert_matches;
    use data_types::{
        ColumnId, ColumnSet, CompactionLevel, ParquetFileParams, PartitionId, PartitionKey,
        ShardIndex, Timestamp,
    };
    use iox_catalog::{interface::Catalog, mem::MemCatalog};
    use iox_time::SystemProvider;
    use metric::{Attributes, Metric, MetricObserver, Observation};
    use uuid::Uuid;

    use super::*;
    use crate::{
        buffer_tree::{
            namespace::NamespaceData,
            partition::{
                resolver::{CatalogPartitionResolver, MockPartitionProvider},
                PartitionData, SortKeyState,
            },
            table::{name_resolver::mock::MockTableNameProvider, TableName},
        },
        deferred_load::{self, DeferredLoad},
        lifecycle::{mock_handle::MockLifecycleHandle, LifecycleConfig, LifecycleManager},
        test_util::{make_write_op, TEST_TABLE},
    };

    const SHARD_INDEX: ShardIndex = ShardIndex::new(24);
    const SHARD_ID: ShardId = ShardId::new(22);
    const TABLE_NAME: &str = TEST_TABLE;
    const TABLE_ID: TableId = TableId::new(44);
    const NAMESPACE_NAME: &str = "platanos";
    const NAMESPACE_ID: NamespaceId = NamespaceId::new(42);

    #[tokio::test]
    async fn test_namespace_init_table() {
        let metrics = Arc::new(metric::Registry::default());

        // Configure the mock partition provider to return a partition for this
        // table ID.
        let partition_provider = Arc::new(MockPartitionProvider::default().with_partition(
            PartitionData::new(
                PartitionId::new(0),
                PartitionKey::from("banana-split"),
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

        let ns = NamespaceData::new(
            NAMESPACE_ID,
            DeferredLoad::new(Duration::from_millis(1), async { NAMESPACE_NAME.into() }),
            Arc::new(MockTableNameProvider::new(TABLE_NAME)),
            SHARD_ID,
            partition_provider,
            &metrics,
        );

        // Assert the namespace name was stored
        let name = ns.namespace_name().to_string();
        assert!(
            (name == NAMESPACE_NAME) || (name == deferred_load::UNRESOLVED_DISPLAY_STRING),
            "unexpected namespace name: {name}"
        );

        // Assert the namespace does not contain the test data
        assert!(ns.table(TABLE_ID).is_none());

        // Write some test data
        ns.buffer_operation(
            DmlOperation::Write(make_write_op(
                &PartitionKey::from("banana-split"),
                SHARD_INDEX,
                NAMESPACE_ID,
                TABLE_NAME,
                TABLE_ID,
                0,
                r#"test_table,city=Medford day="sun",temp=55 22"#,
            )),
            &MockLifecycleHandle::default(),
        )
        .await
        .expect("buffer op should succeed");

        // Referencing the table should succeed
        assert!(ns.table(TABLE_ID).is_some());

        // And the table counter metric should increase
        let tables = metrics
            .get_instrument::<Metric<U64Counter>>("ingester_tables")
            .expect("failed to read metric")
            .get_observer(&Attributes::from([]))
            .expect("failed to get observer")
            .fetch();
        assert_eq!(tables, 1);

        // Ensure the deferred namespace name is loaded.
        let name = ns.namespace_name().get().await;
        assert_eq!(&**name, NAMESPACE_NAME);
        assert_eq!(ns.namespace_name().to_string(), NAMESPACE_NAME);
    }

    #[tokio::test]
    async fn buffer_operation_ignores_already_persisted_data() {
        test_helpers::maybe_start_logging();
        let metrics = Arc::new(metric::Registry::new());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metrics)));
        let mut repos = catalog.repositories().await;

        let w1 = make_write_op(
            &PartitionKey::from("1970-01-01"),
            SHARD_INDEX,
            NAMESPACE_ID,
            TABLE_NAME,
            TABLE_ID,
            1,
            "test_table foo=1 10",
        );
        let w2 = make_write_op(
            &PartitionKey::from("1970-01-01"),
            SHARD_INDEX,
            NAMESPACE_ID,
            TABLE_NAME,
            TABLE_ID,
            2,
            "test_table foo=1 10",
        );

        // create some persisted state
        let partition = repos
            .partitions()
            .create_or_get("1970-01-01".into(), SHARD_ID, TABLE_ID)
            .await
            .unwrap();
        repos
            .partitions()
            .update_persisted_sequence_number(partition.id, SequenceNumber::new(1))
            .await
            .unwrap();
        let partition2 = repos
            .partitions()
            .create_or_get("1970-01-02".into(), SHARD_ID, TABLE_ID)
            .await
            .unwrap();

        let parquet_file_params = ParquetFileParams {
            shard_id: SHARD_ID,
            namespace_id: NAMESPACE_ID,
            table_id: TABLE_ID,
            partition_id: partition.id,
            object_store_id: Uuid::new_v4(),
            max_sequence_number: SequenceNumber::new(1),
            min_time: Timestamp::new(1),
            max_time: Timestamp::new(1),
            file_size_bytes: 0,
            row_count: 0,
            compaction_level: CompactionLevel::Initial,
            created_at: Timestamp::new(1),
            column_set: ColumnSet::new([ColumnId::new(1), ColumnId::new(2)]),
            max_l0_created_at: Timestamp::new(1),
        };
        repos
            .parquet_files()
            .create(parquet_file_params.clone())
            .await
            .unwrap();

        // now create a parquet file in another partition with a much higher sequence persisted
        // sequence number. We want to make sure that this doesn't cause our write in the other
        // partition to get ignored.
        let other_file_params = ParquetFileParams {
            max_sequence_number: SequenceNumber::new(15),
            object_store_id: Uuid::new_v4(),
            partition_id: partition2.id,
            ..parquet_file_params
        };
        repos
            .parquet_files()
            .create(other_file_params)
            .await
            .unwrap();
        std::mem::drop(repos);

        let manager = LifecycleManager::new(
            LifecycleConfig::new(
                1,
                0,
                0,
                Duration::from_secs(1),
                Duration::from_secs(1),
                1000000,
            ),
            Arc::clone(&metrics),
            Arc::new(SystemProvider::new()),
        );

        let partition_provider = Arc::new(CatalogPartitionResolver::new(Arc::clone(&catalog)));

        let data = NamespaceData::new(
            NAMESPACE_ID,
            DeferredLoad::new(Duration::from_millis(1), async { "foo".into() }),
            Arc::new(MockTableNameProvider::new(TABLE_NAME)),
            SHARD_ID,
            partition_provider,
            &metrics,
        );

        // w1 should be ignored because the per-partition replay offset is set
        // to 1 already, so it shouldn't be buffered and the buffer should
        // remain empty.
        let action = data
            .buffer_operation(DmlOperation::Write(w1), &manager.handle())
            .await
            .unwrap();
        {
            let table = data.table(TABLE_ID).unwrap();
            assert!(table
                .partitions()
                .into_iter()
                .all(|p| p.lock().get_query_data().is_none()));
            assert_eq!(
                table
                    .get_partition_by_key(&"1970-01-01".into())
                    .unwrap()
                    .lock()
                    .max_persisted_sequence_number(),
                Some(SequenceNumber::new(1))
            );
        }
        assert_matches!(action, DmlApplyAction::Skipped);

        // w2 should be in the buffer
        data.buffer_operation(DmlOperation::Write(w2), &manager.handle())
            .await
            .unwrap();

        let table = data.table(TABLE_ID).unwrap();
        let partition = table.get_partition_by_key(&"1970-01-01".into()).unwrap();
        assert_eq!(
            partition.lock().sequence_number_range().inclusive_min(),
            Some(SequenceNumber::new(2))
        );

        assert_matches!(data.table_count().observe(), Observation::U64Counter(v) => {
            assert_eq!(v, 1, "unexpected table count metric value");
        });
    }
}
