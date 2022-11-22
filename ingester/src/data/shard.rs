//! Shard level data buffer structures.

use std::sync::Arc;

use data_types::{NamespaceId, ShardId, ShardIndex};
use dml::DmlOperation;
use write_summary::ShardProgress;

use super::DmlApplyAction;
use crate::{
    buffer_tree::{
        namespace::{name_resolver::NamespaceNameProvider, NamespaceData},
        partition::resolver::PartitionProvider,
        table::name_resolver::TableNameProvider,
        BufferTree,
    },
    lifecycle::LifecycleHandle,
};

/// Data of a Shard
#[derive(Debug)]
pub(crate) struct ShardData {
    /// The shard index for this shard
    shard_index: ShardIndex,
    buffer_tree: BufferTree,
}

impl ShardData {
    /// Initialise a new [`ShardData`] that emits metrics to `metrics`.
    pub(crate) fn new(
        shard_index: ShardIndex,
        shard_id: ShardId,
        namespace_name_resolver: Arc<dyn NamespaceNameProvider>,
        table_name_resolver: Arc<dyn TableNameProvider>,
        partition_provider: Arc<dyn PartitionProvider>,
        metrics: Arc<metric::Registry>,
    ) -> Self {
        let buffer_tree = BufferTree::new(
            namespace_name_resolver,
            table_name_resolver,
            partition_provider,
            metrics,
            shard_id,
        );

        Self {
            shard_index,
            buffer_tree,
        }
    }

    /// Buffer the provided [`DmlOperation`] into the ingester state.
    pub(super) async fn buffer_operation(
        &self,
        dml_operation: DmlOperation,
        lifecycle_handle: &dyn LifecycleHandle,
    ) -> Result<DmlApplyAction, super::Error> {
        self.buffer_tree
            .buffer_operation(dml_operation, lifecycle_handle)
            .await
    }

    /// Gets the namespace data out of the map
    pub(crate) fn namespace(&self, namespace_id: NamespaceId) -> Option<Arc<NamespaceData>> {
        self.buffer_tree.namespace(namespace_id)
    }

    /// Return the progress of this shard
    pub(super) async fn progress(&self) -> ShardProgress {
        self.buffer_tree.progress().await
    }

    /// Return the [`ShardIndex`] this [`ShardData`] is buffering for.
    pub(super) fn shard_index(&self) -> ShardIndex {
        self.shard_index
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use data_types::{PartitionId, PartitionKey, ShardIndex, TableId};
    use metric::{Attributes, Metric, U64Counter};

    use super::*;
    use crate::{
        buffer_tree::{
            namespace::name_resolver::mock::MockNamespaceNameProvider,
            partition::{resolver::MockPartitionProvider, PartitionData, SortKeyState},
            table::{name_resolver::mock::MockTableNameProvider, TableName},
        },
        deferred_load::DeferredLoad,
        lifecycle::mock_handle::MockLifecycleHandle,
        test_util::{make_write_op, TEST_TABLE},
    };

    const SHARD_INDEX: ShardIndex = ShardIndex::new(24);
    const SHARD_ID: ShardId = ShardId::new(22);
    const TABLE_NAME: &str = TEST_TABLE;
    const TABLE_ID: TableId = TableId::new(44);
    const NAMESPACE_NAME: &str = "platanos";
    const NAMESPACE_ID: NamespaceId = NamespaceId::new(42);

    #[tokio::test]
    async fn test_shard_init_namespace() {
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

        let shard = ShardData::new(
            SHARD_INDEX,
            SHARD_ID,
            Arc::new(MockNamespaceNameProvider::new(NAMESPACE_NAME)),
            Arc::new(MockTableNameProvider::new(TABLE_NAME)),
            partition_provider,
            Arc::clone(&metrics),
        );

        // Assert the namespace does not contain the test data
        assert!(shard.namespace(NAMESPACE_ID).is_none());

        // Write some test data
        shard
            .buffer_operation(
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

        assert!(shard.namespace(NAMESPACE_ID).is_some());

        // And the table counter metric should increase
        let tables = metrics
            .get_instrument::<Metric<U64Counter>>("ingester_namespaces")
            .expect("failed to read metric")
            .get_observer(&Attributes::from([]))
            .expect("failed to get observer")
            .fetch();
        assert_eq!(tables, 1);
    }
}
