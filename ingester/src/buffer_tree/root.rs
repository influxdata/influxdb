use std::sync::Arc;

use data_types::{NamespaceId, ShardId};
use dml::DmlOperation;
use metric::U64Counter;
use write_summary::ShardProgress;

use super::{
    namespace::{name_resolver::NamespaceNameProvider, NamespaceData},
    partition::resolver::PartitionProvider,
    table::name_resolver::TableNameProvider,
};
use crate::{arcmap::ArcMap, data::DmlApplyAction, lifecycle::LifecycleHandle};

#[derive(Debug)]
pub(crate) struct BufferTree {
    /// The resolver of `(table_id, partition_key)` to [`PartitionData`].
    ///
    /// [`PartitionData`]: super::partition::PartitionData
    partition_provider: Arc<dyn PartitionProvider>,

    /// A set of namespaces this [`BufferTree`] instance has processed
    /// [`DmlOperation`]'s for.
    ///
    /// The [`NamespaceNameProvider`] acts as a [`DeferredLoad`] constructor to
    /// resolve the [`NamespaceName`] for new [`NamespaceData`] out of the hot
    /// path.
    ///
    /// [`DeferredLoad`]: crate::deferred_load::DeferredLoad
    /// [`NamespaceName`]: data_types::NamespaceName
    namespaces: ArcMap<NamespaceId, NamespaceData>,
    namespace_name_resolver: Arc<dyn NamespaceNameProvider>,
    /// The [`TableName`] provider used by [`NamespaceData`] to initialise a
    /// [`TableData`].
    ///
    /// [`TableName`]: crate::buffer_tree::table::TableName
    /// [`TableData`]: crate::buffer_tree::table::TableData
    table_name_resolver: Arc<dyn TableNameProvider>,

    metrics: Arc<metric::Registry>,
    namespace_count: U64Counter,

    // TODO: the concept of a shard should not be pushed into a generic
    // BufferTree.
    //
    // This exists temporarily to satisfy current needs, and will be removed in
    // the near future.
    shard_id: ShardId,
}

impl BufferTree {
    /// Initialise a new [`BufferTree`] that emits metrics to `metrics`.
    pub(crate) fn new(
        namespace_name_resolver: Arc<dyn NamespaceNameProvider>,
        table_name_resolver: Arc<dyn TableNameProvider>,
        partition_provider: Arc<dyn PartitionProvider>,
        metrics: Arc<metric::Registry>,
        shard_id: ShardId,
    ) -> Self {
        let namespace_count = metrics
            .register_metric::<U64Counter>(
                "ingester_namespaces",
                "Number of namespaces known to the ingester",
            )
            .recorder(&[]);

        Self {
            namespaces: Default::default(),
            namespace_name_resolver,
            table_name_resolver,
            metrics,
            partition_provider,
            namespace_count,
            shard_id,
        }
    }

    /// Buffer the provided [`DmlOperation`] into the ingester state.
    pub(crate) async fn buffer_operation(
        &self,
        dml_operation: DmlOperation,
        lifecycle_handle: &dyn LifecycleHandle,
    ) -> Result<DmlApplyAction, crate::data::Error> {
        let namespace_id = dml_operation.namespace_id();
        let namespace_data = self.namespaces.get_or_insert_with(&namespace_id, || {
            // Increase the metric that records the number of namespaces
            // buffered in this ingester instance.
            self.namespace_count.inc(1);

            Arc::new(NamespaceData::new(
                namespace_id,
                self.namespace_name_resolver.for_namespace(namespace_id),
                Arc::clone(&self.table_name_resolver),
                self.shard_id,
                Arc::clone(&self.partition_provider),
                &self.metrics,
            ))
        });

        namespace_data
            .buffer_operation(dml_operation, lifecycle_handle)
            .await
    }

    /// Gets the namespace data out of the map
    pub(crate) fn namespace(&self, namespace_id: NamespaceId) -> Option<Arc<NamespaceData>> {
        self.namespaces.get(&namespace_id)
    }

    /// Return the progress of this shard
    pub(crate) async fn progress(&self) -> ShardProgress {
        let namespaces: Vec<_> = self.namespaces.values();

        let mut progress = ShardProgress::new();

        for namespace_data in namespaces {
            progress = progress.combine(namespace_data.progress().await);
        }
        progress
    }
}
