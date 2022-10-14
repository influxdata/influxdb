//! Shard level data buffer structures.

use std::{collections::HashMap, sync::Arc};

use data_types::{NamespaceId, ShardId, ShardIndex};
use dml::DmlOperation;
use iox_catalog::interface::Catalog;
use metric::U64Counter;
use parking_lot::RwLock;
use snafu::{OptionExt, ResultExt};
use write_summary::ShardProgress;

use super::{
    namespace::{NamespaceData, NamespaceName},
    partition::resolver::PartitionProvider,
    DmlApplyAction,
};
use crate::lifecycle::LifecycleHandle;

/// A double-referenced map where [`NamespaceData`] can be looked up by name, or
/// ID.
#[derive(Debug, Default)]
struct DoubleRef {
    // TODO(4880): this can be removed when IDs are sent over the wire.
    by_name: HashMap<NamespaceName, Arc<NamespaceData>>,
    by_id: HashMap<NamespaceId, Arc<NamespaceData>>,
}

impl DoubleRef {
    fn insert(&mut self, name: NamespaceName, ns: NamespaceData) -> Arc<NamespaceData> {
        let id = ns.namespace_id();

        let ns = Arc::new(ns);
        self.by_name.insert(name, Arc::clone(&ns));
        self.by_id.insert(id, Arc::clone(&ns));
        ns
    }

    fn by_name(&self, name: &NamespaceName) -> Option<Arc<NamespaceData>> {
        self.by_name.get(name).map(Arc::clone)
    }

    fn by_id(&self, id: NamespaceId) -> Option<Arc<NamespaceData>> {
        self.by_id.get(&id).map(Arc::clone)
    }
}

/// Data of a Shard
#[derive(Debug)]
pub(crate) struct ShardData {
    /// The shard index for this shard
    shard_index: ShardIndex,
    /// The catalog ID for this shard.
    shard_id: ShardId,

    /// The resolver of `(shard_id, table_id, partition_key)` to
    /// [`PartitionData`].
    ///
    /// [`PartitionData`]: super::partition::PartitionData
    partition_provider: Arc<dyn PartitionProvider>,

    // New namespaces can come in at any time so we need to be able to add new ones
    namespaces: RwLock<DoubleRef>,

    metrics: Arc<metric::Registry>,
    namespace_count: U64Counter,
}

impl ShardData {
    /// Initialise a new [`ShardData`] that emits metrics to `metrics`.
    pub(crate) fn new(
        shard_index: ShardIndex,
        shard_id: ShardId,
        partition_provider: Arc<dyn PartitionProvider>,
        metrics: Arc<metric::Registry>,
    ) -> Self {
        let namespace_count = metrics
            .register_metric::<U64Counter>(
                "ingester_namespaces_total",
                "Number of namespaces known to the ingester",
            )
            .recorder(&[]);

        Self {
            shard_index,
            shard_id,
            namespaces: Default::default(),
            metrics,
            partition_provider,
            namespace_count,
        }
    }

    /// Store the write or delete in the shard. Deletes will
    /// be written into the catalog before getting stored in the buffer.
    /// Any writes that create new IOx partitions will have those records
    /// created in the catalog before putting into the buffer.
    pub(super) async fn buffer_operation(
        &self,
        dml_operation: DmlOperation,
        catalog: &Arc<dyn Catalog>,
        lifecycle_handle: &dyn LifecycleHandle,
    ) -> Result<DmlApplyAction, super::Error> {
        let namespace_data = match self.namespace(&NamespaceName::from(dml_operation.namespace())) {
            Some(d) => d,
            None => {
                self.insert_namespace(dml_operation.namespace(), &**catalog)
                    .await?
            }
        };

        namespace_data
            .buffer_operation(dml_operation, catalog, lifecycle_handle)
            .await
    }

    /// Gets the namespace data out of the map
    pub(crate) fn namespace(&self, namespace: &NamespaceName) -> Option<Arc<NamespaceData>> {
        let n = self.namespaces.read();
        n.by_name(namespace)
    }

    /// Gets the namespace data out of the map
    pub(crate) fn namespace_by_id(&self, namespace_id: NamespaceId) -> Option<Arc<NamespaceData>> {
        // TODO: this should be the default once IDs are pushed over the wire.
        //
        // At which point the map should be indexed by IDs, instead of namespace
        // names.
        let n = self.namespaces.read();
        n.by_id(namespace_id)
    }

    /// Retrieves the namespace from the catalog and initializes an empty buffer, or
    /// retrieves the buffer if some other caller gets it first
    async fn insert_namespace(
        &self,
        namespace: &str,
        catalog: &dyn Catalog,
    ) -> Result<Arc<NamespaceData>, super::Error> {
        let mut repos = catalog.repositories().await;

        let ns_name = NamespaceName::from(namespace);
        let namespace = repos
            .namespaces()
            .get_by_name(namespace)
            .await
            .context(super::CatalogSnafu)?
            .context(super::NamespaceNotFoundSnafu { namespace })?;

        let mut n = self.namespaces.write();

        Ok(match n.by_name(&ns_name) {
            Some(v) => v,
            None => {
                self.namespace_count.inc(1);

                // Insert the table and then return a ref to it.
                n.insert(
                    ns_name.clone(),
                    NamespaceData::new(
                        namespace.id,
                        ns_name,
                        self.shard_id,
                        Arc::clone(&self.partition_provider),
                        &*self.metrics,
                    ),
                )
            }
        })
    }

    /// Return the progress of this shard
    pub(super) async fn progress(&self) -> ShardProgress {
        let namespaces: Vec<_> = self
            .namespaces
            .read()
            .by_id
            .values()
            .map(Arc::clone)
            .collect();

        let mut progress = ShardProgress::new();

        for namespace_data in namespaces {
            progress = progress.combine(namespace_data.progress().await);
        }
        progress
    }

    /// Return the [`ShardIndex`] this [`ShardData`] is buffering for.
    pub(super) fn shard_index(&self) -> ShardIndex {
        self.shard_index
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use data_types::{PartitionId, PartitionKey, ShardIndex};
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
    async fn test_shard_double_ref() {
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

        let shard = ShardData::new(
            SHARD_INDEX,
            shard_id,
            partition_provider,
            Arc::clone(&metrics),
        );

        // Assert the namespace does not contain the test data
        assert!(shard.namespace(&NAMESPACE_NAME.into()).is_none());
        assert!(shard.namespace_by_id(ns_id).is_none());

        // Write some test data
        shard
            .buffer_operation(
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
        assert!(shard.namespace(&NAMESPACE_NAME.into()).is_some());
        assert!(shard.namespace_by_id(ns_id).is_some());

        // And the table counter metric should increase
        let tables = metrics
            .get_instrument::<Metric<U64Counter>>("ingester_namespaces_total")
            .expect("failed to read metric")
            .get_observer(&Attributes::from([]))
            .expect("failed to get observer")
            .fetch();
        assert_eq!(tables, 1);
    }
}
