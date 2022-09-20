//! Shard level data buffer structures.

use std::{
    collections::{btree_map::Entry, BTreeMap},
    sync::Arc,
};

use data_types::{ShardId, ShardIndex};
use dml::DmlOperation;
use iox_catalog::interface::Catalog;
use iox_query::exec::Executor;
use metric::U64Counter;
use parking_lot::RwLock;
use snafu::{OptionExt, ResultExt};
use write_summary::ShardProgress;

use super::namespace::NamespaceData;
use crate::lifecycle::LifecycleHandle;

/// Data of a Shard
#[derive(Debug)]
pub struct ShardData {
    /// The shard index for this shard
    shard_index: ShardIndex,
    /// The catalog ID for this shard.
    shard_id: ShardId,

    // New namespaces can come in at any time so we need to be able to add new ones
    namespaces: RwLock<BTreeMap<String, Arc<NamespaceData>>>,

    metrics: Arc<metric::Registry>,
    namespace_count: U64Counter,
}

impl ShardData {
    /// Initialise a new [`ShardData`] that emits metrics to `metrics`.
    pub fn new(shard_index: ShardIndex, shard_id: ShardId, metrics: Arc<metric::Registry>) -> Self {
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
        catalog: &dyn Catalog,
        lifecycle_handle: &dyn LifecycleHandle,
        executor: &Executor,
    ) -> Result<bool, super::Error> {
        let namespace_data = match self.namespace(dml_operation.namespace()) {
            Some(d) => d,
            None => {
                self.insert_namespace(dml_operation.namespace(), catalog)
                    .await?
            }
        };

        namespace_data
            .buffer_operation(dml_operation, catalog, lifecycle_handle, executor)
            .await
    }

    /// Gets the namespace data out of the map
    pub(crate) fn namespace(&self, namespace: &str) -> Option<Arc<NamespaceData>> {
        let n = self.namespaces.read();
        n.get(namespace).cloned()
    }

    /// Retrieves the namespace from the catalog and initializes an empty buffer, or
    /// retrieves the buffer if some other caller gets it first
    async fn insert_namespace(
        &self,
        namespace: &str,
        catalog: &dyn Catalog,
    ) -> Result<Arc<NamespaceData>, super::Error> {
        let mut repos = catalog.repositories().await;
        let namespace = repos
            .namespaces()
            .get_by_name(namespace)
            .await
            .context(super::CatalogSnafu)?
            .context(super::NamespaceNotFoundSnafu { namespace })?;

        let mut n = self.namespaces.write();

        let data = match n.entry(namespace.name) {
            Entry::Vacant(v) => {
                let v = v.insert(Arc::new(NamespaceData::new(
                    namespace.id,
                    self.shard_id,
                    &*self.metrics,
                )));
                self.namespace_count.inc(1);
                Arc::clone(v)
            }
            Entry::Occupied(v) => Arc::clone(v.get()),
        };

        Ok(data)
    }

    /// Return the progress of this shard
    pub(crate) async fn progress(&self) -> ShardProgress {
        let namespaces: Vec<_> = self.namespaces.read().values().map(Arc::clone).collect();

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
