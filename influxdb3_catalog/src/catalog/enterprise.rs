use std::sync::Arc;

use anyhow::Context;
use iox_time::TimeProvider;
use object_store::ObjectStore;
use parking_lot::RwLock;
use tokio::sync::broadcast;

use crate::object_store::ObjectStoreCatalog;
use crate::{Result, log::NodeSpec};

use super::{
    CATALOG_BROADCAST_CHANNEL_CAPACITY, CATALOG_CHECKPOINT_INTERVAL, Catalog, NodeDefinition,
};

impl Catalog {
    pub async fn new_enterprise(
        current_node_id: impl Into<Arc<str>>,
        catalog_id: impl Into<Arc<str>>,
        store: Arc<dyn ObjectStore>,
        time_provider: Arc<dyn TimeProvider>,
    ) -> Result<Self> {
        let current_node_id = Some(current_node_id.into());
        let object_store_prefix = catalog_id.into();
        let store = ObjectStoreCatalog::new(
            Arc::clone(&object_store_prefix),
            CATALOG_CHECKPOINT_INTERVAL,
            store,
        );
        let (channel, _) = broadcast::channel(CATALOG_BROADCAST_CHANNEL_CAPACITY);
        store
            .load_or_create_catalog()
            .await
            .map_err(Into::into)
            .map(RwLock::new)
            .map(|inner| Self {
                current_node_id,
                channel,
                time_provider,
                store,
                inner,
            })
    }

    pub fn current_node(&self) -> Result<Arc<NodeDefinition>> {
        let node_id = Arc::clone(
            self.current_node_id
                .as_ref()
                .context("the current node is not set in the enterprise catalog")?,
        );
        self.inner
            .read()
            .nodes
            .get_by_name(&node_id)
            .context(
                "the current node set on the catalog did not correspond \
                to any nodes registered in the catalog",
            )
            .map_err(Into::into)
    }

    pub fn matches_node_spec(&self, node_spec: &NodeSpec) -> Result<bool> {
        match node_spec {
            NodeSpec::All => Ok(true),
            NodeSpec::Nodes(v) => self
                .current_node()
                .map(|cn| v.contains(&cn.node_catalog_id)),
        }
    }
}
