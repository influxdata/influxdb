use std::sync::Arc;

use iox_time::TimeProvider;
use object_store::ObjectStore;
use parking_lot::RwLock;
use tokio::sync::broadcast;

use crate::Result;
use crate::object_store::ObjectStoreCatalog;

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
        let node_id = catalog_id.into();
        let store =
            ObjectStoreCatalog::new(Arc::clone(&node_id), CATALOG_CHECKPOINT_INTERVAL, store);
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

    pub fn current_node(&self) -> Option<Arc<NodeDefinition>> {
        let id = Arc::clone(self.current_node_id.as_ref().unwrap_or(&self.store.prefix));
        self.inner.read().nodes.get(&id).cloned()
    }
}
