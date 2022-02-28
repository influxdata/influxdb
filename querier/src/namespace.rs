use std::sync::Arc;

use db::catalog::Catalog as DbCatalog;
use iox_catalog::interface::{Catalog, NamespaceId};
use object_store::ObjectStore;
use time::TimeProvider;

use crate::chunk::ParquetChunkAdapter;

/// Maps a catalog namespace to all the in-memory resources and sync-state that the querier needs.
#[derive(Debug)]
pub struct QuerierNamespace {
    /// Old-gen DB catalog.
    catalog: DbCatalog,

    /// Adapter to create old-gen chunks.
    chunk_adapter: ParquetChunkAdapter,

    /// ID of this namespace.
    id: NamespaceId,

    /// Name of this namespace.
    name: Arc<str>,
}

impl QuerierNamespace {
    /// Create new, empty namespace.
    ///
    /// You may call [`sync`](Self::sync) to fill the namespace with chunks.
    pub fn new(
        catalog: Arc<dyn Catalog>,
        name: Arc<str>,
        id: NamespaceId,
        metric_registry: Arc<metric::Registry>,
        object_store: Arc<ObjectStore>,
        time_provider: Arc<dyn TimeProvider>,
    ) -> Self {
        Self {
            catalog: DbCatalog::new(
                Arc::clone(&name),
                Arc::clone(&metric_registry),
                time_provider,
            ),
            chunk_adapter: ParquetChunkAdapter::new(catalog, object_store, metric_registry),
            id,
            name,
        }
    }

    /// Namespace name.
    pub fn name(&self) -> Arc<str> {
        Arc::clone(&self.name)
    }

    /// Sync tables and chunks.
    ///
    /// Should be called regularly.
    pub async fn sync(&self) {
        // TODO: implement this
    }
}
