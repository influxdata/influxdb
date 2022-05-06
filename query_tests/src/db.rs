use std::{any::Any, sync::Arc};

use datafusion::catalog::catalog::CatalogProvider;
use querier::QuerierNamespace;
use query::{exec::ExecutionContextProvider, QueryDatabase};

/// Abstract database used during testing.
pub trait AbstractDb: CatalogProvider + ExecutionContextProvider + QueryDatabase {
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn Any + Send + Sync + 'static>;

    /// Upcast to [`CatalogProvider`].
    ///
    /// This is required due to <https://github.com/rust-lang/rust/issues/65991>.
    fn as_catalog_provider_arc(self: Arc<Self>) -> Arc<dyn CatalogProvider>;

    /// Upcast to [`QueryDatabase`].
    ///
    /// This is required due to <https://github.com/rust-lang/rust/issues/65991>.
    fn as_query_database(&self) -> &dyn QueryDatabase;
}

impl AbstractDb for QuerierNamespace {
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn Any + Send + Sync + 'static> {
        self as _
    }

    fn as_catalog_provider_arc(self: Arc<Self>) -> Arc<dyn CatalogProvider> {
        self as _
    }

    fn as_query_database(&self) -> &dyn QueryDatabase {
        self
    }
}
