use std::{any::Any, sync::Arc};

use datafusion::catalog::catalog::CatalogProvider;
use iox_query::{exec::ExecutionContextProvider, QueryNamespace};
use querier::QuerierNamespace;

/// Abstract database used during testing.
pub trait AbstractDb: CatalogProvider + ExecutionContextProvider + QueryNamespace {
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn Any + Send + Sync + 'static>;

    /// Upcast to [`CatalogProvider`].
    ///
    /// This is required due to <https://github.com/rust-lang/rust/issues/65991>.
    fn as_catalog_provider_arc(self: Arc<Self>) -> Arc<dyn CatalogProvider>;

    /// Upcast to [`QueryNamespace`].
    ///
    /// This is required due to <https://github.com/rust-lang/rust/issues/65991>.
    fn as_query_namespace_arc(self: Arc<Self>) -> Arc<dyn QueryNamespace>;
}

impl AbstractDb for QuerierNamespace {
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn Any + Send + Sync + 'static> {
        self as _
    }

    fn as_catalog_provider_arc(self: Arc<Self>) -> Arc<dyn CatalogProvider> {
        self as _
    }

    fn as_query_namespace_arc(self: Arc<Self>) -> Arc<dyn QueryNamespace> {
        self
    }
}
