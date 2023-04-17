//! Caching of [`NamespaceSchema`].

mod memory;
pub use memory::*;

mod sharded_cache;
pub use sharded_cache::*;

pub mod metrics;

mod read_through_cache;
pub use read_through_cache::*;

use std::{error::Error, fmt::Debug, sync::Arc};

use async_trait::async_trait;
use data_types::{NamespaceName, NamespaceSchema};

/// An abstract cache of [`NamespaceSchema`].
#[async_trait]
pub trait NamespaceCache: Debug + Send + Sync {
    /// The type of error a [`NamespaceCache`] implementation produces
    /// when unable to read the [`NamespaceSchema`] requested from the
    /// cache.
    type ReadError: Error + Send;

    /// Return the [`NamespaceSchema`] for `namespace`.
    async fn get_schema(
        &self,
        namespace: &NamespaceName<'static>,
    ) -> Result<Arc<NamespaceSchema>, Self::ReadError>;

    /// Place `schema` in the cache, merging the set of tables and their columns
    /// with the existing entry for `namespace`, if any.
    ///
    /// All data except the set of tables/columns have "last writer wins"
    /// semantics.
    ///
    /// If the entry in the cache for `namespace` resolves to a different ID than
    /// the incoming `schema` the write unconditionally overwrites any pre-existing
    /// entry for `namespace`.
    fn put_schema(
        &self,
        namespace: NamespaceName<'static>,
        schema: NamespaceSchema,
    ) -> (Option<Arc<NamespaceSchema>>, Arc<NamespaceSchema>);
}
