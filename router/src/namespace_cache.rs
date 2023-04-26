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
    /// semantics. The resulting merged schema is returned, along with a set
    /// of change statistics.
    ///
    /// Concurrent calls to this method will race and may result in a schema
    /// change being lost.
    fn put_schema(
        &self,
        namespace: NamespaceName<'static>,
        schema: NamespaceSchema,
    ) -> (Arc<NamespaceSchema>, ChangeStats);
}

/// Change statistics describing how the cache entry was modified by the
/// associated [`NamespaceCache::put_schema()`] call.
#[derive(Debug, PartialEq, Eq)]
pub struct ChangeStats {
    /// The number of tables added to the cache.
    pub(crate) new_tables: usize,

    /// The number of columns added to the cache (across all tables).
    pub(crate) new_columns: usize,

    /// Indicates whether the change took place when an entry already
    /// existed.
    pub(crate) did_update: bool,
}
