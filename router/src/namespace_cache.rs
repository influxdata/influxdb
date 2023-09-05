//! Caching of [`NamespaceSchema`].

mod memory;
pub use memory::*;

mod sharded_cache;
pub use sharded_cache::*;

pub mod metrics;

mod read_through_cache;
pub use read_through_cache::*;

use std::{collections::BTreeMap, error::Error, fmt::Debug, sync::Arc};

use async_trait::async_trait;
use data_types::{ColumnsByName, NamespaceName, NamespaceSchema, TableSchema};

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
    fn put_schema(
        &self,
        namespace: NamespaceName<'static>,
        schema: NamespaceSchema,
    ) -> (Arc<NamespaceSchema>, ChangeStats);
}

#[async_trait]
impl<T> NamespaceCache for Arc<T>
where
    T: NamespaceCache,
{
    type ReadError = T::ReadError;

    async fn get_schema(
        &self,
        namespace: &NamespaceName<'static>,
    ) -> Result<Arc<NamespaceSchema>, Self::ReadError> {
        T::get_schema(self, namespace).await
    }

    fn put_schema(
        &self,
        namespace: NamespaceName<'static>,
        schema: NamespaceSchema,
    ) -> (Arc<NamespaceSchema>, ChangeStats) {
        T::put_schema(self, namespace, schema)
    }
}

/// Change statistics describing how the cache entry was modified by the
/// associated [`NamespaceCache::put_schema()`] call.
#[derive(Debug, PartialEq, Eq)]
pub struct ChangeStats {
    /// The new tables added to the cache, keyed by table name.
    pub(crate) new_tables: BTreeMap<String, TableSchema>,

    /// The new columns added to cache for all pre-existing tables, keyed by
    /// the table name.
    pub(crate) new_columns_per_table: BTreeMap<String, ColumnsByName>,

    /// The number of new columns added across new and existing tables.
    pub(crate) num_new_columns: usize,

    /// Indicates whether the change took place when an entry already
    /// existed.
    pub(crate) did_update: bool,
}

/// An optional [`NamespaceCache`] decorator layer.
#[derive(Debug)]
pub enum MaybeLayer<T, U> {
    /// With the optional layer.
    With(T),
    /// Without the operational layer.
    Without(U),
}

#[async_trait]
impl<T, U, E> NamespaceCache for MaybeLayer<T, U>
where
    T: NamespaceCache<ReadError = E>,
    U: NamespaceCache<ReadError = E>,
    E: Error + Send,
{
    type ReadError = E;

    async fn get_schema(
        &self,
        namespace: &NamespaceName<'static>,
    ) -> Result<Arc<NamespaceSchema>, Self::ReadError> {
        match self {
            MaybeLayer::With(v) => v.get_schema(namespace).await,
            MaybeLayer::Without(v) => v.get_schema(namespace).await,
        }
    }

    fn put_schema(
        &self,
        namespace: NamespaceName<'static>,
        schema: NamespaceSchema,
    ) -> (Arc<NamespaceSchema>, ChangeStats) {
        match self {
            MaybeLayer::With(v) => v.put_schema(namespace, schema),
            MaybeLayer::Without(v) => v.put_schema(namespace, schema),
        }
    }
}
