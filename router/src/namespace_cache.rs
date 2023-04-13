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

    /// Place `schema` in the cache, unconditionally overwriting any existing
    /// [`NamespaceSchema`] mapped to `namespace`.
    fn put_schema(
        &self,
        namespace: NamespaceName<'static>,
        schema: impl Into<Arc<NamespaceSchema>>,
    ) -> (Option<Arc<NamespaceSchema>>, NamespaceStats);
}

#[derive(Debug, PartialEq, Eq)]
/// An encapsulation of statistics associated with a namespace schema.
pub struct NamespaceStats {
    /// Number of tables within the namespace
    pub table_count: u64,
    /// Total number of columns across all tables within the namespace
    pub column_count: u64,
}

impl NamespaceStats {
    /// Derives a set of [`NamespaceStats`] from the given schema.
    pub fn new(ns: &NamespaceSchema) -> Self {
        let table_count = ns.tables.len() as _;
        let column_count = ns.tables.values().fold(0, |acc, t| acc + t.columns.len()) as _;
        Self {
            table_count,
            column_count,
        }
    }
}
