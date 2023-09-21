use std::fmt::{Debug, Display};

use async_trait::async_trait;
use data_types::{Namespace, NamespaceId, NamespaceSchema};

pub mod catalog;
pub mod mock;

#[async_trait]
pub trait NamespacesSource: Debug + Display + Send + Sync {
    /// Get Namespace for a given namespace
    ///
    /// This method performs retries.
    async fn fetch_by_id(&self, ns: NamespaceId) -> Option<Namespace>;

    /// Get NamespaceSchema for a given namespace
    ///
    /// todo: make this method perform retries.
    async fn fetch_schema_by_id(&self, ns: NamespaceId) -> Option<NamespaceSchema>;
}
