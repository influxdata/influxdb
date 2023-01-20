use std::fmt::{Debug, Display};

use async_trait::async_trait;
use data_types::{NamespaceId, NamespaceSchema};

pub mod catalog;
pub mod mock;

#[async_trait]
pub trait NamespacesSource: Debug + Display + Send + Sync {
    /// Get NamespaceSchema for a given namespace
    ///
    /// todo: make this method perform retries.
    async fn fetch(&self, ns: NamespaceId) -> Option<NamespaceSchema>;
}
