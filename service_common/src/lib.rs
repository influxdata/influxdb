//! Common methods for RPC service implementations

mod error;
pub mod planner;
pub mod test_util;

use std::sync::Arc;

use async_trait::async_trait;
use iox_query::{exec::ExecutionContextProvider, QueryNamespace};
use trace::span::Span;
use tracker::InstrumentedAsyncOwnedSemaphorePermit;

/// Trait that allows the query engine (which includes flight and storage/InfluxRPC) to access a
/// virtual set of namespaces.
///
/// The query engine MUST ONLY use this trait to access the namespaces / catalogs.
#[async_trait]
pub trait QueryNamespaceProvider: std::fmt::Debug + Send + Sync + 'static {
    /// Abstract namespace.
    type Db: ExecutionContextProvider + QueryNamespace;

    /// Get namespace if it exists.
    async fn db(&self, name: &str, span: Option<Span>) -> Option<Arc<Self::Db>>;

    /// Acquire concurrency-limiting sempahore
    async fn acquire_semaphore(&self, span: Option<Span>) -> InstrumentedAsyncOwnedSemaphorePermit;
}

pub use error::datafusion_error_to_tonic_code;
