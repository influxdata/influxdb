//! Common methods for RPC service implementations

mod error;
pub mod planner;
pub mod test_util;

use std::sync::Arc;

use async_trait::async_trait;
use iox_query::{exec::ExecutionContextProvider, QueryDatabase};
use trace::span::Span;
use tracker::InstrumentedAsyncOwnedSemaphorePermit;

/// Trait that allows the query engine (which includes flight and storage/InfluxRPC) to access a virtual set of
/// databases.
///
/// The query engine MUST ONLY use this trait to access the databases / catalogs.
#[async_trait]
pub trait QueryDatabaseProvider: std::fmt::Debug + Send + Sync + 'static {
    /// Abstract database.
    type Db: ExecutionContextProvider + QueryDatabase;

    /// Get database if it exists.
    async fn db(&self, name: &str, span: Option<Span>) -> Option<Arc<Self::Db>>;

    /// Acquire concurrency-limiting sempahore
    async fn acquire_semaphore(&self, span: Option<Span>) -> InstrumentedAsyncOwnedSemaphorePermit;
}

pub use error::datafusion_error_to_tonic_code;
