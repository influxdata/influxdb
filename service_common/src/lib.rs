//! Common methods for RPC service implementations

pub mod planner;

use std::sync::Arc;

use query::{exec::ExecutionContextProvider, QueryDatabase};

/// Trait that allows the query engine (which includes flight and storage/InfluxRPC) to access a virtual set of
/// databases.
///
/// The query engine MUST ONLY use this trait to access the databases / catalogs.
pub trait QueryDatabaseProvider: std::fmt::Debug + Send + Sync + 'static {
    /// Abstract database.
    type Db: ExecutionContextProvider + QueryDatabase;

    /// Get database if it exists.
    fn db(&self, name: &str) -> Option<Arc<Self::Db>>;
}
