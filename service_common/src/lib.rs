//! Common methods for RPC service implementations

#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![allow(clippy::clone_on_ref_ptr)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    // See https://github.com/influxdata/influxdb_iox/pull/1671
    clippy::future_not_send,
    clippy::clone_on_ref_ptr,
    clippy::todo,
    clippy::dbg_macro,
    unused_crate_dependencies
)]

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

mod error;
pub mod planner;
pub mod test_util;

use std::sync::Arc;

use async_trait::async_trait;
use iox_query::QueryNamespace;
use trace::span::Span;
use tracker::InstrumentedAsyncOwnedSemaphorePermit;

/// Trait that allows the query engine (which includes flight and storage/InfluxRPC) to access a
/// virtual set of namespaces.
///
/// The query engine MUST ONLY use this trait to access the namespaces / catalogs.
#[async_trait]
pub trait QueryNamespaceProvider: std::fmt::Debug + Send + Sync + 'static {
    /// Abstract namespace.
    type Db: QueryNamespace;

    /// Get namespace if it exists.
    ///
    /// System tables may contain debug information depending on `include_debug_info_tables`.
    async fn db(
        &self,
        name: &str,
        span: Option<Span>,
        include_debug_info_tables: bool,
    ) -> Option<Arc<Self::Db>>;

    /// Acquire concurrency-limiting sempahore
    async fn acquire_semaphore(&self, span: Option<Span>) -> InstrumentedAsyncOwnedSemaphorePermit;
}

pub use error::datafusion_error_to_tonic_code;
