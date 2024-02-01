//! IOx parquet cache client.
//!
//! ParquetCache client interface to be used by IOx components to
//! get and put parquet files into the cache.

#![deny(rustdoc::broken_intra_doc_links, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_docs,
    clippy::explicit_iter_loop,
    // See https://github.com/influxdata/influxdb_iox/pull/1671
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr,
    clippy::todo,
    clippy::dbg_macro,
    unused_crate_dependencies
)]
#![allow(rustdoc::private_intra_doc_links, unreachable_pub)]

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

mod client;
pub use client::{cache_connector::Error, write_hints::WriteHintingObjectStore};

pub mod controller;

pub(crate) mod data_types;

mod server;
#[cfg(test)]
pub use server::mock::MockCacheServer;
pub use server::{build_cache_server, ParquetCacheServer, ParquetCacheServerConfig, ServerError};

use object_store::ObjectStore;
use std::sync::Arc;

use crate::client::{cache_connector::build_cache_connector, object_store::DataCacheObjectStore};

// TODO: change this to `Arc<dyn WriteHintingObjectStore>`
// and have consumers (e.g. ingester, compactor) issue write-hints.
//
/// Build a cache client.
pub fn make_client(
    namespace_service_address: String,
    object_store: Arc<dyn ObjectStore>,
) -> Arc<dyn ObjectStore> {
    let server_connection = build_cache_connector(namespace_service_address);
    Arc::new(DataCacheObjectStore::new(server_connection, object_store))
}
