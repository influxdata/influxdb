//! This module contains gRPC service implementation for "InfluxRPC" (aka the
//! storage RPC API used for Flux and InfluxQL)

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

// rustc doesn't seem to understand test-only requirements
#[cfg(test)]
use panic_logging as _;

/// `[0x00]` is the magic value that that the storage gRPC layer uses to
/// encode a tag_key that means "measurement name"
pub(crate) const TAG_KEY_MEASUREMENT: &[u8] = &[0];

/// `[0xff]` is is the magic value that that the storage gRPC layer uses
/// to encode a tag_key that means "field name"
pub(crate) const TAG_KEY_FIELD: &[u8] = &[255];

pub mod data;
pub mod expr;
pub mod id;
pub mod input;
mod permit;
mod query_completed_token;
mod response_chunking;
pub mod service;

#[cfg(any(test, feature = "test-util"))]
pub mod test_util;

use generated_types::storage_server::{Storage, StorageServer};
use service_common::QueryNamespaceProvider;
use std::sync::Arc;

/// Concrete implementation of the gRPC InfluxDB Storage Service API
#[derive(Debug)]
struct StorageService<T: QueryNamespaceProvider> {
    pub db_store: Arc<T>,
}

pub fn make_server<T: QueryNamespaceProvider + 'static>(
    db_store: Arc<T>,
) -> StorageServer<impl Storage> {
    StorageServer::new(StorageService { db_store })
}
