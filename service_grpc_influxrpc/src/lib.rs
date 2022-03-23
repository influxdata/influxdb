//! This module contains gRPC service implementation for "InfluxRPC" (aka the storage RPC API used for Flux and InfluxQL)

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
pub mod service;

use generated_types::storage_server::{Storage, StorageServer};
use service_common::QueryDatabaseProvider;
use std::sync::Arc;

/// Concrete implementation of the gRPC InfluxDB Storage Service API
#[derive(Debug)]
struct StorageService<T: QueryDatabaseProvider> {
    pub db_store: Arc<T>,
}

pub fn make_server<T: QueryDatabaseProvider + 'static>(
    db_store: Arc<T>,
) -> StorageServer<impl Storage> {
    StorageServer::new(StorageService { db_store })
}
