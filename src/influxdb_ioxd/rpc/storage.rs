//! This module contains gRPC service implementations

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
use metrics::{MetricRegistry, RedMetric};
use server::DatabaseStore;
use std::sync::Arc;

/// Concrete implementation of the gRPC InfluxDB Storage Service API
#[derive(Debug)]
struct StorageService<T: DatabaseStore> {
    pub db_store: Arc<T>,
    pub metrics: Metrics,
}

pub fn make_server<T: DatabaseStore + 'static>(
    db_store: Arc<T>,
    metrics_registry: Arc<MetricRegistry>,
) -> StorageServer<impl Storage> {
    StorageServer::new(StorageService {
        db_store,
        metrics: Metrics::new(metrics_registry),
    })
}

// These are the metrics associated with the gRPC server.
#[derive(Debug)]
pub struct Metrics {
    /// The metric tracking the outcome of requests to the gRPC service
    pub requests: RedMetric,

    // Holding the registry allows it to be replaced for a test implementation
    pub(crate) metrics_registry: Arc<MetricRegistry>,
}

impl Metrics {
    fn new(registry: Arc<MetricRegistry>) -> Self {
        Self {
            requests: registry.register_domain("gRPC").register_red_metric(None),
            metrics_registry: registry,
        }
    }
}
