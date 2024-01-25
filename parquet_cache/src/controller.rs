//! The controller module contains the API and functionality
//! used to implement the controller for a DataCacheSet.

use futures::future::select;
use kube::Client;
use std::time::Duration;

mod error;
pub use error::{Error, Result};
mod kube_util;
mod parquet_cache;
pub use parquet_cache::{
    ParquetCache, ParquetCacheInstanceSet, ParquetCacheSpec, ParquetCacheStatus,
};

mod parquet_cache_controller;

mod parquet_cache_set;
pub use parquet_cache_set::{ParquetCacheSet, ParquetCacheSetSpec, ParquetCacheSetStatus};

mod parquet_cache_set_controller;

mod state_service;

/// The name of the controller.
const CONTROLLER_NAME: &str = "parquet-cache-set-controller";

/// Label used to annotate the objects with the hash of the pod template.
const POD_TEMPLATE_HASH_LABEL: &str = "pod-template-hash";

/// Label used to annotate objects with the count of parquet cache replicas.
const PARQUET_CACHE_REPLICAS_LABEL: &str = "parquet-cache-replicas";

/// The time to wait before re-executing when waiting for cache instances to warm, or cool.
const SHORT_WAIT: Duration = Duration::from_secs(60);

/// The time to wait before re-executing when there is no longer any active work to do, or
/// the controller will be awoken by changes to owned objects.
const LONG_WAIT: Duration = Duration::from_secs(3600);

/// Run the controllers for ParquetCache and ParquetCacheSet resources to completion.
pub async fn run(client: Client, namespace: Option<String>) -> Result<(), kube::Error> {
    let parquet_cache_join_handle =
        parquet_cache_controller::spawn_controller(client.clone(), namespace.clone());
    let parquet_cache_set_join_handle =
        parquet_cache_set_controller::spawn_controller(client.clone(), namespace.clone());

    select(parquet_cache_join_handle, parquet_cache_set_join_handle)
        .await
        .factor_first()
        .0
        .unwrap()
}
