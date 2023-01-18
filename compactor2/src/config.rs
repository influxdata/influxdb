//! Config-related stuff.
use std::sync::Arc;

use iox_catalog::interface::Catalog;
use iox_query::exec::Executor;
use iox_time::TimeProvider;
use parquet_file::storage::ParquetStorage;

/// Config to set up a compactor.
pub struct Config {
    /// Metric registry.
    pub metric_registry: Arc<metric::Registry>,

    /// Central catalog.
    pub catalog: Arc<dyn Catalog>,

    /// Store holding the parquet files.
    pub parquet_store: ParquetStorage,

    /// Executor.
    pub exec: Arc<Executor>,

    /// Time provider.
    pub time_provider: Arc<dyn TimeProvider>,
}
