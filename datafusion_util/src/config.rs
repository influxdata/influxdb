use std::{fmt::Display, sync::Arc};

use datafusion::{
    config::ConfigOptions, execution::runtime_env::RuntimeEnv, prelude::SessionConfig,
};
use object_store::ObjectStore;
use url::Url;

// The default catalog name - this impacts what SQL queries use if not specified
pub const DEFAULT_CATALOG: &str = "public";
// The default schema name - this impacts what SQL queries use if not specified
pub const DEFAULT_SCHEMA: &str = "iox";

/// The maximum number of rows that DataFusion should create in each RecordBatch
pub const BATCH_SIZE: usize = 8 * 1024;

/// Return a SessionConfig object configured for IOx
pub fn iox_session_config() -> SessionConfig {
    // Enable parquet predicate pushdown optimization
    let mut options = ConfigOptions::new();
    options.execution.parquet.pushdown_filters = true;
    options.execution.parquet.reorder_filters = true;
    options.optimizer.repartition_sorts = true;

    SessionConfig::from(options)
        .with_batch_size(BATCH_SIZE)
        .with_create_default_catalog_and_schema(true)
        .with_information_schema(true)
        .with_default_catalog_and_schema(DEFAULT_CATALOG, DEFAULT_SCHEMA)
}

/// Register the "IOx" object store provider for URLs of the form "iox://{id}
///
/// Return the previous registered store, if any
pub fn register_iox_object_store<D: Display>(
    runtime: impl AsRef<RuntimeEnv>,
    id: D,
    object_store: Arc<dyn ObjectStore>,
) -> Option<Arc<dyn ObjectStore>> {
    let url = Url::parse(&format!("iox://{id}")).unwrap();
    runtime.as_ref().register_object_store(&url, object_store)
}
