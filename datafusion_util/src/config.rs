use datafusion::{config::ConfigOptions, prelude::SessionConfig};

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
    // Disable repartioned Sorts in IOx until we rework plan
    // construction as part of
    // https://github.com/influxdata/influxdb_iox/issues/6098.
    options.optimizer.repartition_sorts = false;

    SessionConfig::from(options)
        .with_batch_size(BATCH_SIZE)
        .with_create_default_catalog_and_schema(true)
        .with_information_schema(true)
        .with_default_catalog_and_schema(DEFAULT_CATALOG, DEFAULT_SCHEMA)
}
