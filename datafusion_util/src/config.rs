use datafusion::{
    config::{
        OPT_COALESCE_TARGET_BATCH_SIZE, OPT_PARQUET_PUSHDOWN_FILTERS, OPT_PARQUET_REORDER_FILTERS,
    },
    prelude::SessionConfig,
};

// The default catalog name - this impacts what SQL queries use if not specified
pub const DEFAULT_CATALOG: &str = "public";
// The default schema name - this impacts what SQL queries use if not specified
pub const DEFAULT_SCHEMA: &str = "iox";

/// The maximum number of rows that DataFusion should create in each RecordBatch
pub const BATCH_SIZE: usize = 8 * 1024;

const COALESCE_BATCH_SIZE: usize = BATCH_SIZE / 2;

/// Return a SessionConfig object configured for IOx
pub fn iox_session_config() -> SessionConfig {
    SessionConfig::new()
        .with_batch_size(BATCH_SIZE)
        .set_u64(
            OPT_COALESCE_TARGET_BATCH_SIZE,
            COALESCE_BATCH_SIZE.try_into().unwrap(),
        )
        // Enable parquet predicate pushdown optimization
        .set_bool(OPT_PARQUET_PUSHDOWN_FILTERS, true)
        .set_bool(OPT_PARQUET_REORDER_FILTERS, true)
        .with_create_default_catalog_and_schema(true)
        .with_information_schema(true)
        .with_default_catalog_and_schema(DEFAULT_CATALOG, DEFAULT_SCHEMA)
}
