use std::{fmt::Display, sync::Arc};

use datafusion::config::{ParquetOptions, TableParquetOptions};
use datafusion::datasource::file_format::FileFormatFactory;
use datafusion::datasource::file_format::arrow::ArrowFormatFactory;
use datafusion::datasource::file_format::csv::CsvFormatFactory;
use datafusion::datasource::file_format::json::JsonFormatFactory;
use datafusion::datasource::file_format::parquet::ParquetFormatFactory;
use datafusion::{
    config::ConfigOptions, execution::runtime_env::RuntimeEnv, prelude::SessionConfig,
};
use object_store::ObjectStore;
use schema::TIME_DATA_TIMEZONE;
use url::Url;

// The default catalog name - this impacts what SQL queries use if not specified
pub const DEFAULT_CATALOG: &str = "public";
// The default schema name - this impacts what SQL queries use if not specified
pub const DEFAULT_SCHEMA: &str = "iox";
// The system table schema name
pub const SYSTEM_SCHEMA: &str = "system";

/// The maximum number of rows that DataFusion should create in each RecordBatch
pub const BATCH_SIZE: usize = 8 * 1024;

/// Return a SessionConfig object configured for IOx
pub fn iox_session_config() -> SessionConfig {
    // Enable parquet predicate pushdown optimization
    let mut options = ConfigOptions::new();
    options.execution.parquet.pushdown_filters = true;
    options.execution.parquet.reorder_filters = true;
    options.execution.parquet.schema_force_view_types = false;
    if let Some(time_zone) = TIME_DATA_TIMEZONE() {
        options.execution.time_zone = time_zone.to_string();
    }
    options.optimizer.repartition_sorts = true;
    options.optimizer.prefer_existing_union = true;
    // DataSourceExec now returns estimates rather than actual
    // row counts, so we must use estimates rather than exact to optimize partitioning
    // Related to https://github.com/apache/datafusion/issues/8078
    options
        .execution
        .use_row_number_estimates_to_optimize_partitioning = true;

    // This was set to `true` by https://github.com/apache/datafusion/pull/16290 , however it is unlikely that all our
    // clients support this yet. Some people may experience errors like this:
    //
    // ```
    // flightsql: arrow/flight: could not create flight reader: arrow/ipc: unknown error while reading: arrow/ipc: type not implemented
    // ```
    options.sql_parser.map_string_types_to_utf8view = false;

    SessionConfig::from(options)
        .with_batch_size(BATCH_SIZE)
        .with_create_default_catalog_and_schema(true)
        .with_information_schema(true)
        .with_default_catalog_and_schema(DEFAULT_CATALOG, DEFAULT_SCHEMA)
        // Tell the datafusion optimizer to avoid repartitioning sorted inputs
        .with_prefer_existing_sort(true)
}

/// Use mostly default file formats, but apply iox-specific [`TableParquetOptions`].
pub fn iox_file_formats() -> Vec<Arc<dyn FileFormatFactory>> {
    vec![
        Arc::new(ParquetFormatFactory::new_with_options(
            table_parquet_options(),
        )),
        Arc::new(JsonFormatFactory::new()),
        Arc::new(CsvFormatFactory::new()),
        Arc::new(ArrowFormatFactory::new()),
    ]
}

/// Return a TableParquetOptions object configured for IOx
/// This is a workaround until DataFusion supports retrieving these options from the DataSourceExec
/// <https://github.com/apache/arrow-datafusion/issues/9908>
pub fn table_parquet_options() -> TableParquetOptions {
    TableParquetOptions {
        global: ParquetOptions {
            // should match the configuration in `iox_session_config`
            pushdown_filters: true,
            reorder_filters: true,

            // TODO: enable view types in iox code
            // Refer to https://github.com/influxdata/influxdb_iox/issues/13093
            schema_force_view_types: false,

            ..ParquetOptions::default()
        },
        ..TableParquetOptions::default()
    }
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
