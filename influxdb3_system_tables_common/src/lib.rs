//! System tables shared between Core and Enterprise
//!
//! The system tables in Core and Enterprise editions share the system table
//! implementation by using the traits defined here (e.g. [`ParquetFilesSource`])

use std::{ops::Deref, sync::Arc};

use datafusion::{
    logical_expr::{BinaryExpr, Expr, Operator, col},
    scalar::ScalarValue,
};

mod databases;
pub use databases::DatabasesTable;
mod distinct_caches;
pub use distinct_caches::DistinctCachesTable;
mod generations;
pub use generations::GenerationDurationsTable;
mod influxdb_schema;
pub use influxdb_schema::InfluxdbSchemaTable;
mod last_caches;
pub use last_caches::LastCachesTable;
mod nodes;
pub use nodes::NodeSystemTable;
mod parquet_files;
pub use parquet_files::{ParquetFileRow, ParquetFilesSource, ParquetFilesTable};
mod plugins;
pub use plugins::{PluginFileRow, PluginsSource, PluginsTable};
mod processing_engine_logs;
pub use processing_engine_logs::{
    ProcessingEngineLogsSource, ProcessingEngineLogsTable, processing_engine_logs_view,
};
mod processing_engine_trigger_arguments;
pub use processing_engine_trigger_arguments::ProcessingEngineTriggerArgumentsTable;
mod processing_engine_triggers;
pub use processing_engine_triggers::ProcessingEngineTriggerTable;
mod queries;
pub use queries::QueriesTable;
mod tables;
pub use tables::TablesTable;
mod tokens;
pub use tokens::{TokenPermissionsFormatter, TokenSystemTable};

/// The default timezone used in the system schema.
pub const DEFAULT_TIMEZONE: &str = "UTC";

pub const TABLE_NAME_PREDICATE: &str = "table_name";

/// Used in queries to the system.{table_name} table
///
/// # Example
/// ```sql
/// SELECT * FROM system.parquet_files WHERE table_name = 'foo'
/// ```
pub fn find_table_name_in_filter(filters: Option<Vec<Expr>>) -> Option<Arc<str>> {
    filters.map(|all_filters| {
        all_filters.iter().find_map(|f| match f {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                if left.deref() == &col(TABLE_NAME_PREDICATE) && op == &Operator::Eq {
                    match right.deref() {
                        Expr::Literal(
                            ScalarValue::Utf8(Some(s))
                            | ScalarValue::LargeUtf8(Some(s))
                            | ScalarValue::Utf8View(Some(s)),
                            _,
                        ) => Some(s.as_str().into()),
                        _ => None,
                    }
                } else {
                    None
                }
            }
            _ => None,
        })
    })?
}
