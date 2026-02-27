//! System table for exposing database schema information using InfluxDB terminology.
//!
//! This module provides the `system.influxdb_schema` table which presents column
//! information using InfluxDB's data model terminology (measurement, tag, field, etc.)
//! rather than SQL table/column terminology.

use std::sync::Arc;

use arrow::array::StringViewBuilder;
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::{error::DataFusionError, logical_expr::Expr};
use influxdb3_catalog::catalog::{DatabaseSchema, InfluxColumnType, InfluxFieldType};
use iox_system_tables::IoxSystemTable;

/// System table that provides schema information using InfluxDB terminology.
///
/// This table exposes the schema of all tables across the current database,
/// presenting the information in terms familiar to InfluxDB users:
/// - `measurement`: The table name (equivalent to measurement name in InfluxDB)
/// - `key`: The column name (field or tag name)
/// - `data_type`: The InfluxDB data type (time, tag, boolean, integer, uinteger, float, string)
///
/// # Example Usage
/// ```sql
/// -- Get all schema information
/// SELECT * FROM system.influxdb_schema;
///
/// -- Get schema for a specific measurement
/// SELECT * FROM system.influxdb_schema WHERE measurement = 'cpu_usage';
///
/// -- Get only tag columns
/// SELECT * FROM system.influxdb_schema WHERE data_type = 'tag';
/// ```
#[derive(Debug)]
pub(super) struct InfluxdbSchemaTable {
    db_schema: Arc<DatabaseSchema>,
    schema: SchemaRef,
}

impl InfluxdbSchemaTable {
    /// Creates a new InfluxdbSchemaTable instance.
    ///
    /// # Arguments
    /// * `db_schema` - The schema of the current database.
    pub(super) fn new(db_schema: Arc<DatabaseSchema>) -> Self {
        Self {
            db_schema,
            schema: influxdb_schema_schema(),
        }
    }
}

/// Creates the Arrow schema for the influxdb_schema system table.
///
/// The schema consists of three columns:
/// - `measurement`: The table/measurement name (String)
/// - `key`: The column/field name (String)
/// - `data_type`: The InfluxDB data type (String)
fn influxdb_schema_schema() -> SchemaRef {
    let columns = vec![
        Field::new("measurement", DataType::Utf8View, false),
        Field::new("key", DataType::Utf8View, false),
        Field::new("data_type", DataType::Utf8View, false),
    ];
    Arc::new(Schema::new(columns))
}

/// Converts an InfluxDB column type to its string representation.
///
/// This function maps the internal InfluxDB column types to user-friendly
/// string representations that match InfluxDB's data model:
///
/// # Arguments
/// * `column_type` - The internal InfluxDB column type
///
/// # Returns
/// A string representation of the column type:
/// - `"time"` for timestamp columns
/// - `"tag"` for tag columns
/// - `"boolean"`, `"integer"`, `"uinteger"`, `"float"`, `"string"` for field columns
fn column_type_to_influxdb_type(column_type: &InfluxColumnType) -> &'static str {
    match column_type {
        InfluxColumnType::Timestamp => "time",
        InfluxColumnType::Tag => "tag",
        InfluxColumnType::Field(field_type) => match field_type {
            InfluxFieldType::Boolean => "boolean",
            InfluxFieldType::Integer => "integer",
            InfluxFieldType::UInteger => "uinteger",
            InfluxFieldType::Float => "float",
            InfluxFieldType::String => "string",
        },
    }
}

#[async_trait::async_trait]
impl IoxSystemTable for InfluxdbSchemaTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    /// Scans the influxdb_schema system table to return schema information.
    ///
    /// This method iterates through all tables in the current database,
    /// extracting column information and presenting it in InfluxDB terminology.
    ///
    /// # Arguments
    /// * `_filters` - Optional filter expressions (currently unused)
    /// * `_limit` - Optional limit on number of rows (currently unused)
    ///
    /// # Returns
    /// A `RecordBatch` containing three columns:
    /// - `measurement`: Table names in the current database
    /// - `key`: Column names within each table
    /// - `data_type`: InfluxDB data type for each column
    async fn scan(
        &self,
        _filters: Option<Vec<Expr>>,
        _limit: Option<usize>,
    ) -> Result<RecordBatch, DataFusionError> {
        // Count total columns across all tables
        let total_columns: usize = self
            .db_schema
            .tables
            .resource_iter()
            .map(|table| table.columns.len())
            .sum();

        let mut measurement_arr = StringViewBuilder::with_capacity(total_columns);
        let mut key_arr = StringViewBuilder::with_capacity(total_columns);
        let mut data_type_array = StringViewBuilder::with_capacity(total_columns);

        for table in self.db_schema.tables.resource_iter() {
            for column in table.columns.resource_iter() {
                measurement_arr.append_value(&table.table_name);
                key_arr.append_value(column.name());

                let column_type = column_type_to_influxdb_type(&column.column_type());
                data_type_array.append_value(column_type);
            }
        }

        let columns: Vec<ArrayRef> = vec![
            Arc::new(measurement_arr.finish()),
            Arc::new(key_arr.finish()),
            Arc::new(data_type_array.finish()),
        ];

        RecordBatch::try_new(self.schema(), columns).map_err(DataFusionError::from)
    }
}

#[cfg(test)]
mod tests;
