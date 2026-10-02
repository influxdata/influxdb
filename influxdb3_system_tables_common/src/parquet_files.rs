//! `system.parquet_files` system table implementation [`ParquetFilesTable`]
use std::{fmt::Debug, sync::Arc};

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::{error::DataFusionError, logical_expr::Expr};
use influxdb3_catalog::catalog::Catalog;
use influxdb3_id::{DbId, TableId};
use iox_system_tables::IoxSystemTable;

use crate::find_table_name_in_filter;

/// API to provide information about Parquet files to [`ParquetFilesTable`]
pub trait ParquetFilesSource: Debug + Send + Sync {
    fn catalog(&self) -> Arc<Catalog>;

    fn parquet_files(&self, db_id: DbId, table_id: TableId) -> Vec<ParquetFileRow>;
}

/// The summary data for a persisted parquet file, as exposed by the
/// `system.parquet_files` table.
#[derive(Debug, Clone)]
pub struct ParquetFileRow {
    pub path: Arc<str>,
    pub size_bytes: u64,
    pub row_count: u64,
    /// min time nanos; aka the time of the oldest record in the file
    pub min_time: i64,
    /// max time nanos; aka the time of the newest record in the file
    pub max_time: i64,
}

/// `system.parquet_files` system table implementation
#[derive(Debug)]
pub struct ParquetFilesTable {
    db_id: DbId,
    schema: SchemaRef,
    source: Arc<dyn ParquetFilesSource>,
}

impl ParquetFilesTable {
    pub fn new(db_id: DbId, source: Arc<dyn ParquetFilesSource>) -> Self {
        Self {
            db_id,
            schema: parquet_files_schema(),
            source,
        }
    }
}

fn parquet_files_schema() -> SchemaRef {
    let columns = vec![
        Field::new("table_name", DataType::Utf8, false),
        Field::new("path", DataType::Utf8, false),
        Field::new("size_bytes", DataType::UInt64, false),
        Field::new("row_count", DataType::UInt64, false),
        Field::new("min_time", DataType::Int64, false),
        Field::new("max_time", DataType::Int64, false),
    ];
    Arc::new(Schema::new(columns))
}

#[async_trait]
impl IoxSystemTable for ParquetFilesTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    async fn scan(
        &self,
        filters: Option<Vec<Expr>>,
        limit: Option<usize>,
    ) -> Result<RecordBatch, DataFusionError> {
        let schema = self.schema();
        let limit = limit.unwrap_or(usize::MAX);

        // extract `table_name` from filters
        let table_name = find_table_name_in_filter(filters);

        let parquet_files = if let Some(table_name) = table_name {
            // `table_name` is an untrusted value from the query's WHERE clause, and the database can
            // be dropped between this table's construction and the scan. Either miss resolves to
            // `None` and must yield zero rows, not a panic.
            let table_id = self
                .source
                .catalog()
                .db_schema_by_id(&self.db_id)
                .and_then(|db| db.table_name_to_id(Arc::clone(&table_name)));
            match table_id {
                Some(table_id) => self
                    .source
                    .parquet_files(self.db_id, table_id)
                    .into_iter()
                    .map(|file| (Arc::clone(&table_name), file))
                    .take(limit)
                    .collect(),
                None => vec![],
            }
        } else {
            self.source
                .catalog()
                .list_db_schema()
                .iter()
                .flat_map(|db| db.tables())
                .flat_map(|table_def| {
                    self.source
                        .parquet_files(self.db_id, table_def.table_id)
                        .into_iter()
                        .map(move |file| (Arc::clone(&table_def.table_name), file))
                })
                .take(limit)
                .collect()
        };

        from_parquet_files(schema, parquet_files)
    }
}

/// Produce a record batch listing parquet file information based on the given `schema` and
/// `parquet_files`, a list of table name and parquet file pairs.
fn from_parquet_files(
    schema: SchemaRef,
    parquet_files: Vec<(Arc<str>, ParquetFileRow)>,
) -> Result<RecordBatch, DataFusionError> {
    let columns: Vec<ArrayRef> = vec![
        Arc::new(
            parquet_files
                .iter()
                .map(|(table_name, _)| Some(table_name))
                .collect::<StringArray>(),
        ),
        Arc::new(
            parquet_files
                .iter()
                .map(|(_, f)| Some(f.path.to_string()))
                .collect::<StringArray>(),
        ),
        Arc::new(
            parquet_files
                .iter()
                .map(|(_, f)| Some(f.size_bytes))
                .collect::<UInt64Array>(),
        ),
        Arc::new(
            parquet_files
                .iter()
                .map(|(_, f)| Some(f.row_count))
                .collect::<UInt64Array>(),
        ),
        Arc::new(
            parquet_files
                .iter()
                .map(|(_, f)| Some(f.min_time))
                .collect::<Int64Array>(),
        ),
        Arc::new(
            parquet_files
                .iter()
                .map(|(_, f)| Some(f.max_time))
                .collect::<Int64Array>(),
        ),
    ];

    Ok(RecordBatch::try_new(schema, columns)?)
}
