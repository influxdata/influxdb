use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::{error::DataFusionError, logical_expr::Expr};
use influxdb3_id::DbId;
use influxdb3_write::{ParquetFile, WriteBuffer};
use iox_system_tables::IoxSystemTable;

use crate::system_tables::find_table_name_in_filter;

#[derive(Debug)]
pub(super) struct ParquetFilesTable {
    db_id: DbId,
    schema: SchemaRef,
    buffer: Arc<dyn WriteBuffer>,
}

impl ParquetFilesTable {
    pub(super) fn new(db_id: DbId, buffer: Arc<dyn WriteBuffer>) -> Self {
        Self {
            db_id,
            schema: parquet_files_schema(),
            buffer,
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
            let table_id = self
                .buffer
                .catalog()
                .db_schema_by_id(&self.db_id)
                .expect("db exists")
                .table_name_to_id(Arc::clone(&table_name))
                .expect("table exists");
            self.buffer
                .parquet_files(self.db_id, table_id)
                .into_iter()
                .map(|file| (Arc::clone(&table_name), file))
                .collect()
        } else {
            self.buffer
                .catalog()
                .list_db_schema()
                .iter()
                .flat_map(|db| db.tables())
                .flat_map(|table_def| {
                    self.buffer
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
    parquet_files: Vec<(Arc<str>, ParquetFile)>,
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
