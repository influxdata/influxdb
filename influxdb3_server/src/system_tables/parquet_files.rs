use std::{ops::Deref, sync::Arc};

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::{
    error::DataFusionError,
    logical_expr::{col, BinaryExpr, Expr, Operator},
    scalar::ScalarValue,
};
use influxdb3_write::{ParquetFile, WriteBuffer};
use iox_system_tables::IoxSystemTable;

use super::{PARQUET_FILES_TABLE_NAME, SYSTEM_SCHEMA_NAME};

pub(super) struct ParquetFilesTable {
    db_name: Arc<str>,
    schema: SchemaRef,
    buffer: Arc<dyn WriteBuffer>,
}

impl ParquetFilesTable {
    pub(super) fn new(db_name: Arc<str>, buffer: Arc<dyn WriteBuffer>) -> Self {
        Self {
            db_name,
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

/// Used in queries to the system.parquet_files table
///
/// # Example
/// ```sql
/// SELECT * FROM system.parquet_files WHERE table_name = 'foo'
/// ```
const TABLE_NAME_PREDICATE: &str = "table_name";

pub(crate) fn table_name_predicate_error() -> DataFusionError {
    DataFusionError::Plan(format!(
        "must provide a {TABLE_NAME_PREDICATE} = '<table_name>' predicate in queries to \
            {SYSTEM_SCHEMA_NAME}.{PARQUET_FILES_TABLE_NAME}"
    ))
}

#[async_trait]
impl IoxSystemTable for ParquetFilesTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    async fn scan(
        &self,
        filters: Option<Vec<Expr>>,
        _limit: Option<usize>,
    ) -> Result<RecordBatch, DataFusionError> {
        let schema = self.schema();

        // extract `table_name` from filters
        let table_name = filters
            .ok_or_else(table_name_predicate_error)?
            .iter()
            .find_map(|f| match f {
                Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                    if left.deref() == &col(TABLE_NAME_PREDICATE) && op == &Operator::Eq {
                        match right.deref() {
                            Expr::Literal(
                                ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)),
                            ) => Some(s.to_owned()),
                            _ => None,
                        }
                    } else {
                        None
                    }
                }
                _ => None,
            })
            .ok_or_else(table_name_predicate_error)?;

        let parquet_files: Vec<ParquetFile> = self
            .buffer
            .parquet_files(&self.db_name, table_name.as_str());

        from_parquet_files(&table_name, schema, parquet_files)
    }
}

fn from_parquet_files(
    table_name: &str,
    schema: SchemaRef,
    parquet_files: Vec<ParquetFile>,
) -> Result<RecordBatch, DataFusionError> {
    let columns: Vec<ArrayRef> = vec![
        Arc::new(
            vec![table_name; parquet_files.len()]
                .iter()
                .map(|s| Some(s.to_string()))
                .collect::<StringArray>(),
        ),
        Arc::new(
            parquet_files
                .iter()
                .map(|f| Some(f.path.to_string()))
                .collect::<StringArray>(),
        ),
        Arc::new(
            parquet_files
                .iter()
                .map(|f| Some(f.size_bytes))
                .collect::<UInt64Array>(),
        ),
        Arc::new(
            parquet_files
                .iter()
                .map(|f| Some(f.row_count))
                .collect::<UInt64Array>(),
        ),
        Arc::new(
            parquet_files
                .iter()
                .map(|f| Some(f.min_time))
                .collect::<Int64Array>(),
        ),
        Arc::new(
            parquet_files
                .iter()
                .map(|f| Some(f.max_time))
                .collect::<Int64Array>(),
        ),
    ];

    Ok(RecordBatch::try_new(schema, columns)?)
}
