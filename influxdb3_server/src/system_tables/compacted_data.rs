use std::sync::Arc;

use arrow::array::{StringViewBuilder, TimestampNanosecondBuilder, UInt64Builder, UInt8Builder};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use async_trait::async_trait;
use datafusion::{error::DataFusionError, prelude::Expr};
use influxdb3_catalog::catalog::DatabaseSchema;
use influxdb3_pro_compactor::compacted_data::CompactedDataSystemTableView;
use influxdb3_pro_data_layout::CompactedDataSystemTableQueryResult;
use iox_system_tables::IoxSystemTable;
use observability_deps::tracing::debug;

use crate::system_tables::{
    find_table_name_in_filter, table_name_predicate_error, COMPACTED_DATA_TABLE_NAME,
};

#[derive(Debug)]
pub(crate) struct CompactedDataTable {
    db_schema: Arc<DatabaseSchema>,
    compacted_data: Option<Arc<dyn CompactedDataSystemTableView>>,
    compacted_table_schema: Arc<Schema>,
}

impl CompactedDataTable {
    pub fn new(
        db_schema: Arc<DatabaseSchema>,
        compacted_data: Option<Arc<dyn CompactedDataSystemTableView>>,
    ) -> Self {
        Self {
            db_schema,
            compacted_data,
            compacted_table_schema: compacted_data_table_schema(),
        }
    }
}

fn compacted_data_table_schema() -> SchemaRef {
    let columns = vec![
        Field::new("table_name", DataType::Utf8View, false),
        Field::new("generation_id", DataType::UInt64, false),
        Field::new("generation_level", DataType::UInt8, false),
        Field::new("generation_time", DataType::Utf8View, false),
        Field::new("parquet_id", DataType::UInt64, false),
        Field::new("parquet_path", DataType::Utf8View, false),
        Field::new("parquet_size_bytes", DataType::UInt64, false),
        Field::new("parquet_row_count", DataType::UInt64, false),
        Field::new(
            "parquet_chunk_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new(
            "parquet_min_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new(
            "parquet_max_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
    ];
    Arc::new(Schema::new(columns))
}

fn compaction_not_setup_error() -> DataFusionError {
    DataFusionError::Plan("Compaction not setup, cannot fetch compaction details".to_owned())
}

fn table_not_found_error() -> DataFusionError {
    DataFusionError::Plan("Cannot find table name".to_owned())
}

#[async_trait]
impl IoxSystemTable for CompactedDataTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.compacted_table_schema)
    }

    async fn scan(
        &self,
        filters: Option<Vec<Expr>>,
        _limit: Option<usize>,
    ) -> Result<RecordBatch, DataFusionError> {
        let data = self
            .compacted_data
            .clone()
            .ok_or_else(compaction_not_setup_error)?;

        debug!(filters = ?filters, ">>>> all filters");
        let table_name = find_table_name_in_filter(filters)
            .ok_or_else(|| table_name_predicate_error(COMPACTED_DATA_TABLE_NAME))?;

        let _ = self
            .db_schema
            .table_name_to_id(table_name.as_str())
            .ok_or_else(table_not_found_error)?;

        let results = data.query(self.db_schema.name.as_ref(), &table_name);
        to_record_batch(&table_name, &self.compacted_table_schema, results)
    }
}

fn to_record_batch(
    table_name: &str,
    schema: &Arc<Schema>,
    query_results: Option<Vec<CompactedDataSystemTableQueryResult>>,
) -> Result<RecordBatch, DataFusionError> {
    let results =
        query_results.ok_or_else(|| DataFusionError::Plan("Cannot get results".to_owned()))?;

    let mut table_name_array = StringViewBuilder::with_capacity(results.len());
    let mut gen_id_arr = UInt64Builder::with_capacity(results.len());
    let mut gen_level_arr = UInt8Builder::with_capacity(results.len());
    let mut gen_time_arr = StringViewBuilder::with_capacity(results.len());
    let mut parquet_id_arr = UInt64Builder::with_capacity(results.len());
    let mut parquet_path_arr = StringViewBuilder::with_capacity(results.len());
    let mut parquet_size_bytes_arr = UInt64Builder::with_capacity(results.len());
    let mut parquet_row_count_arr = UInt64Builder::with_capacity(results.len());
    let mut parquet_chunk_time_arr = TimestampNanosecondBuilder::with_capacity(results.len());
    let mut parquet_min_time_arr = TimestampNanosecondBuilder::with_capacity(results.len());
    let mut parquet_max_time_arr = TimestampNanosecondBuilder::with_capacity(results.len());

    for result in &results {
        table_name_array.append_value(table_name);
        gen_id_arr.append_value(result.generation_id);
        gen_level_arr.append_value(result.generation_level);
        gen_time_arr.append_value(&result.generation_time);

        for parquet_file in &result.parquet_files {
            parquet_id_arr.append_value(parquet_file.id.as_u64());
            parquet_path_arr.append_value(&parquet_file.path);
            parquet_size_bytes_arr.append_value(parquet_file.size_bytes);
            parquet_row_count_arr.append_value(parquet_file.row_count);
            parquet_chunk_time_arr.append_value(parquet_file.chunk_time);
            parquet_min_time_arr.append_value(parquet_file.min_time);
            parquet_max_time_arr.append_value(parquet_file.max_time);
        }
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(table_name_array.finish()),
        Arc::new(gen_id_arr.finish()),
        Arc::new(gen_level_arr.finish()),
        Arc::new(gen_time_arr.finish()),
        Arc::new(parquet_id_arr.finish()),
        Arc::new(parquet_path_arr.finish()),
        Arc::new(parquet_size_bytes_arr.finish()),
        Arc::new(parquet_row_count_arr.finish()),
        Arc::new(parquet_chunk_time_arr.finish()),
        Arc::new(parquet_min_time_arr.finish()),
        Arc::new(parquet_max_time_arr.finish()),
    ];
    Ok(RecordBatch::try_new(Arc::clone(schema), columns)?)
}

#[cfg(test)]
mod tests {
    use datafusion::{
        logical_expr::{BinaryExpr, Operator},
        scalar::ScalarValue,
    };
    use influxdb3_id::{DbId, ParquetFileId, TableId};
    use influxdb3_write::ParquetFile;
    use observability_deps::tracing::info;
    use pretty_assertions::assert_eq;

    use super::*;

    #[derive(Debug)]
    struct MockCompactedDataSystemTable {
        db_name: String,
        table_name: String,
    }

    impl MockCompactedDataSystemTable {
        pub fn new(db_name: impl Into<String>, table_name: impl Into<String>) -> Self {
            Self {
                db_name: db_name.into(),
                table_name: table_name.into(),
            }
        }
    }

    impl CompactedDataSystemTableView for MockCompactedDataSystemTable {
        fn query(
            &self,
            db_name: &str,
            table_name: &str,
        ) -> Option<Vec<CompactedDataSystemTableQueryResult>> {
            assert_eq!(self.db_name, db_name);
            assert_eq!(self.table_name, table_name);

            Some(vec![CompactedDataSystemTableQueryResult {
                generation_id: 1,
                generation_level: 2,
                generation_time: "2024-01-02/23-00".to_owned(),
                parquet_files: vec![Arc::new(ParquetFile {
                    id: ParquetFileId::new(),
                    path: "/some/path.parquet".to_owned(),
                    size_bytes: 450_000,
                    row_count: 100_000,
                    chunk_time: 1234567890000000000,
                    min_time: 1234567890000000000,
                    max_time: 1234567890000000000,
                })],
            }])
        }

        fn catalog(&self) -> &influxdb3_pro_compactor::catalog::CompactedCatalog {
            todo!()
        }
    }

    #[test_log::test(tokio::test)]
    async fn test_query_compacted_data_sys_table() {
        let db_name = Arc::from("foo");
        let db_id = DbId::new();
        let table_id = TableId::new();
        let table_name = "bar_table";

        let mut db_schema = DatabaseSchema::new(db_id, db_name);
        db_schema.table_map.insert(table_id, Arc::from(table_name));
        let compacted_data_table = CompactedDataTable::new(
            Arc::new(db_schema),
            Some(Arc::new(MockCompactedDataSystemTable::new(
                "foo",
                "bar_table",
            ))),
        );
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column("table_name".into())),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(ScalarValue::Utf8(Some(
                table_name.to_owned(),
            )))),
        });

        let result = compacted_data_table.scan(Some(vec![expr]), None).await;

        info!(res = ?result, "Result from scanning");
        assert!(result.is_ok());
        let record_batch = result.unwrap();
        assert_eq!(1, record_batch.num_rows());
        assert_eq!(11, record_batch.columns().len());
    }

    #[test_log::test(tokio::test)]
    async fn test_query_compacted_data_sys_table_no_compaction() {
        let db_name = Arc::from("foo");
        let db_id = DbId::new();
        let table_id = TableId::new();
        let table_name = "bar_table";

        let mut db_schema = DatabaseSchema::new(db_id, db_name);
        db_schema.table_map.insert(table_id, Arc::from(table_name));
        let compacted_data_table = CompactedDataTable::new(Arc::new(db_schema), None);
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column("table_name".into())),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(ScalarValue::Utf8(Some(
                table_name.to_owned(),
            )))),
        });

        let result = compacted_data_table.scan(Some(vec![expr]), None).await;

        info!(res = ?result, "Result from scanning");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(
            "Compaction not setup, cannot fetch compaction details".to_owned(),
            err.message()
        );
    }

    #[test_log::test(tokio::test)]
    async fn test_query_compacted_data_sys_table_missing_table_id() {
        let db_name = Arc::from("foo");
        let db_id = DbId::new();
        let table_name = "bar_table";

        // schema has no table populated so table id translation should fail
        let db_schema = DatabaseSchema::new(db_id, db_name);
        let compacted_data_table = CompactedDataTable::new(
            Arc::new(db_schema),
            Some(Arc::new(MockCompactedDataSystemTable::new(
                "foo",
                "bar_table",
            ))),
        );
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column("table_name".into())),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(ScalarValue::Utf8(Some(
                table_name.to_owned(),
            )))),
        });

        let result = compacted_data_table.scan(Some(vec![expr]), None).await;

        info!(res = ?result, "Result from scanning");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!("Cannot find table name".to_owned(), err.message());
    }

    #[test_log::test(tokio::test)]
    async fn test_query_compacted_data_sys_table_missing_table() {
        let db_name = Arc::from("foo");
        let db_id = DbId::new();
        let table_id = TableId::new();
        let table_name = "bar_table";

        let mut db_schema = DatabaseSchema::new(db_id, db_name);
        db_schema.table_map.insert(table_id, Arc::from(table_name));
        let compacted_data_table = CompactedDataTable::new(
            Arc::new(db_schema),
            Some(Arc::new(MockCompactedDataSystemTable::new(
                "foo",
                "bar_table",
            ))),
        );
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column("table_name".into())),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(ScalarValue::Null)),
        });

        let result = compacted_data_table.scan(Some(vec![expr]), None).await;

        info!(res = ?result, "Result from scanning");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(
            "must provide a table_name = '<table_name>' predicate in queries to system.compacted_data".to_owned(),
            err.message()
        );
    }
}
