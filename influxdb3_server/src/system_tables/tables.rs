use std::sync::Arc;

use arrow::array::{StringViewBuilder, UInt64Builder};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::{error::DataFusionError, logical_expr::Expr};
use influxdb3_catalog::catalog::Catalog;
use iox_system_tables::IoxSystemTable;

#[derive(Debug)]
pub(super) struct TablesTable {
    catalog: Arc<Catalog>,
    schema: SchemaRef,
}

impl TablesTable {
    pub(super) fn new(catalog: Arc<Catalog>) -> Self {
        Self {
            catalog,
            schema: tables_schema(),
        }
    }
}

fn tables_schema() -> SchemaRef {
    let columns = vec![
        Field::new("database_name", DataType::Utf8View, false),
        Field::new("table_name", DataType::Utf8View, false),
        Field::new("column_count", DataType::UInt64, false),
        Field::new("series_key_columns", DataType::Utf8View, false),
        Field::new("last_cache_count", DataType::UInt64, false),
        Field::new("distinct_cache_count", DataType::UInt64, false),
        Field::new("deleted", DataType::Boolean, false),
        Field::new(
            "hard_deletion_date",
            DataType::Timestamp(TimeUnit::Second, None),
            true,
        ),
    ];
    Arc::new(Schema::new(columns))
}

#[async_trait::async_trait]
impl IoxSystemTable for TablesTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    async fn scan(
        &self,
        _filters: Option<Vec<Expr>>,
        _limit: Option<usize>,
    ) -> Result<RecordBatch, DataFusionError> {
        let databases = self.catalog.list_db_schema();

        // Count total tables across all databases
        let total_tables: usize = databases
            .iter()
            .map(|db| db.tables.resource_iter().count())
            .sum();

        let mut database_name_arr = StringViewBuilder::with_capacity(total_tables);
        let mut table_name_arr = StringViewBuilder::with_capacity(total_tables);
        let mut column_count_arr = UInt64Builder::with_capacity(total_tables);
        let mut series_key_arr = StringViewBuilder::with_capacity(total_tables);
        let mut last_cache_count_arr = UInt64Builder::with_capacity(total_tables);
        let mut distinct_cache_count_arr = UInt64Builder::with_capacity(total_tables);
        let mut deleted_arr = arrow::array::BooleanBuilder::with_capacity(total_tables);
        let mut hard_deletion_date_arr =
            arrow::array::TimestampSecondBuilder::with_capacity(total_tables);

        for db in databases {
            for table in db.tables.resource_iter() {
                database_name_arr.append_value(&db.name);
                table_name_arr.append_value(&table.table_name);
                column_count_arr.append_value(table.columns.resource_iter().count() as u64);

                // Build series key string
                let series_key_str = table.series_key_names.join(", ");
                series_key_arr.append_value(&series_key_str);

                last_cache_count_arr.append_value(table.last_caches.resource_iter().count() as u64);
                distinct_cache_count_arr
                    .append_value(table.distinct_caches.resource_iter().count() as u64);
                deleted_arr.append_value(table.deleted);

                if let Some(hard_delete_time) = &table.hard_delete_time {
                    hard_deletion_date_arr.append_value(hard_delete_time.timestamp())
                } else {
                    hard_deletion_date_arr.append_null()
                }
            }
        }

        let columns: Vec<ArrayRef> = vec![
            Arc::new(database_name_arr.finish()),
            Arc::new(table_name_arr.finish()),
            Arc::new(column_count_arr.finish()),
            Arc::new(series_key_arr.finish()),
            Arc::new(last_cache_count_arr.finish()),
            Arc::new(distinct_cache_count_arr.finish()),
            Arc::new(deleted_arr.finish()),
            Arc::new(hard_deletion_date_arr.finish()),
        ];

        RecordBatch::try_new(self.schema(), columns).map_err(DataFusionError::from)
    }
}
