use std::sync::Arc;

use arrow::array::{StringViewBuilder, UInt64Builder};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::{error::DataFusionError, logical_expr::Expr};
use influxdb3_catalog::catalog::{Catalog, RetentionPeriod};
use iox_system_tables::IoxSystemTable;

#[derive(Debug)]
pub(super) struct DatabasesTable {
    catalog: Arc<Catalog>,
    schema: SchemaRef,
}

impl DatabasesTable {
    pub(super) fn new(catalog: Arc<Catalog>) -> Self {
        Self {
            catalog,
            schema: databases_schema(),
        }
    }
}

fn databases_schema() -> SchemaRef {
    let columns = vec![
        Field::new("database_name", DataType::Utf8View, false),
        Field::new("retention_period_ns", DataType::UInt64, true),
        Field::new("deleted", DataType::Boolean, false),
    ];
    Arc::new(Schema::new(columns))
}

#[async_trait::async_trait]
impl IoxSystemTable for DatabasesTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    async fn scan(
        &self,
        _filters: Option<Vec<Expr>>,
        _limit: Option<usize>,
    ) -> Result<RecordBatch, DataFusionError> {
        let databases = self.catalog.list_db_schema();

        let mut database_name_arr = StringViewBuilder::with_capacity(databases.len());
        let mut retention_period_arr = UInt64Builder::with_capacity(databases.len());
        let mut deleted_arr = arrow::array::BooleanBuilder::with_capacity(databases.len());

        for db in databases {
            database_name_arr.append_value(&db.name);

            match db.retention_period {
                RetentionPeriod::Indefinite => retention_period_arr.append_null(),
                RetentionPeriod::Duration(duration) => {
                    retention_period_arr.append_value(duration.as_nanos() as u64);
                }
            }

            deleted_arr.append_value(db.deleted);
        }

        let columns: Vec<ArrayRef> = vec![
            Arc::new(database_name_arr.finish()),
            Arc::new(retention_period_arr.finish()),
            Arc::new(deleted_arr.finish()),
        ];

        RecordBatch::try_new(self.schema(), columns).map_err(DataFusionError::from)
    }
}
