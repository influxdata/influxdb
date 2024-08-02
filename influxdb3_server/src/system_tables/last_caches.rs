use std::sync::Arc;

use arrow::array::{GenericListBuilder, StringBuilder};
use arrow_array::{ArrayRef, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::{error::DataFusionError, logical_expr::Expr};
use influxdb3_catalog::catalog::{LastCacheDefinition, LastCacheValueColumnsDef};
use influxdb3_write::last_cache::LastCacheProvider;
use iox_system_tables::IoxSystemTable;

pub(super) struct LastCachesTable {
    db_name: String,
    schema: SchemaRef,
    provider: Arc<LastCacheProvider>,
}

impl LastCachesTable {
    pub(super) fn new(db: impl Into<String>, provider: Arc<LastCacheProvider>) -> Self {
        Self {
            db_name: db.into(),
            schema: last_caches_schema(),
            provider,
        }
    }
}

fn last_caches_schema() -> SchemaRef {
    let columns = vec![
        Field::new("table", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new(
            "key_columns",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            false,
        ),
        Field::new(
            "value_columns",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
        Field::new("count", DataType::UInt64, false),
        Field::new("ttl", DataType::UInt64, false),
    ];
    Arc::new(Schema::new(columns))
}

#[async_trait::async_trait]
impl IoxSystemTable for LastCachesTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    async fn scan(
        &self,
        _filters: Option<Vec<Expr>>,
        _limit: Option<usize>,
    ) -> Result<RecordBatch, DataFusionError> {
        let caches = self.provider.get_last_caches_for_db(&self.db_name);
        from_last_cache_definitions(self.schema(), &caches)
    }
}

fn from_last_cache_definitions(
    schema: SchemaRef,
    caches: &[LastCacheDefinition],
) -> Result<RecordBatch, DataFusionError> {
    let mut columns: Vec<ArrayRef> = vec![];

    // Table Name
    columns.push(Arc::new(
        caches
            .iter()
            .map(|c| Some(c.table.to_string()))
            .collect::<StringArray>(),
    ));
    // Cache Name
    columns.push(Arc::new(
        caches
            .iter()
            .map(|c| Some(c.name.to_string()))
            .collect::<StringArray>(),
    ));
    // Key Columns
    columns.push({
        let values_builder = StringBuilder::new();
        let mut builder = GenericListBuilder::<i32, _>::new(values_builder);

        for c in caches {
            c.key_columns
                .iter()
                .for_each(|k| builder.values().append_value(k));
            builder.append(true);
        }
        Arc::new(builder.finish())
    });
    // Value Columns
    columns.push({
        let values_builder = StringBuilder::new();
        let mut builder = GenericListBuilder::<i32, _>::new(values_builder);

        for c in caches {
            match &c.value_columns {
                LastCacheValueColumnsDef::Explicit { columns } => {
                    columns
                        .iter()
                        .for_each(|v| builder.values().append_value(v));
                    builder.append(true);
                }
                LastCacheValueColumnsDef::AllNonKeyColumns => {
                    builder.append_null();
                }
            }
        }
        Arc::new(builder.finish())
    });
    columns.push(Arc::new(
        caches
            .iter()
            .map(|e| Some(e.count.into()))
            .collect::<UInt64Array>(),
    ));
    columns.push(Arc::new(
        caches.iter().map(|e| Some(e.ttl)).collect::<UInt64Array>(),
    ));

    Ok(RecordBatch::try_new(schema, columns)?)
}
