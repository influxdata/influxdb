use std::sync::Arc;

use arrow::array::{GenericListBuilder, StringViewBuilder, UInt16Builder, UInt64Builder};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::{error::DataFusionError, prelude::Expr};
use influxdb3_catalog::{catalog::DatabaseSchema, log::CreateDistinctCacheLog};
use iox_system_tables::IoxSystemTable;

#[derive(Debug)]
pub(super) struct DistinctCachesTable {
    db_schema: Arc<DatabaseSchema>,
    schema: SchemaRef,
}

impl DistinctCachesTable {
    pub(super) fn new(db_schema: Arc<DatabaseSchema>) -> Self {
        Self {
            db_schema,
            schema: distinct_caches_schema(),
        }
    }
}

fn distinct_caches_schema() -> SchemaRef {
    let columns = vec![
        Field::new("table", DataType::Utf8View, false),
        Field::new("name", DataType::Utf8View, false),
        Field::new(
            "column_ids",
            DataType::List(Arc::new(Field::new("item", DataType::UInt16, true))),
            false,
        ),
        Field::new(
            "column_names",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8View, true))),
            false,
        ),
        Field::new("max_cardinality", DataType::UInt64, false),
        Field::new("max_age_seconds", DataType::UInt64, false),
    ];
    Arc::new(Schema::new(columns))
}

#[async_trait::async_trait]
impl IoxSystemTable for DistinctCachesTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    async fn scan(
        &self,
        _filters: Option<Vec<Expr>>,
        _limit: Option<usize>,
    ) -> Result<RecordBatch, DataFusionError> {
        let caches = self.db_schema.list_distinct_caches();
        from_distinct_cache_definitions(&self.db_schema, self.schema(), caches)
    }
}

fn from_distinct_cache_definitions(
    db_schema: &DatabaseSchema,
    sys_table_schema: SchemaRef,
    cache_definitions: Vec<&CreateDistinctCacheLog>,
) -> Result<RecordBatch, DataFusionError> {
    let mut table_name_arr = StringViewBuilder::with_capacity(cache_definitions.len());
    let mut cache_name_arr = StringViewBuilder::with_capacity(cache_definitions.len());

    let col_id_builder = UInt16Builder::new();
    let mut col_id_arr = GenericListBuilder::<i32, UInt16Builder>::with_capacity(
        col_id_builder,
        cache_definitions.len(),
    );

    let col_name_builder = StringViewBuilder::new();
    let mut col_name_arr = GenericListBuilder::<i32, StringViewBuilder>::with_capacity(
        col_name_builder,
        cache_definitions.len(),
    );

    let mut max_cardinality_arr = UInt64Builder::with_capacity(cache_definitions.len());
    let mut max_age_arr = UInt64Builder::with_capacity(cache_definitions.len());

    for cache in cache_definitions {
        let table_def = db_schema
            .table_definition_by_id(&cache.table_id)
            .expect("table should exist for distinct value cache");

        table_name_arr.append_value(&cache.table_name);
        cache_name_arr.append_value(&cache.cache_name);

        // loop to create the list of column id and name values for their respective columns
        for col in &cache.column_ids {
            col_id_arr.values().append_value(col.get());
            let col_name = table_def
                .column_id_to_name(col)
                .expect("column id should have associated name");
            col_name_arr.values().append_value(col_name);
        }
        // finish the list built using the for loop:
        col_id_arr.append(true);
        col_name_arr.append(true);

        max_cardinality_arr.append_value(cache.max_cardinality.to_u64());
        max_age_arr.append_value(cache.max_age_seconds.as_secs());
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(table_name_arr.finish()),
        Arc::new(cache_name_arr.finish()),
        Arc::new(col_id_arr.finish()),
        Arc::new(col_name_arr.finish()),
        Arc::new(max_cardinality_arr.finish()),
        Arc::new(max_age_arr.finish()),
    ];

    RecordBatch::try_new(sys_table_schema, columns).map_err(Into::into)
}
