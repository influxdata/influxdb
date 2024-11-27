use std::sync::Arc;

use arrow::array::{GenericListBuilder, StringViewBuilder, UInt32Builder, UInt64Builder};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::{error::DataFusionError, prelude::Expr};
use influxdb3_cache::meta_cache::MetaCacheProvider;
use influxdb3_catalog::catalog::DatabaseSchema;
use influxdb3_wal::MetaCacheDefinition;
use iox_system_tables::IoxSystemTable;

pub(super) struct MetaCachesTable {
    db_schema: Arc<DatabaseSchema>,
    schema: SchemaRef,
    provider: Arc<MetaCacheProvider>,
}

impl MetaCachesTable {
    pub(super) fn new(db_schema: Arc<DatabaseSchema>, provider: Arc<MetaCacheProvider>) -> Self {
        Self {
            db_schema,
            schema: meta_caches_schema(),
            provider,
        }
    }
}

fn meta_caches_schema() -> SchemaRef {
    let columns = vec![
        Field::new("table", DataType::Utf8View, false),
        Field::new("name", DataType::Utf8View, false),
        Field::new(
            "column_ids",
            DataType::List(Arc::new(Field::new("item", DataType::UInt32, true))),
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
impl IoxSystemTable for MetaCachesTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    async fn scan(
        &self,
        _filters: Option<Vec<Expr>>,
        _limit: Option<usize>,
    ) -> Result<RecordBatch, DataFusionError> {
        let caches = self
            .provider
            .get_cache_definitions_for_db(&self.db_schema.id);
        from_meta_cache_definitions(&self.db_schema, self.schema(), &caches)
    }
}

fn from_meta_cache_definitions(
    db_schema: &DatabaseSchema,
    sys_table_schema: SchemaRef,
    cache_definitions: &[MetaCacheDefinition],
) -> Result<RecordBatch, DataFusionError> {
    let mut table_name_arr = StringViewBuilder::with_capacity(cache_definitions.len());
    let mut cache_name_arr = StringViewBuilder::with_capacity(cache_definitions.len());

    let col_id_builder = UInt32Builder::new();
    let mut col_id_arr = GenericListBuilder::<i32, UInt32Builder>::with_capacity(
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
            .expect("table should exist for metadata cache");

        table_name_arr.append_value(&cache.table_name);
        cache_name_arr.append_value(&cache.cache_name);

        // loop to create the list of column id and name values for their respective columns
        for col in &cache.column_ids {
            col_id_arr.values().append_value(col.as_u32());
            let col_name = table_def
                .column_id_to_name(col)
                .expect("column id should have associated name");
            col_name_arr.values().append_value(col_name);
        }
        // finish the list built using the for loop:
        col_id_arr.append(true);
        col_name_arr.append(true);

        max_cardinality_arr.append_value(cache.max_cardinality as u64);
        max_age_arr.append_value(cache.max_age_seconds);
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
