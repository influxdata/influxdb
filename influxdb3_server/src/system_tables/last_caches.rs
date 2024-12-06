use std::sync::Arc;

use arrow::array::{GenericListBuilder, StringViewBuilder, UInt32Builder, UInt64Builder};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::{error::DataFusionError, logical_expr::Expr};
use influxdb3_cache::last_cache::LastCacheProvider;
use influxdb3_catalog::catalog::DatabaseSchema;
use influxdb3_wal::{LastCacheDefinition, LastCacheValueColumnsDef};
use iox_system_tables::IoxSystemTable;

#[derive(Debug)]
pub(super) struct LastCachesTable {
    db_schema: Arc<DatabaseSchema>,
    schema: SchemaRef,
    provider: Arc<LastCacheProvider>,
}

impl LastCachesTable {
    pub(super) fn new(db_schema: Arc<DatabaseSchema>, provider: Arc<LastCacheProvider>) -> Self {
        Self {
            db_schema,
            schema: last_caches_schema(),
            provider,
        }
    }
}

fn last_caches_schema() -> SchemaRef {
    let columns = vec![
        Field::new("table", DataType::Utf8View, false),
        Field::new("name", DataType::Utf8View, false),
        Field::new(
            "key_column_ids",
            DataType::List(Arc::new(Field::new("item", DataType::UInt32, true))),
            false,
        ),
        Field::new(
            "key_column_names",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8View, true))),
            false,
        ),
        Field::new(
            "value_column_ids",
            DataType::List(Arc::new(Field::new("item", DataType::UInt32, true))),
            true,
        ),
        Field::new(
            "value_column_names",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8View, true))),
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
        let caches = self.provider.get_last_caches_for_db(self.db_schema.id);
        from_last_cache_definitions(&self.db_schema, self.schema(), &caches)
    }
}

fn from_last_cache_definitions(
    db_schema: &DatabaseSchema,
    sys_table_schema: SchemaRef,
    cache_defns: &[LastCacheDefinition],
) -> Result<RecordBatch, DataFusionError> {
    let mut table_name_arr = StringViewBuilder::with_capacity(cache_defns.len());
    let mut cache_name_arr = StringViewBuilder::with_capacity(cache_defns.len());

    let key_col_id_builder = UInt32Builder::new();
    let mut key_col_ids_arr = GenericListBuilder::<i32, UInt32Builder>::with_capacity(
        key_col_id_builder,
        cache_defns.len(),
    );

    let key_col_name_builder = StringViewBuilder::new();
    let mut key_col_names_arr = GenericListBuilder::<i32, StringViewBuilder>::with_capacity(
        key_col_name_builder,
        cache_defns.len(),
    );

    let value_col_builder = UInt32Builder::new();
    let mut value_col_ids_arr = GenericListBuilder::<i32, UInt32Builder>::with_capacity(
        value_col_builder,
        cache_defns.len(),
    );

    let value_col_name_builder = StringViewBuilder::new();
    let mut value_col_names_arr = GenericListBuilder::<i32, StringViewBuilder>::with_capacity(
        value_col_name_builder,
        cache_defns.len(),
    );
    let mut count_arr = UInt64Builder::with_capacity(cache_defns.len());
    let mut ttl_arr = UInt64Builder::with_capacity(cache_defns.len());

    for cache_defn in cache_defns {
        let table_defn = db_schema
            .table_definition_by_id(&cache_defn.table_id)
            .expect("table should exist for last cache");

        table_name_arr.append_value(&cache_defn.table);
        cache_name_arr.append_value(&cache_defn.name);

        for key_col in &cache_defn.key_columns {
            key_col_ids_arr.values().append_value(key_col.as_u32());
            let col_name = table_defn
                .column_id_to_name(key_col)
                .expect("column id should have name associated to it");
            key_col_names_arr.values().append_value(col_name);
        }
        key_col_ids_arr.append(true);
        key_col_names_arr.append(true);

        match &cache_defn.value_columns {
            LastCacheValueColumnsDef::Explicit { columns } => {
                for col in columns {
                    let col_name = table_defn
                        .column_id_to_name(col)
                        .expect("column id should have name associated to it");
                    value_col_ids_arr.values().append_value(col.as_u32());
                    value_col_names_arr.values().append_value(col_name);
                }
                value_col_ids_arr.append(true);
                value_col_names_arr.append(true);
            }
            LastCacheValueColumnsDef::AllNonKeyColumns => {
                value_col_ids_arr.append_null();
                value_col_names_arr.append_null();
            }
        }

        count_arr.append_value(cache_defn.count.into());
        ttl_arr.append_value(cache_defn.ttl);
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(table_name_arr.finish()),
        Arc::new(cache_name_arr.finish()),
        Arc::new(key_col_ids_arr.finish()),
        Arc::new(key_col_names_arr.finish()),
        Arc::new(value_col_ids_arr.finish()),
        Arc::new(value_col_names_arr.finish()),
        Arc::new(count_arr.finish()),
        Arc::new(ttl_arr.finish()),
    ];

    let record_batch = RecordBatch::try_new(sys_table_schema, columns)?;
    Ok(record_batch)
}
