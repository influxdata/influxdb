use arrow::array::{UInt8Builder, UInt64Builder};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::{error::DataFusionError, prelude::Expr};
use iox_system_tables::IoxSystemTable;
use std::{sync::Arc, time::Duration};

use influxdb3_catalog::catalog::Catalog;

#[derive(Debug)]
pub(super) struct GenerationDurationsTable {
    schema: SchemaRef,
    catalog: Arc<Catalog>,
}

impl GenerationDurationsTable {
    pub(crate) fn new(catalog: Arc<Catalog>) -> Self {
        Self {
            schema: table_schema(),
            catalog,
        }
    }
}

#[async_trait::async_trait]
impl IoxSystemTable for GenerationDurationsTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    async fn scan(
        &self,
        _filters: Option<Vec<Expr>>,
        _limit: Option<usize>,
    ) -> Result<RecordBatch, DataFusionError> {
        let generations = self.catalog.list_generation_durations();
        to_record_batch(&self.schema, generations)
    }
}

fn table_schema() -> SchemaRef {
    let fields = vec![
        Field::new("level", DataType::UInt8, false),
        Field::new("duration_seconds", DataType::UInt64, false),
    ];
    Arc::new(Schema::new(fields))
}

fn to_record_batch(
    schema: &SchemaRef,
    generations: Vec<(u8, Duration)>,
) -> Result<RecordBatch, DataFusionError> {
    let capacity = generations.len();
    let mut level_arr = UInt8Builder::with_capacity(capacity);
    let mut duration_arr = UInt64Builder::with_capacity(capacity);
    for (level, duration) in generations {
        level_arr.append_value(level);
        duration_arr.append_value(duration.as_secs());
    }
    RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(level_arr.finish()),
            Arc::new(duration_arr.finish()),
        ],
    )
    .map_err(Into::into)
}
