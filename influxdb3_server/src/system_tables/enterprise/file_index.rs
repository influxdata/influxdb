use arrow::array::GenericListBuilder;
use arrow::array::StringViewBuilder;
use arrow::array::UInt16Builder;
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::{error::DataFusionError, logical_expr::Expr};
use influxdb3_catalog::catalog::Catalog;
use influxdb3_config::EnterpriseConfig;
use iox_system_tables::IoxSystemTable;

#[derive(Debug)]
pub(crate) struct FileIndexTable {
    catalog: Arc<Catalog>,
    config: Arc<EnterpriseConfig>,
    schema: SchemaRef,
}

impl FileIndexTable {
    pub(crate) fn new(catalog: Arc<Catalog>, config: Arc<EnterpriseConfig>) -> Self {
        Self {
            catalog,
            config,
            schema: Self::file_index_schema(),
        }
    }

    fn file_index_schema() -> SchemaRef {
        let columns = vec![
            Field::new("database_name", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, true),
            Field::new(
                "index_columns",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8View, true))),
                false,
            ),
            Field::new(
                "index_column_ids",
                DataType::List(Arc::new(Field::new("item", DataType::UInt16, true))),
                false,
            ),
        ];
        Arc::new(Schema::new(columns))
    }

    async fn file_index_tables(&self) -> Result<RecordBatch, DataFusionError> {
        let lines = self.config.index_summaries(Arc::clone(&self.catalog));

        let columns: Vec<ArrayRef> = vec![
            Arc::new(
                lines
                    .iter()
                    .map(|line| Some(Arc::clone(&line.db)))
                    .collect::<StringArray>(),
            ),
            Arc::new(
                lines
                    .iter()
                    .map(|line| line.table.clone())
                    .collect::<StringArray>(),
            ),
            {
                let string_builder = StringViewBuilder::new();
                let mut list_builder = GenericListBuilder::<i32, StringViewBuilder>::with_capacity(
                    string_builder,
                    lines.len(),
                );
                for line in &lines {
                    list_builder.append_value(
                        line.columns
                            .iter()
                            .map(|s| Some(s.to_string()))
                            .collect::<Vec<_>>(),
                    );
                }

                Arc::new(list_builder.finish())
            },
            {
                let uint_builder = UInt16Builder::new();
                let mut list_builder = GenericListBuilder::<i32, UInt16Builder>::with_capacity(
                    uint_builder,
                    lines.len(),
                );
                for line in &lines {
                    list_builder.append_value(
                        line.column_ids
                            .iter()
                            .map(|id| Some(id.get()))
                            .collect::<Vec<_>>(),
                    );
                }

                Arc::new(list_builder.finish())
            },
        ];

        Ok(RecordBatch::try_new(Arc::clone(&self.schema), columns)?)
    }
}

#[async_trait]
impl IoxSystemTable for FileIndexTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    async fn scan(
        &self,
        _filters: Option<Vec<Expr>>,
        _limit: Option<usize>,
    ) -> Result<RecordBatch, DataFusionError> {
        self.file_index_tables().await
    }
}
