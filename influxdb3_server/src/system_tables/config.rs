use arrow::array::GenericListBuilder;
use arrow::array::StringViewBuilder;
use arrow::array::UInt32Builder;
use influxdb3_id::ColumnId;
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::{error::DataFusionError, logical_expr::Expr};
use influxdb3_catalog::catalog::Catalog;
use influxdb3_config::ProConfig;
use iox_system_tables::IoxSystemTable;
use tokio::sync::RwLock;

#[derive(Debug)]
pub(super) struct FileIndexTable {
    catalog: Arc<Catalog>,
    config: Arc<RwLock<ProConfig>>,
    schema: SchemaRef,
}

impl FileIndexTable {
    pub(super) fn new(catalog: Arc<Catalog>, config: Arc<RwLock<ProConfig>>) -> Self {
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
                DataType::List(Arc::new(Field::new("item", DataType::UInt32, true))),
                false,
            ),
        ];
        Arc::new(Schema::new(columns))
    }
    async fn file_index_tables(&self) -> Result<RecordBatch, DataFusionError> {
        let config = self.config.read().await;

        #[derive(Clone)]
        struct Line {
            db: Arc<str>,
            table: Option<Arc<str>>,
            columns: Vec<Arc<str>>,
            column_ids: Vec<ColumnId>,
        }

        let mut lines = Vec::new();
        for (db_id, idx) in &config.file_index_columns {
            let line = Line {
                db: self.catalog.db_id_to_name(db_id).unwrap(),
                table: None,
                columns: idx.db_columns.clone(),
                // We don't know the column ids for the db level so we leave this empty by default
                column_ids: Vec::new(),
            };

            if !line.columns.is_empty() {
                lines.push(line.clone());
            }

            for (table_id, columns) in &idx.table_columns {
                let mut line = line.clone();
                let table_def = self
                    .catalog
                    .db_schema_by_id(db_id)
                    .unwrap()
                    .table_definition_by_id(table_id)
                    .unwrap();
                let table_name = Arc::clone(&table_def.table_name);
                line.table = Some(table_name);
                for name in &line.columns {
                    line.column_ids
                        .push(table_def.column_name_to_id_unchecked(Arc::clone(name)));
                }
                line.column_ids.extend_from_slice(columns);
                line.columns.extend(
                    columns
                        .iter()
                        .map(|c| table_def.column_id_to_name_unchecked(c)),
                );
                lines.push(line);
            }
        }
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
                let uint_builder = UInt32Builder::new();
                let mut list_builder = GenericListBuilder::<i32, UInt32Builder>::with_capacity(
                    uint_builder,
                    lines.len(),
                );
                for line in &lines {
                    list_builder.append_value(
                        line.column_ids
                            .iter()
                            .map(|id| Some(id.as_u32()))
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
