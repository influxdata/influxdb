use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::{error::DataFusionError, logical_expr::Expr};
use influxdb3_catalog::catalog::Catalog;
use influxdb3_config::ProConfig;
use iox_system_tables::IoxSystemTable;
use tokio::sync::RwLock;

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
            Field::new("index_columns", DataType::Utf8, false),
        ];
        Arc::new(Schema::new(columns))
    }
    async fn file_index_tables(&self) -> Result<RecordBatch, DataFusionError> {
        let config = self.config.read().await;

        #[derive(Clone)]
        struct Line {
            db: Arc<str>,
            table: Option<Arc<str>>,
            columns: String,
        }

        let mut lines = Vec::new();
        for (db_id, idx) in &config.file_index_columns {
            let line = Line {
                db: self.catalog.db_id_to_name(db_id).unwrap(),
                table: None,
                columns: idx.db_columns.clone().join(","),
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
                line.columns = if !line.columns.is_empty() {
                    [
                        line.columns,
                        columns
                            .iter()
                            .map(|c| table_def.column_id_to_name(c).unwrap())
                            .collect::<Vec<_>>()
                            .join(","),
                    ]
                    .join(",")
                } else {
                    columns
                        .iter()
                        .map(|c| table_def.column_id_to_name(c).unwrap())
                        .collect::<Vec<_>>()
                        .join(",")
                };
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
            Arc::new(
                lines
                    .into_iter()
                    .map(|line| Some(line.columns))
                    .collect::<StringArray>(),
            ),
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
