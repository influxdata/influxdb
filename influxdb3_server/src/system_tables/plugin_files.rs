use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::common::Result;
use datafusion::logical_expr::Expr;
use influxdb3_processing_engine::ProcessingEngineManagerImpl;
use iox_system_tables::IoxSystemTable;
use std::sync::Arc;

#[derive(Debug)]
pub(super) struct PluginFilesTable {
    schema: SchemaRef,
    processing_engine: Option<Arc<ProcessingEngineManagerImpl>>,
}

impl PluginFilesTable {
    pub(super) fn new(processing_engine: Option<Arc<ProcessingEngineManagerImpl>>) -> Self {
        Self {
            schema: plugin_files_schema(),
            processing_engine,
        }
    }
}

fn plugin_files_schema() -> SchemaRef {
    let columns = vec![
        Field::new("plugin_name", DataType::Utf8, false),
        Field::new("file_name", DataType::Utf8, false),
        Field::new("file_path", DataType::Utf8, false),
        Field::new("file_content_hash", DataType::Utf8, false),
        Field::new("size_bytes", DataType::Int64, false),
        Field::new("last_modified", DataType::Int64, false),
    ];
    Schema::new(columns).into()
}

#[async_trait]
impl IoxSystemTable for PluginFilesTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    async fn scan(
        &self,
        _filters: Option<Vec<Expr>>,
        _limit: Option<usize>,
    ) -> Result<RecordBatch> {
        let Some(processing_engine) = &self.processing_engine else {
            return Ok(RecordBatch::new_empty(Arc::clone(&self.schema)));
        };

        let plugin_files = processing_engine.list_plugin_files().await;

        let mut plugin_names = Vec::new();
        let mut file_names = Vec::new();
        let mut file_paths = Vec::new();
        let mut file_hashes = Vec::new();
        let mut sizes = Vec::new();
        let mut last_modifieds = Vec::new();

        for file_info in plugin_files {
            plugin_names.push(Some(file_info.plugin_name.to_string()));
            file_names.push(Some(file_info.file_name.to_string()));
            file_paths.push(Some(file_info.file_path.to_string()));
            file_hashes.push(Some(file_info.content_hash.to_string()));
            sizes.push(Some(file_info.size_bytes));
            last_modifieds.push(Some(file_info.last_modified_millis));
        }

        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(plugin_names)),
            Arc::new(StringArray::from(file_names)),
            Arc::new(StringArray::from(file_paths)),
            Arc::new(StringArray::from(file_hashes)),
            Arc::new(Int64Array::from(sizes)),
            Arc::new(Int64Array::from(last_modifieds)),
        ];

        Ok(RecordBatch::try_new(Arc::clone(&self.schema), columns)?)
    }
}
