//! `system.plugin_files` system table implementation [`PluginsTable`]
use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::common::Result;
use datafusion::logical_expr::Expr;
use iox_system_tables::IoxSystemTable;

/// API to provide information about plugin files to [`PluginsTable`]
#[async_trait]
pub trait PluginsSource: std::fmt::Debug + Send + Sync {
    async fn list_plugin_files(&self) -> Vec<PluginFileRow>;
}

/// A single plugin file, as exposed by the `system.plugin_files` table.
#[derive(Debug, Clone)]
pub struct PluginFileRow {
    pub plugin_name: Arc<str>,
    pub file_name: Arc<str>,
    pub file_path: Arc<str>,
    pub size_bytes: i64,
    pub last_modified_millis: i64,
}

/// `system.plugin_files` system table implementation
#[derive(Debug)]
pub struct PluginsTable {
    schema: SchemaRef,
    source: Option<Arc<dyn PluginsSource>>,
}

impl PluginsTable {
    pub fn new(source: Option<Arc<dyn PluginsSource>>) -> Self {
        Self {
            schema: plugins_schema(),
            source,
        }
    }
}

fn plugins_schema() -> SchemaRef {
    let columns = vec![
        Field::new("plugin_name", DataType::Utf8, false),
        Field::new("file_name", DataType::Utf8, false),
        Field::new("file_path", DataType::Utf8, false),
        Field::new("size_bytes", DataType::Int64, false),
        Field::new("last_modified", DataType::Int64, false),
    ];
    Schema::new(columns).into()
}

#[async_trait]
impl IoxSystemTable for PluginsTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    async fn scan(
        &self,
        _filters: Option<Vec<Expr>>,
        _limit: Option<usize>,
    ) -> Result<RecordBatch> {
        let Some(source) = &self.source else {
            return Ok(RecordBatch::new_empty(Arc::clone(&self.schema)));
        };

        let plugin_files = source.list_plugin_files().await;

        let mut plugin_names = Vec::new();
        let mut file_names = Vec::new();
        let mut file_paths = Vec::new();
        let mut sizes = Vec::new();
        let mut last_modifieds = Vec::new();

        for file_info in plugin_files {
            plugin_names.push(Some(file_info.plugin_name.to_string()));
            file_names.push(Some(file_info.file_name.to_string()));
            file_paths.push(Some(file_info.file_path.to_string()));
            sizes.push(Some(file_info.size_bytes));
            last_modifieds.push(Some(file_info.last_modified_millis));
        }

        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(plugin_names)),
            Arc::new(StringArray::from(file_names)),
            Arc::new(StringArray::from(file_paths)),
            Arc::new(Int64Array::from(sizes)),
            Arc::new(Int64Array::from(last_modifieds)),
        ];

        Ok(RecordBatch::try_new(Arc::clone(&self.schema), columns)?)
    }
}
