use arrow_array::{ArrayRef, BooleanArray, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::Expr;
use influxdb3_wal::{PluginDefinition, TriggerDefinition};
use iox_system_tables::IoxSystemTable;
use std::sync::Arc;

#[derive(Debug)]
pub(super) struct ProcessingEnginePluginTable {
    schema: SchemaRef,
    plugins: Vec<PluginDefinition>,
}

fn plugin_schema() -> SchemaRef {
    let columns = vec![
        Field::new("plugin_name", DataType::Utf8, false),
        Field::new("function_name", DataType::Utf8, false),
        Field::new("code", DataType::Utf8, false),
        Field::new("plugin_type", DataType::Utf8, false),
    ];
    Schema::new(columns).into()
}

impl ProcessingEnginePluginTable {
    pub fn new(python_calls: Vec<PluginDefinition>) -> Self {
        Self {
            schema: plugin_schema(),
            plugins: python_calls,
        }
    }
}

#[async_trait]
impl IoxSystemTable for ProcessingEnginePluginTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
    async fn scan(
        &self,
        _filters: Option<Vec<Expr>>,
        _limit: Option<usize>,
    ) -> Result<RecordBatch, DataFusionError> {
        let schema = self.schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(
                self.plugins
                    .iter()
                    .map(|call| Some(call.plugin_name.clone()))
                    .collect::<StringArray>(),
            ),
            Arc::new(
                self.plugins
                    .iter()
                    .map(|p| Some(p.function_name.clone()))
                    .collect::<StringArray>(),
            ),
            Arc::new(
                self.plugins
                    .iter()
                    .map(|p| Some(p.code.clone()))
                    .collect::<StringArray>(),
            ),
            Arc::new(
                self.plugins
                    .iter()
                    .map(|p| serde_json::to_string(&p.plugin_type).ok())
                    .collect::<StringArray>(),
            ),
        ];
        Ok(RecordBatch::try_new(schema, columns)?)
    }
}

#[derive(Debug)]
pub(super) struct ProcessingEngineTriggerTable {
    schema: SchemaRef,
    triggers: Vec<TriggerDefinition>,
}

impl ProcessingEngineTriggerTable {
    pub fn new(triggers: Vec<TriggerDefinition>) -> Self {
        Self {
            schema: trigger_schema(),
            triggers,
        }
    }
}

fn trigger_schema() -> SchemaRef {
    let columns = vec![
        Field::new("trigger_name", DataType::Utf8, false),
        Field::new("plugin_name", DataType::Utf8, false),
        Field::new("trigger_specification", DataType::Utf8, false),
        Field::new("disabled", DataType::Boolean, false),
    ];
    Schema::new(columns).into()
}

#[async_trait]
impl IoxSystemTable for ProcessingEngineTriggerTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    async fn scan(
        &self,
        _filters: Option<Vec<Expr>>,
        _limit: Option<usize>,
    ) -> Result<RecordBatch> {
        let trigger_column = self
            .triggers
            .iter()
            .map(|trigger| Some(trigger.trigger_name.clone()))
            .collect::<StringArray>();
        let plugin_column = self
            .triggers
            .iter()
            .map(|trigger| Some(trigger.plugin.plugin_name.clone()))
            .collect::<StringArray>();
        let specification_column = self
            .triggers
            .iter()
            .map(|trigger| serde_json::to_string(&trigger.trigger).ok())
            .collect::<StringArray>();
        let disabled = self
            .triggers
            .iter()
            .map(|trigger| Some(trigger.disabled))
            .collect::<BooleanArray>();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(trigger_column),
            Arc::new(plugin_column),
            Arc::new(specification_column),
            Arc::new(disabled),
        ];
        Ok(RecordBatch::try_new(Arc::clone(&self.schema), columns)?)
    }
}
