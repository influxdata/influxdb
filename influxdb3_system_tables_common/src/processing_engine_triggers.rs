//! `system.processing_engine_triggers` system table implementation [`ProcessingEngineTriggerTable`]
use std::sync::Arc;

use arrow_array::{ArrayRef, BooleanArray, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::{error::DataFusionError, logical_expr::Expr};
use influxdb3_catalog::catalog::TriggerDefinition;
use iox_system_tables::IoxSystemTable;

/// `system.processing_engine_triggers` system table implementation
#[derive(Debug)]
pub struct ProcessingEngineTriggerTable {
    schema: SchemaRef,
    triggers: Vec<Arc<TriggerDefinition>>,
}

impl ProcessingEngineTriggerTable {
    pub fn new(triggers: Vec<Arc<TriggerDefinition>>) -> Self {
        Self {
            schema: trigger_schema(),
            triggers,
        }
    }
}

fn trigger_schema() -> SchemaRef {
    let columns = vec![
        Field::new("trigger_name", DataType::Utf8, false),
        Field::new("plugin_filename", DataType::Utf8, false),
        Field::new("trigger_specification", DataType::Utf8, false),
        Field::new("disabled", DataType::Boolean, false),
        Field::new("error_behavior", DataType::Utf8, false),
    ];
    Schema::new(columns).into()
}

fn error_behavior_name(error_behavior: influxdb3_catalog::catalog::ErrorBehavior) -> &'static str {
    match error_behavior {
        influxdb3_catalog::catalog::ErrorBehavior::Log => "log",
        influxdb3_catalog::catalog::ErrorBehavior::Retry => "retry",
        influxdb3_catalog::catalog::ErrorBehavior::Disable => "disable",
    }
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
    ) -> Result<RecordBatch, DataFusionError> {
        let trigger_column = self
            .triggers
            .iter()
            .map(|trigger| Some(trigger.trigger_name.as_ref()))
            .collect::<StringArray>();
        let plugin_column = self
            .triggers
            .iter()
            .map(|trigger| Some(trigger.plugin_filename.clone()))
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
        let error_behavior_column = self
            .triggers
            .iter()
            .map(|trigger| Some(error_behavior_name(trigger.trigger_settings.error_behavior)))
            .collect::<StringArray>();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(trigger_column),
            Arc::new(plugin_column),
            Arc::new(specification_column),
            Arc::new(disabled),
            Arc::new(error_behavior_column),
        ];
        Ok(RecordBatch::try_new(Arc::clone(&self.schema), columns)?)
    }
}

#[cfg(test)]
mod tests;
