//! `system.processing_engine_trigger_arguments` system table implementation [`ProcessingEngineTriggerArgumentsTable`]
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::{error::DataFusionError, logical_expr::Expr};
use influxdb3_catalog::catalog::TriggerDefinition;
use iox_system_tables::IoxSystemTable;

/// `system.processing_engine_trigger_arguments` system table implementation
#[derive(Debug)]
pub struct ProcessingEngineTriggerArgumentsTable {
    schema: SchemaRef,
    triggers: Vec<Arc<TriggerDefinition>>,
}

impl ProcessingEngineTriggerArgumentsTable {
    pub fn new(triggers: Vec<Arc<TriggerDefinition>>) -> Self {
        Self {
            schema: trigger_arguments_schema(),
            triggers,
        }
    }
}

fn trigger_arguments_schema() -> SchemaRef {
    let columns = vec![
        Field::new("trigger_name", DataType::Utf8, false),
        Field::new("argument_key", DataType::Utf8, false),
        Field::new("argument_value", DataType::Utf8, false),
    ];
    Schema::new(columns).into()
}

#[async_trait]
impl IoxSystemTable for ProcessingEngineTriggerArgumentsTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    async fn scan(
        &self,
        _filters: Option<Vec<Expr>>,
        _limit: Option<usize>,
    ) -> Result<RecordBatch, DataFusionError> {
        let mut trigger_names = Vec::new();
        let mut argument_keys = Vec::new();
        let mut argument_values = Vec::new();

        for trigger in &self.triggers {
            if let Some(ref arguments) = trigger.trigger_arguments {
                for (key, value) in arguments {
                    trigger_names.push(Some(trigger.trigger_name.as_ref()));
                    argument_keys.push(Some(key.as_str()));
                    argument_values.push(Some(value.as_str()));
                }
            }
        }

        let trigger_column = StringArray::from(trigger_names);
        let key_column = StringArray::from(argument_keys);
        let value_column = StringArray::from(argument_values);

        let columns: Vec<ArrayRef> = vec![
            Arc::new(trigger_column),
            Arc::new(key_column),
            Arc::new(value_column),
        ];
        Ok(RecordBatch::try_new(Arc::clone(&self.schema), columns)?)
    }
}

#[cfg(test)]
mod tests;
