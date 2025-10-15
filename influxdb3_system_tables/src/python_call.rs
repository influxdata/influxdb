use arrow_array::{ArrayRef, BooleanArray, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::common::Result;
use datafusion::logical_expr::Expr;
use influxdb3_catalog::log::TriggerDefinition;
use influxdb3_py_api::logging::ProcessingEngineLog;
use influxdb3_sys_events::{SysEventStore, ToRecordBatch};
use iox_system_tables::IoxSystemTable;
use std::sync::Arc;

#[derive(Debug)]
pub(super) struct ProcessingEngineTriggerTable {
    schema: SchemaRef,
    triggers: Vec<Arc<TriggerDefinition>>,
}

impl ProcessingEngineTriggerTable {
    pub(super) fn new(triggers: Vec<Arc<TriggerDefinition>>) -> Self {
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
        let columns: Vec<ArrayRef> = vec![
            Arc::new(trigger_column),
            Arc::new(plugin_column),
            Arc::new(specification_column),
            Arc::new(disabled),
        ];
        Ok(RecordBatch::try_new(Arc::clone(&self.schema), columns)?)
    }
}

#[derive(Debug)]
pub(super) struct ProcessingEngineLogsTable {
    sys_event_store: Arc<SysEventStore>,
}

impl ProcessingEngineLogsTable {
    pub(super) fn new(sys_event_store: Arc<SysEventStore>) -> Self {
        Self { sys_event_store }
    }
}

#[async_trait]
impl IoxSystemTable for ProcessingEngineLogsTable {
    fn schema(&self) -> SchemaRef {
        Arc::new(ProcessingEngineLog::schema())
    }

    async fn scan(
        &self,
        _filters: Option<Vec<Expr>>,
        _limit: Option<usize>,
    ) -> Result<RecordBatch> {
        let Some(result) = self
            .sys_event_store
            .as_record_batch::<ProcessingEngineLog>()
        else {
            return Ok(RecordBatch::new_empty(Arc::new(
                ProcessingEngineLog::schema(),
            )));
        };
        Ok(result?)
    }
}

#[derive(Debug)]
pub(super) struct ProcessingEngineTriggerArgumentsTable {
    schema: SchemaRef,
    triggers: Vec<Arc<TriggerDefinition>>,
}

impl ProcessingEngineTriggerArgumentsTable {
    pub(super) fn new(triggers: Vec<Arc<TriggerDefinition>>) -> Self {
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
    ) -> Result<RecordBatch> {
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
mod tests {
    use super::*;
    use datafusion::assert_batches_sorted_eq;
    use influxdb3_catalog::log::{
        TriggerDefinition, TriggerSettings, TriggerSpecificationDefinition,
    };
    use influxdb3_id::TriggerId;

    #[tokio::test]
    async fn test_trigger_arguments_table_empty() {
        let table = ProcessingEngineTriggerArgumentsTable::new(vec![]);
        let batch = table.scan(None, None).await.unwrap();
        assert_eq!(batch.num_rows(), 0);
    }

    #[tokio::test]
    async fn test_trigger_arguments_table_with_triggers_no_args() {
        let trigger = Arc::new(TriggerDefinition {
            trigger_id: TriggerId::new(1),
            trigger_name: "test_trigger".into(),
            plugin_filename: "test_plugin.py".to_string(),
            database_name: "mydb".into(),
            node_id: Default::default(),
            trigger: TriggerSpecificationDefinition::AllTablesWalWrite,
            trigger_settings: TriggerSettings::default(),
            trigger_arguments: None,
            disabled: false,
        });

        let table = ProcessingEngineTriggerArgumentsTable::new(vec![trigger]);
        let batch = table.scan(None, None).await.unwrap();
        assert_eq!(batch.num_rows(), 0);
    }

    #[tokio::test]
    async fn test_trigger_arguments_table_with_arguments() {
        let mut args = hashbrown::HashMap::new();
        args.insert("key1".to_string(), "value1".to_string());
        args.insert("key2".to_string(), "value2".to_string());

        let trigger = Arc::new(TriggerDefinition {
            trigger_id: TriggerId::new(1),
            trigger_name: "test_trigger".into(),
            plugin_filename: "test_plugin.py".to_string(),
            database_name: "mydb".into(),
            node_id: Default::default(),
            trigger: TriggerSpecificationDefinition::AllTablesWalWrite,
            trigger_settings: TriggerSettings::default(),
            trigger_arguments: Some(args),
            disabled: false,
        });

        let table = ProcessingEngineTriggerArgumentsTable::new(vec![trigger]);
        let batch = table.scan(None, None).await.unwrap();

        // Use assert_batches_sorted_eq to check the output
        assert_batches_sorted_eq!(
            [
                "+--------------+--------------+----------------+",
                "| trigger_name | argument_key | argument_value |",
                "+--------------+--------------+----------------+",
                "| test_trigger | key1         | value1         |",
                "| test_trigger | key2         | value2         |",
                "+--------------+--------------+----------------+",
            ],
            &[batch]
        );
    }

    #[tokio::test]
    async fn test_trigger_arguments_table_multiple_triggers() {
        let mut args1 = hashbrown::HashMap::new();
        args1.insert("arg1".to_string(), "val1".to_string());

        let mut args2 = hashbrown::HashMap::new();
        args2.insert("arg2".to_string(), "val2".to_string());
        args2.insert("arg3".to_string(), "val3".to_string());

        let trigger1 = Arc::new(TriggerDefinition {
            trigger_id: TriggerId::new(1),
            trigger_name: "trigger1".into(),
            plugin_filename: "plugin1.py".to_string(),
            database_name: "db1".into(),
            node_id: Default::default(),
            trigger: TriggerSpecificationDefinition::AllTablesWalWrite,
            trigger_settings: TriggerSettings::default(),
            trigger_arguments: Some(args1),
            disabled: false,
        });

        let trigger2 = Arc::new(TriggerDefinition {
            trigger_id: TriggerId::new(2),
            trigger_name: "trigger2".into(),
            plugin_filename: "plugin2.py".to_string(),
            database_name: "db2".into(),
            node_id: Default::default(),
            trigger: TriggerSpecificationDefinition::AllTablesWalWrite,
            trigger_settings: TriggerSettings::default(),
            trigger_arguments: Some(args2),
            disabled: false,
        });

        let table = ProcessingEngineTriggerArgumentsTable::new(vec![trigger1, trigger2]);
        let batch = table.scan(None, None).await.unwrap();

        // Use assert_batches_sorted_eq to check the output
        assert_batches_sorted_eq!(
            [
                "+--------------+--------------+----------------+",
                "| trigger_name | argument_key | argument_value |",
                "+--------------+--------------+----------------+",
                "| trigger1     | arg1         | val1           |",
                "| trigger2     | arg2         | val2           |",
                "| trigger2     | arg3         | val3           |",
                "+--------------+--------------+----------------+",
            ],
            &[batch]
        );
    }
}
