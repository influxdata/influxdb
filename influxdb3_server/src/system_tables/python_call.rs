use arrow_array::{ArrayRef, BooleanArray, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::common::Result;
use datafusion::logical_expr::Expr;
use influxdb3_catalog::log::CreateTriggerLog;
use influxdb3_py_api::logging::ProcessingEngineLog;
use influxdb3_sys_events::{SysEventStore, ToRecordBatch};
use iox_system_tables::IoxSystemTable;
use std::sync::Arc;

#[derive(Debug)]
pub(super) struct ProcessingEngineTriggerTable {
    schema: SchemaRef,
    triggers: Vec<CreateTriggerLog>,
}

impl ProcessingEngineTriggerTable {
    pub(super) fn new(triggers: Vec<CreateTriggerLog>) -> Self {
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
            .map(|trigger| Some(trigger.trigger_name.clone()))
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
