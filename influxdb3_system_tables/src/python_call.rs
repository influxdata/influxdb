use std::{any::Any, sync::Arc};

use arrow_array::{ArrayRef, BooleanArray, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::{
    catalog::Session,
    common::{Column, Result},
    datasource::{TableProvider, TableType, ViewTable, provider_as_source},
    error::DataFusionError,
    logical_expr::{
        BinaryExpr, Expr, LogicalPlanBuilder, Operator, TableProviderFilterPushDown, col,
    },
    physical_plan::{ExecutionPlan, empty::EmptyExec},
    scalar::ScalarValue,
};
use influxdb3_catalog::catalog::{
    DatabaseSchema, INTERNAL_DB_NAME, TIME_COLUMN_NAME, TableDefinition, TriggerDefinition,
};
use influxdb3_py_api::logging::{PROCESSING_ENGINE_LOGS_TABLE_NAME, processing_engine_logs_schema};
use influxdb3_write::{ChunkFilter, WriteBuffer};
use iox_query::provider::ProviderBuilder;
use iox_system_tables::IoxSystemTable;

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

/// Name of the virtual column exposed by the `processing_engine_logs` view
/// that mirrors the `time` column, for compatibility with clients that
/// queried `event_time` before the column was renamed.
const EVENT_TIME_COLUMN_NAME: &str = "event_time";

/// Wrap the `processing_engine_logs` table in a view equivalent to
/// `SELECT *, time AS event_time FROM processing_engine_logs`, so DataFusion's
/// optimizer rewrites projections and filters on `event_time` to the
/// underlying `time` column.
pub(super) fn processing_engine_logs_view(
    table: Arc<dyn TableProvider>,
) -> Result<Arc<dyn TableProvider>> {
    let schema = table.schema();
    // A schema that already has a physical event_time column serves it
    // directly; aliasing time on top would create a duplicate field name.
    if schema.fields().find(EVENT_TIME_COLUMN_NAME).is_some() {
        return Ok(table);
    }
    let mut exprs = schema
        .fields()
        .iter()
        .map(|field| Expr::Column(Column::new_unqualified(field.name())))
        .collect::<Vec<_>>();
    exprs.push(
        Expr::Column(Column::new_unqualified(TIME_COLUMN_NAME)).alias(EVENT_TIME_COLUMN_NAME),
    );
    let plan = LogicalPlanBuilder::scan(
        PROCESSING_ENGINE_LOGS_TABLE_NAME,
        provider_as_source(table),
        None,
    )?
    .project(exprs)?
    .build()?;
    Ok(Arc::new(ViewTable::new(plan, None)))
}

#[derive(Debug)]
pub(super) struct ProcessingEngineLogsTable {
    db_schema: Arc<DatabaseSchema>,
    buffer: Arc<dyn WriteBuffer>,
}

impl ProcessingEngineLogsTable {
    pub(super) fn new(db_schema: Arc<DatabaseSchema>, buffer: Arc<dyn WriteBuffer>) -> Self {
        Self { db_schema, buffer }
    }

    fn storage_table(&self) -> Option<(Arc<DatabaseSchema>, Arc<TableDefinition>)> {
        let internal_db_schema = self.buffer.catalog().db_schema(INTERNAL_DB_NAME)?;
        let table_def = internal_db_schema.table_definition(PROCESSING_ENGINE_LOGS_TABLE_NAME)?;
        Some((internal_db_schema, table_def))
    }

    fn empty_schema() -> SchemaRef {
        Arc::new(processing_engine_logs_schema())
    }

    fn scoped_filters(&self, filters: &[Expr]) -> Vec<Expr> {
        let mut filters = filters.to_vec();
        if self.db_schema.name.as_ref() != INTERNAL_DB_NAME {
            filters.push(Expr::BinaryExpr(BinaryExpr {
                left: Box::new(col("database_name")),
                op: Operator::Eq,
                right: Box::new(Expr::Literal(
                    ScalarValue::Utf8(Some(self.db_schema.name.as_ref().to_owned())),
                    None,
                )),
            }));
        }
        filters
    }

    fn chunks(
        &self,
        internal_db_schema: Arc<DatabaseSchema>,
        table_def: Arc<TableDefinition>,
        ctx: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
    ) -> Result<Vec<Arc<dyn iox_query::QueryChunk>>> {
        let mut filter = ChunkFilter::new(&table_def, filters)
            .map_err(|error| DataFusionError::External(Box::new(error)))?;

        let catalog = self.buffer.catalog();
        if let Some(retention_cutoff) = internal_db_schema.get_retention_period_cutoff_ts_nanos(
            catalog.time_provider().now(),
            &table_def.table_id,
        ) {
            filter.time_lower_bound_ns = filter
                .time_lower_bound_ns
                .map(|lb| lb.max(retention_cutoff.timestamp_nanos()))
                .or(Some(retention_cutoff.timestamp_nanos()));
        }

        self.buffer
            .get_table_chunks(internal_db_schema, table_def, &filter, projection, ctx)
    }
}

#[async_trait]
impl TableProvider for ProcessingEngineLogsTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.storage_table()
            .map(|(_, table_def)| table_def.schema.as_arrow())
            .unwrap_or_else(Self::empty_schema)
    }

    async fn scan(
        &self,
        ctx: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let Some((internal_db_schema, table_def)) = self.storage_table() else {
            let schema = self.schema();
            let schema = match projection {
                Some(projection) => Arc::new(schema.project(projection)?),
                None => schema,
            };
            return Ok(Arc::new(EmptyExec::new(schema)));
        };

        let filters = self.scoped_filters(filters);
        let mut builder =
            ProviderBuilder::new(Arc::clone(&table_def.table_name), table_def.schema.clone());
        for chunk in self.chunks(
            Arc::clone(&internal_db_schema),
            Arc::clone(&table_def),
            ctx,
            projection,
            &filters,
        )? {
            builder = builder.add_chunk(chunk);
        }
        let provider = builder
            .build()
            .map_err(|e| DataFusionError::Internal(format!("unexpected error: {e:?}")))?;

        provider.scan(ctx, projection, &filters, limit).await
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
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
mod tests;
