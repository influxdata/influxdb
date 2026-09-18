//! `system.processing_engine_logs` system table implementation [`ProcessingEngineLogsTable`]
use std::{any::Any, fmt::Debug, sync::Arc};

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::{
    catalog::Session,
    common::Column,
    datasource::{TableProvider, TableType, ViewTable, provider_as_source},
    error::DataFusionError,
    logical_expr::{
        BinaryExpr, Expr, LogicalPlanBuilder, Operator, TableProviderFilterPushDown, col,
    },
    physical_plan::{ExecutionPlan, empty::EmptyExec},
    scalar::ScalarValue,
};
use influxdb3_catalog::catalog::{
    DatabaseSchema, INTERNAL_DB_NAME, TIME_COLUMN_NAME, TableDefinition,
};
use iox_query::{QueryChunk, provider::ProviderBuilder};

/// Name of the table in the `_internal` database.
const PROCESSING_ENGINE_LOGS_TABLE_NAME: &str = "processing_engine_logs";

/// API needed by [`ProcessingEngineLogsTable`] to fill `processing_engine_logs` table.
///
/// `ent` and `oss` have different WriteBuffer implementations; This trait
/// abstracts the differences.
pub trait ProcessingEngineLogsSource: Debug + Send + Sync {
    /// Catalog used to look up the storage-backed `processing_engine_logs` table, if any.
    fn catalog(&self) -> Arc<influxdb3_catalog::catalog::Catalog>;

    /// Chunks backing the storage-based `processing_engine_logs` table for the given scan.
    fn chunks(
        &self,
        internal_db_schema: Arc<DatabaseSchema>,
        table_def: Arc<TableDefinition>,
        ctx: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
    ) -> Result<Vec<Arc<dyn QueryChunk>>, DataFusionError>;
}

/// Name of the virtual column exposed by the `processing_engine_logs` view
/// that mirrors the `time` column, for compatibility with clients that
/// queried `event_time` before the column was renamed.
const EVENT_TIME_COLUMN_NAME: &str = "event_time";

/// Wrap the `processing_engine_logs` table in a view equivalent to
/// `SELECT *, time AS event_time FROM processing_engine_logs`, so DataFusion's
/// optimizer rewrites projections and filters on `event_time` to the
/// underlying `time` column.
pub fn processing_engine_logs_view(
    table: Arc<dyn TableProvider>,
) -> Result<Arc<dyn TableProvider>, DataFusionError> {
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

/// `system.processing_engine_logs` system table implementation
#[derive(Debug)]
pub struct ProcessingEngineLogsTable {
    db_schema: Arc<DatabaseSchema>,
    source: Arc<dyn ProcessingEngineLogsSource>,
    /// Schema served when the storage-backed table does not exist yet (e.g. a fresh catalog).
    empty_schema: SchemaRef,
}

impl ProcessingEngineLogsTable {
    pub fn new(
        db_schema: Arc<DatabaseSchema>,
        source: Arc<dyn ProcessingEngineLogsSource>,
        empty_schema: SchemaRef,
    ) -> Self {
        Self {
            db_schema,
            source,
            empty_schema,
        }
    }

    fn storage_table(&self) -> Option<(Arc<DatabaseSchema>, Arc<TableDefinition>)> {
        let internal_db_schema = self.source.catalog().db_schema(INTERNAL_DB_NAME)?;
        let table_def = internal_db_schema.table_definition(PROCESSING_ENGINE_LOGS_TABLE_NAME)?;
        Some((internal_db_schema, table_def))
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
}

#[async_trait]
impl TableProvider for ProcessingEngineLogsTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.storage_table()
            .map(|(_, table_def)| table_def.schema.as_arrow())
            .unwrap_or_else(|| Arc::clone(&self.empty_schema))
    }

    async fn scan(
        &self,
        ctx: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
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
        for chunk in self.source.chunks(
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
    ) -> Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
}

#[cfg(test)]
mod tests;
