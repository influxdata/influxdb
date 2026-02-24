use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, Statistics,
};
use datafusion::prelude::Expr;
use futures::TryStreamExt;

/// The minimal thing that a system table needs to implement
#[async_trait]
pub trait IoxSystemTable: std::fmt::Debug + Send + Sync {
    /// Produce the schema from this system table
    fn schema(&self) -> SchemaRef;

    /// Get the contents of the system table
    async fn scan(
        &self,
        filters: Option<Vec<Expr>>,
        limit: Option<usize>,
    ) -> DataFusionResult<RecordBatch>;
}

/// Adapter that makes any `IoxSystemTable` a DataFusion `TableProvider`
#[derive(Debug)]
pub struct SystemTableProvider<T: IoxSystemTable> {
    table: Arc<T>,
}

impl<T: IoxSystemTable> SystemTableProvider<T> {
    /// Create a new [`SystemTableProvider`]
    pub fn new(table: Arc<T>) -> Self {
        Self { table }
    }
}

#[async_trait]
impl<T> TableProvider for SystemTableProvider<T>
where
    T: IoxSystemTable + 'static,
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.table.schema()
    }

    async fn scan(
        &self,
        _ctx: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let schema = self.table.schema();
        let projected_schema = match projection.as_ref() {
            Some(projection) => Arc::new(schema.project(projection)?),
            None => schema,
        };

        Ok(Arc::new(SystemTableExecutionPlan::new(
            Arc::clone(&self.table),
            projected_schema,
            projection.cloned(),
            filters,
            limit,
        )))
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        // provide `Inexact` so DataFusion will push down the predicates
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
}

/// Implementor of the [`ExecutionPlan`] trait for system tables
pub struct SystemTableExecutionPlan<T> {
    table: Arc<T>,
    projected_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    /// Cache holding plan properties like equivalences, output partitioning, output ordering etc.
    cache: PlanProperties,
    filters: Option<Vec<Expr>>,
    limit: Option<usize>,
}

impl<T> SystemTableExecutionPlan<T> {
    fn new(
        table: Arc<T>,
        projected_schema: SchemaRef,
        projection: Option<Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Self {
        let cache = Self::compute_properties(Arc::clone(&projected_schema));
        Self {
            table,
            projected_schema,
            projection,
            cache,
            filters: (!filters.is_empty()).then(|| filters.to_vec()),
            limit,
        }
    }

    /// This function creates the cache object that stores the plan properties such as equivalence properties, partitioning, ordering, etc.
    fn compute_properties(projected_schema: SchemaRef) -> PlanProperties {
        let eq_properties = EquivalenceProperties::new(projected_schema);

        let output_partitioning = Partitioning::UnknownPartitioning(1);

        PlanProperties::new(
            eq_properties,
            output_partitioning,
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
    }
}

impl<T> std::fmt::Debug for SystemTableExecutionPlan<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.fmt_as(DisplayFormatType::Default, f)
    }
}

impl<T: IoxSystemTable + 'static> ExecutionPlan for SystemTableExecutionPlan<T> {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.projected_schema)
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        assert!(children.is_empty());
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let batch_size = context.session_config().batch_size();
        let table = Arc::clone(&self.table);
        let projection = self.projection.clone();
        let filters = self.filters.clone();
        let limit = self.limit;
        let stream = futures::stream::once(async move {
            let batch = table.scan(filters, limit).await?;
            let batch = match projection {
                Some(projection) => batch.project(&projection)?,
                None => batch,
            };

            // https://stackoverflow.com/a/17974
            let n_slices = batch.num_rows().div_ceil(batch_size);

            Ok(futures::stream::iter((0..n_slices).map(move |n| {
                let offset = n * batch_size;
                let end = batch.num_rows().min(offset + batch_size);
                let length = end - offset;
                Ok(batch.slice(offset, length)) as DataFusionResult<_>
            }))) as DataFusionResult<_>
        })
        .try_flatten();

        let stream = RecordBatchStreamAdapter::new(Arc::clone(&self.projected_schema), stream);

        Ok(Box::pin(stream))
    }

    fn statistics(&self) -> Result<Statistics, DataFusionError> {
        Ok(Statistics::new_unknown(&self.schema()))
    }
}

impl<T> DisplayAs for SystemTableExecutionPlan<T> {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => f
                .debug_struct("SystemTableExecutionPlan")
                .field("projection", &self.projection)
                .field("filters", &self.filters)
                .finish(),
        }
    }
}
