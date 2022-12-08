use crate::query_log::QueryLog;
use arrow::{datatypes::SchemaRef, error::Result as ArrowResult, record_batch::RecordBatch};
use async_trait::async_trait;
use data_types::NamespaceId;
use datafusion::{
    catalog::schema::SchemaProvider,
    datasource::TableProvider,
    error::Result as DataFusionResult,
    execution::context::{SessionState, TaskContext},
    logical_expr::TableType,
    physical_plan::{
        expressions::PhysicalSortExpr, ExecutionPlan, Partitioning, RecordBatchStream,
        SendableRecordBatchStream, Statistics,
    },
    prelude::Expr,
};
use std::{
    any::Any,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

mod queries;

pub const SYSTEM_SCHEMA: &str = "system";

const QUERIES_TABLE: &str = "queries";

const ALL_SYSTEM_TABLES: &[&str] = &[QUERIES_TABLE];

pub struct SystemSchemaProvider {
    queries: Arc<dyn TableProvider>,
}

impl SystemSchemaProvider {
    pub fn new(query_log: Arc<QueryLog>, namespace_id: NamespaceId) -> Self {
        let queries = Arc::new(SystemTableProvider {
            table: Arc::new(queries::QueriesTable::new(query_log, Some(namespace_id))),
        });

        Self { queries }
    }
}

impl SchemaProvider for SystemSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }

    fn table_names(&self) -> Vec<String> {
        ALL_SYSTEM_TABLES
            .iter()
            .map(|name| name.to_string())
            .collect()
    }

    fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        match name {
            QUERIES_TABLE => Some(Arc::clone(&self.queries)),
            _ => None,
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        ALL_SYSTEM_TABLES
            .iter()
            .any(|&system_table| system_table == name)
    }
}

type BatchIterator = Box<dyn Iterator<Item = ArrowResult<RecordBatch>> + Send + Sync>;

/// The minimal thing that a system table needs to implement
trait IoxSystemTable: Send + Sync {
    /// Produce the schema from this system table
    fn schema(&self) -> SchemaRef;

    /// Get the contents of the system table
    fn scan(&self, batch_size: usize) -> ArrowResult<BatchIterator>;
}

/// Adapter that makes any `IoxSystemTable` a DataFusion `TableProvider`
struct SystemTableProvider<T: IoxSystemTable> {
    table: Arc<T>,
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
        _ctx: &SessionState,
        projection: Option<&Vec<usize>>,
        // It would be cool to push projection and limit down
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let schema = self.table.schema();
        let projected_schema = match projection.as_ref() {
            Some(projection) => Arc::new(schema.project(projection)?),
            None => schema,
        };

        Ok(Arc::new(SystemTableExecutionPlan {
            table: Arc::clone(&self.table),
            projection: projection.cloned(),
            projected_schema,
        }))
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }
}

struct SystemTableExecutionPlan<T> {
    table: Arc<T>,
    projected_schema: SchemaRef,
    projection: Option<Vec<usize>>,
}

impl<T> std::fmt::Debug for SystemTableExecutionPlan<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SystemTableExecutionPlan")
            .field("projection", &self.projection)
            .finish()
    }
}

impl<T: IoxSystemTable + 'static> ExecutionPlan for SystemTableExecutionPlan<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.projected_schema)
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }
    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        unimplemented!()
    }

    fn execute(
        &self,
        _partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let batch_size = context.session_config().batch_size();

        Ok(Box::pin(SystemTableStream {
            projected_schema: Arc::clone(&self.projected_schema),
            batches: self.table.scan(batch_size)?,
            projection: self.projection.clone(),
        }))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

struct SystemTableStream {
    projected_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    batches: BatchIterator,
}

impl RecordBatchStream for SystemTableStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.projected_schema)
    }
}

impl futures::Stream for SystemTableStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.batches.next().map(|maybe_batch| {
            maybe_batch.and_then(|batch| match &self.projection {
                Some(projection) => batch.project(projection),
                None => Ok(batch),
            })
        }))
    }
}
