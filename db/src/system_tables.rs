//! Contains implementation of IOx system tables:
//!
//! system.chunks
//! system.columns
//! system.chunk_columns
//! system.operations
//!
//! For example `SELECT * FROM system.chunks`

use super::{catalog::Catalog, query_log::QueryLog};
use arrow::{datatypes::SchemaRef, error::Result, record_batch::RecordBatch};
use async_trait::async_trait;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::expressions::PhysicalSortExpr;
use datafusion::physical_plan::{
    Partitioning, RecordBatchStream, SendableRecordBatchStream, Statistics,
};
use datafusion::{
    catalog::schema::SchemaProvider, datasource::TableProvider, error::Result as DataFusionResult,
    physical_plan::ExecutionPlan,
};
use job_registry::JobRegistry;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::{any::Any, sync::Arc};

mod chunks;
mod columns;
mod operations;
mod persistence;
mod queries;

// The IOx system schema
pub const SYSTEM_SCHEMA: &str = "system";

const CHUNKS: &str = "chunks";
const COLUMNS: &str = "columns";
const CHUNK_COLUMNS: &str = "chunk_columns";
const OPERATIONS: &str = "operations";
const PERSISTENCE_WINDOWS: &str = "persistence_windows";
const QUERIES: &str = "queries";

pub struct SystemSchemaProvider {
    chunks: Arc<dyn TableProvider>,
    columns: Arc<dyn TableProvider>,
    chunk_columns: Arc<dyn TableProvider>,
    operations: Arc<dyn TableProvider>,
    persistence_windows: Arc<dyn TableProvider>,
    queries: Arc<dyn TableProvider>,
}

impl std::fmt::Debug for SystemSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SystemSchemaProvider")
            .field("fields", &"...")
            .finish()
    }
}

impl SystemSchemaProvider {
    pub fn new(
        db_name: impl Into<String>,
        catalog: Arc<Catalog>,
        jobs: Arc<JobRegistry>,
        query_log: Arc<QueryLog>,
    ) -> Self {
        let db_name = db_name.into();
        let chunks = Arc::new(SystemTableProvider {
            table: Arc::new(chunks::ChunksTable::new(Arc::clone(&catalog))),
        });
        let columns = Arc::new(SystemTableProvider {
            table: Arc::new(columns::ColumnsTable::new(Arc::clone(&catalog))),
        });
        let chunk_columns = Arc::new(SystemTableProvider {
            table: Arc::new(columns::ChunkColumnsTable::new(Arc::clone(&catalog))),
        });
        let operations = Arc::new(SystemTableProvider {
            table: Arc::new(operations::OperationsTable::new(db_name, jobs)),
        });
        let persistence_windows = Arc::new(SystemTableProvider {
            table: Arc::new(persistence::PersistenceWindowsTable::new(catalog)),
        });
        let queries = Arc::new(SystemTableProvider {
            table: Arc::new(queries::QueriesTable::new(query_log)),
        });
        Self {
            chunks,
            columns,
            chunk_columns,
            operations,
            persistence_windows,
            queries,
        }
    }
}

const ALL_SYSTEM_TABLES: [&str; 6] = [
    CHUNKS,
    COLUMNS,
    CHUNK_COLUMNS,
    OPERATIONS,
    PERSISTENCE_WINDOWS,
    QUERIES,
];

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
            CHUNKS => Some(Arc::clone(&self.chunks)),
            COLUMNS => Some(Arc::clone(&self.columns)),
            CHUNK_COLUMNS => Some(Arc::clone(&self.chunk_columns)),
            OPERATIONS => Some(Arc::clone(&self.operations)),
            PERSISTENCE_WINDOWS => Some(Arc::clone(&self.persistence_windows)),
            QUERIES => Some(Arc::clone(&self.queries)),
            _ => None,
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        ALL_SYSTEM_TABLES
            .iter()
            .any(|&system_table| system_table == name)
    }
}

type BatchIterator = Box<dyn Iterator<Item = Result<RecordBatch>> + Send + Sync>;

/// The minimal thing that a system table needs to implement
trait IoxSystemTable: Send + Sync {
    /// Produce the schema from this system table
    fn schema(&self) -> SchemaRef;

    /// Get the contents of the system table
    fn scan(&self, batch_size: usize) -> Result<BatchIterator>;
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
        projection: &Option<Vec<usize>>,
        // It would be cool to push projection and limit down
        _filters: &[datafusion::logical_plan::Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let schema = self.table.schema();
        let projected_schema = match projection.as_ref() {
            Some(projection) => Arc::new(schema.project(projection)?),
            None => schema,
        };

        Ok(Arc::new(SystemTableExecutionPlan {
            table: Arc::clone(&self.table),
            projection: projection.clone(),
            projected_schema,
        }))
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
        let session_config = context.session_config();
        let batch_size = session_config.batch_size;

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
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.batches.next().map(|maybe_batch| {
            maybe_batch.and_then(|batch| match &self.projection {
                Some(projection) => batch.project(projection),
                None => Ok(batch),
            })
        }))
    }
}
