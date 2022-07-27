use std::{any::Any, sync::Arc};

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::{
    datasource::{TableProvider, TableType},
    error::DataFusionError,
    execution::context::SessionState,
    logical_expr::TableProviderFilterPushDown,
    logical_plan::Expr,
    physical_plan::ExecutionPlan,
};
use iox_query::{
    exec::{ExecutorType, SessionContextIOxExt},
    provider::{ChunkPruner, Error as ProviderError, ProviderBuilder},
    pruning::{prune_chunks, PruningObserver},
    QueryChunk,
};
use predicate::Predicate;
use schema::Schema;

use crate::{chunk::QuerierChunk, ingester::IngesterChunk};

use super::QuerierTable;

#[async_trait]
impl TableProvider for QuerierTable {
    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }

    fn schema(&self) -> SchemaRef {
        self.schema().as_arrow()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        ctx: &SessionState,
        projection: &Option<Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        // build provider out of all chunks
        // TODO: push down some predicates to catalog
        let iox_ctx = self.exec.new_context_from_df(ExecutorType::Query, ctx);

        let mut builder =
            ProviderBuilder::new(self.table_name(), Arc::clone(self.schema()), iox_ctx);
        builder = builder.add_pruner(self.chunk_pruner());

        let predicate = filters
            .iter()
            .fold(Predicate::new(), |b, expr| b.with_expr(expr.clone()));

        let chunks = self
            .chunks(&predicate, ctx.child_span("querier table chunks"))
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        for chunk in chunks {
            builder = builder.add_chunk(chunk);
        }

        let provider = match builder.build() {
            Ok(provider) => provider,
            Err(e) => panic!("unexpected error: {:?}", e),
        };

        provider.scan(ctx, projection, filters, limit).await
    }

    fn supports_filter_pushdown(
        &self,
        _filter: &Expr,
    ) -> Result<TableProviderFilterPushDown, DataFusionError> {
        // we may apply filtering (via pruning) but can not guarantee
        // that the filter catches all row during scan
        Ok(TableProviderFilterPushDown::Inexact)
    }
}

#[derive(Debug)]
pub struct QuerierTableChunkPruner {
    max_bytes: usize,
}

impl QuerierTableChunkPruner {
    pub fn new(max_bytes: usize) -> Self {
        Self { max_bytes }
    }
}

impl ChunkPruner for QuerierTableChunkPruner {
    fn prune_chunks(
        &self,
        _table_name: &str,
        table_schema: Arc<Schema>,
        chunks: Vec<Arc<dyn QueryChunk>>,
        predicate: &Predicate,
    ) -> Result<Vec<Arc<dyn QueryChunk>>, ProviderError> {
        // TODO: bring back metrics (https://github.com/influxdata/influxdb_iox/issues/4087)
        let chunks = prune_chunks(&NoopPruningObserver {}, table_schema, chunks, predicate);

        let estimated_bytes = chunks
            .iter()
            .map(|chunk| {
                let chunk = chunk.as_any();
                if let Some(chunk) = chunk.downcast_ref::<IngesterChunk>() {
                    chunk.estimate_size()
                } else if let Some(chunk) = chunk.downcast_ref::<QuerierChunk>() {
                    chunk.estimate_size()
                } else {
                    panic!("Unknown chunk type")
                }
            })
            .sum::<usize>();
        if estimated_bytes > self.max_bytes {
            return Err(ProviderError::TooMuchData {
                actual_bytes: estimated_bytes,
                limit_bytes: self.max_bytes,
            });
        }

        Ok(chunks)
    }
}

struct NoopPruningObserver;

impl PruningObserver for NoopPruningObserver {}
