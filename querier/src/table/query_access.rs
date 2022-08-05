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
    pruning::{prune_chunks, NotPrunedReason, PruningObserver},
    QueryChunk,
};
use metric::U64Counter;
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
    metrics: Arc<PruneMetrics>,
}

impl QuerierTableChunkPruner {
    pub fn new(max_bytes: usize, metrics: Arc<PruneMetrics>) -> Self {
        Self { max_bytes, metrics }
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
        let chunks = prune_chunks(
            &MetricPruningObserver {
                metrics: Arc::clone(&self.metrics),
            },
            table_schema,
            chunks,
            predicate,
        );

        let estimated_bytes = chunks
            .iter()
            .map(|chunk| chunk_estimate_size(chunk.as_ref()))
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

struct MetricPruningObserver {
    metrics: Arc<PruneMetrics>,
}

impl PruningObserver for MetricPruningObserver {
    fn was_pruned(&self, chunk: &dyn QueryChunk) {
        self.metrics.chunks_pruned.inc(1);
        self.metrics.rows_pruned.inc(chunk_rows(chunk) as u64);
        self.metrics
            .bytes_pruned
            .inc(chunk_estimate_size(chunk) as u64);
    }

    fn was_not_pruned(&self, chunk: &dyn QueryChunk) {
        self.metrics.chunks_not_pruned.inc(1);
        self.metrics.rows_not_pruned.inc(chunk_rows(chunk) as u64);
        self.metrics
            .bytes_not_pruned
            .inc(chunk_estimate_size(chunk) as u64);
    }

    fn could_not_prune(&self, reason: NotPrunedReason, chunk: &dyn QueryChunk) {
        let (chunks, rows, bytes) = match reason {
            NotPrunedReason::NoExpressionOnPredicate => (
                &self.metrics.chunks_could_not_prune_no_expression,
                &self.metrics.rows_could_not_prune_no_expression,
                &self.metrics.bytes_could_not_prune_no_expression,
            ),
            NotPrunedReason::CanNotCreatePruningPredicate => (
                &self.metrics.chunks_could_not_prune_cannot_create_predicate,
                &self.metrics.rows_could_not_prune_cannot_create_predicate,
                &self.metrics.bytes_could_not_prune_cannot_create_predicate,
            ),
            NotPrunedReason::DataFusionPruningFailed => (
                &self.metrics.chunks_could_not_prune_df,
                &self.metrics.rows_could_not_prune_df,
                &self.metrics.bytes_could_not_prune_df,
            ),
        };

        chunks.inc(1);
        rows.inc(chunk_rows(chunk) as u64);
        bytes.inc(chunk_estimate_size(chunk) as u64);
    }
}

#[derive(Debug)]
pub struct PruneMetrics {
    // number of chunks
    chunks_pruned: U64Counter,
    chunks_not_pruned: U64Counter,
    chunks_could_not_prune_no_expression: U64Counter,
    chunks_could_not_prune_cannot_create_predicate: U64Counter,
    chunks_could_not_prune_df: U64Counter,

    // number of rows
    rows_pruned: U64Counter,
    rows_not_pruned: U64Counter,
    rows_could_not_prune_no_expression: U64Counter,
    rows_could_not_prune_cannot_create_predicate: U64Counter,
    rows_could_not_prune_df: U64Counter,

    // size in bytes
    bytes_pruned: U64Counter,
    bytes_not_pruned: U64Counter,
    bytes_could_not_prune_no_expression: U64Counter,
    bytes_could_not_prune_cannot_create_predicate: U64Counter,
    bytes_could_not_prune_df: U64Counter,
}

impl PruneMetrics {
    pub fn new(metric_registry: &metric::Registry) -> Self {
        let chunks = metric_registry.register_metric::<U64Counter>(
            "query_pruner_chunks",
            "Number of chunks seen by the statistics-based chunk pruner",
        );
        let chunks_pruned = chunks.recorder(&[("result", "pruned")]);
        let chunks_not_pruned = chunks.recorder(&[("result", "not_pruned")]);
        let chunks_could_not_prune_no_expression = chunks.recorder(&[
            ("result", "could_not_prune"),
            ("reason", NotPrunedReason::NoExpressionOnPredicate.name()),
        ]);
        let chunks_could_not_prune_cannot_create_predicate = chunks.recorder(&[
            ("result", "could_not_prune"),
            (
                "reason",
                NotPrunedReason::CanNotCreatePruningPredicate.name(),
            ),
        ]);
        let chunks_could_not_prune_df = chunks.recorder(&[
            ("result", "could_not_prune"),
            ("reason", NotPrunedReason::DataFusionPruningFailed.name()),
        ]);

        let rows = metric_registry.register_metric::<U64Counter>(
            "query_pruner_rows",
            "Number of rows seen by the statistics-based chunk pruner",
        );
        let rows_pruned = rows.recorder(&[("result", "pruned")]);
        let rows_not_pruned = rows.recorder(&[("result", "not_pruned")]);
        let rows_could_not_prune_no_expression = rows.recorder(&[
            ("result", "could_not_prune"),
            ("reason", NotPrunedReason::NoExpressionOnPredicate.name()),
        ]);
        let rows_could_not_prune_cannot_create_predicate = rows.recorder(&[
            ("result", "could_not_prune"),
            (
                "reason",
                NotPrunedReason::CanNotCreatePruningPredicate.name(),
            ),
        ]);
        let rows_could_not_prune_df = rows.recorder(&[
            ("result", "could_not_prune"),
            ("reason", NotPrunedReason::DataFusionPruningFailed.name()),
        ]);

        let bytes = metric_registry.register_metric::<U64Counter>(
            "query_pruner_bytes",
            "Size (in bytes) of chunks seen by the statistics-based chunk pruner",
        );
        let bytes_pruned = bytes.recorder(&[("result", "pruned")]);
        let bytes_not_pruned = bytes.recorder(&[("result", "not_pruned")]);
        let bytes_could_not_prune_no_expression = bytes.recorder(&[
            ("result", "could_not_prune"),
            ("reason", NotPrunedReason::NoExpressionOnPredicate.name()),
        ]);
        let bytes_could_not_prune_cannot_create_predicate = bytes.recorder(&[
            ("result", "could_not_prune"),
            (
                "reason",
                NotPrunedReason::CanNotCreatePruningPredicate.name(),
            ),
        ]);
        let bytes_could_not_prune_df = bytes.recorder(&[
            ("result", "could_not_prune"),
            ("reason", NotPrunedReason::DataFusionPruningFailed.name()),
        ]);

        Self {
            chunks_pruned,
            chunks_not_pruned,
            chunks_could_not_prune_no_expression,
            chunks_could_not_prune_cannot_create_predicate,
            chunks_could_not_prune_df,
            rows_pruned,
            rows_not_pruned,
            rows_could_not_prune_no_expression,
            rows_could_not_prune_cannot_create_predicate,
            rows_could_not_prune_df,
            bytes_pruned,
            bytes_not_pruned,
            bytes_could_not_prune_no_expression,
            bytes_could_not_prune_cannot_create_predicate,
            bytes_could_not_prune_df,
        }
    }
}

fn chunk_estimate_size(chunk: &dyn QueryChunk) -> usize {
    let chunk = chunk.as_any();

    if let Some(chunk) = chunk.downcast_ref::<IngesterChunk>() {
        chunk.estimate_size()
    } else if let Some(chunk) = chunk.downcast_ref::<QuerierChunk>() {
        chunk.estimate_size()
    } else {
        panic!("Unknown chunk type")
    }
}

fn chunk_rows(chunk: &dyn QueryChunk) -> usize {
    let chunk = chunk.as_any();

    if let Some(chunk) = chunk.downcast_ref::<QuerierChunk>() {
        chunk.rows()
    } else if let Some(chunk) = chunk.downcast_ref::<IngesterChunk>() {
        chunk.rows()
    } else {
        panic!("Unknown chunk type");
    }
}
