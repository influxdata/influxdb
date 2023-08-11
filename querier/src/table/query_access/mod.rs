use std::{any::Any, sync::Arc};

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::{
    datasource::{TableProvider, TableType},
    error::DataFusionError,
    execution::context::SessionState,
    logical_expr::TableProviderFilterPushDown,
    physical_plan::ExecutionPlan,
    prelude::Expr,
};
use iox_query::{
    exec::SessionContextIOxExt,
    provider::{ChunkPruner, Error as ProviderError, ProviderBuilder},
    pruning::{prune_chunks, retention_expr, NotPrunedReason, PruningObserver},
    QueryChunk,
};
use predicate::Predicate;
use schema::Schema;

use crate::{ingester::IngesterChunk, parquet::QuerierParquetChunk};

use self::metrics::PruneMetrics;

use super::QuerierTable;

pub mod metrics;

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
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        // build provider out of all chunks
        // TODO: push down some predicates to catalog

        let mut builder =
            ProviderBuilder::new(Arc::clone(self.table_name()), self.schema().clone());

        let filters = match self.namespace_retention_period {
            Some(d) => {
                let ts = self
                    .chunk_adapter
                    .catalog_cache()
                    .time_provider()
                    .now()
                    .timestamp_nanos()
                    - d.as_nanos() as i64;

                filters
                    .iter()
                    .cloned()
                    .chain(std::iter::once(retention_expr(ts)))
                    .collect::<Vec<_>>()
            }
            None => filters.to_vec(),
        };

        let chunks = self
            .chunks(&filters, ctx.child_span("QuerierTable chunks"), projection)
            .await?;

        for chunk in chunks {
            builder = builder.add_chunk(chunk);
        }

        let provider = match builder.build() {
            Ok(provider) => provider,
            Err(e) => panic!("unexpected error: {e:?}"),
        };

        provider.scan(ctx, projection, &filters, limit).await
    }

    fn supports_filter_pushdown(
        &self,
        _filter: &Expr,
    ) -> Result<TableProviderFilterPushDown, DataFusionError> {
        Ok(TableProviderFilterPushDown::Exact)
    }
}

#[derive(Debug)]
pub struct QuerierTableChunkPruner {
    metrics: Arc<PruneMetrics>,
}

impl QuerierTableChunkPruner {
    pub fn new(metrics: Arc<PruneMetrics>) -> Self {
        Self { metrics }
    }
}

impl ChunkPruner for QuerierTableChunkPruner {
    fn prune_chunks(
        &self,
        _table_name: &str,
        table_schema: &Schema,
        chunks: Vec<Arc<dyn QueryChunk>>,
        filters: &[Expr],
    ) -> Result<Vec<Arc<dyn QueryChunk>>, ProviderError> {
        let observer = &MetricPruningObserver::new(Arc::clone(&self.metrics));

        let predicate = Predicate::default().with_exprs(filters.iter().cloned());
        let chunks = match prune_chunks(table_schema, &chunks, &predicate) {
            Ok(keeps) => {
                assert_eq!(chunks.len(), keeps.len());
                chunks
                    .into_iter()
                    .zip(keeps.iter())
                    .filter_map(|(chunk, keep)| {
                        if *keep {
                            observer.was_not_pruned(chunk.as_ref());
                            Some(chunk)
                        } else {
                            observer.was_pruned(chunk.as_ref());
                            None
                        }
                    })
                    .collect()
            }
            Err(reason) => {
                for chunk in &chunks {
                    observer.could_not_prune(reason, chunk.as_ref())
                }
                chunks
            }
        };

        Ok(chunks)
    }
}

pub(crate) struct MetricPruningObserver {
    metrics: Arc<PruneMetrics>,
}

impl MetricPruningObserver {
    pub(crate) fn new(metrics: Arc<PruneMetrics>) -> Self {
        Self { metrics }
    }
}

impl PruningObserver for MetricPruningObserver {
    fn was_pruned(&self, chunk: &dyn QueryChunk) {
        self.metrics.pruned_late.inc(
            1,
            chunk_rows(chunk) as u64,
            chunk_estimate_size(chunk) as u64,
        );
    }

    fn was_not_pruned(&self, chunk: &dyn QueryChunk) {
        self.metrics.not_pruned.inc(
            1,
            chunk_rows(chunk) as u64,
            chunk_estimate_size(chunk) as u64,
        );
    }

    fn could_not_prune(&self, reason: NotPrunedReason, chunk: &dyn QueryChunk) {
        let group = match reason {
            NotPrunedReason::NoExpressionOnPredicate => &self.metrics.could_not_prune_no_expression,
            NotPrunedReason::CanNotCreatePruningPredicate => {
                &self.metrics.could_not_prune_cannot_create_predicate
            }
            NotPrunedReason::DataFusionPruningFailed => &self.metrics.could_not_prune_df,
        };

        group.inc(
            1,
            chunk_rows(chunk) as u64,
            chunk_estimate_size(chunk) as u64,
        );
    }
}

fn chunk_estimate_size(chunk: &dyn QueryChunk) -> usize {
    let chunk = chunk.as_any();

    if let Some(chunk) = chunk.downcast_ref::<IngesterChunk>() {
        chunk.estimate_size()
    } else if let Some(chunk) = chunk.downcast_ref::<QuerierParquetChunk>() {
        chunk.estimate_size()
    } else {
        panic!("Unknown chunk type")
    }
}

fn chunk_rows(chunk: &dyn QueryChunk) -> usize {
    let chunk = chunk.as_any();

    if let Some(chunk) = chunk.downcast_ref::<QuerierParquetChunk>() {
        chunk.rows()
    } else if let Some(chunk) = chunk.downcast_ref::<IngesterChunk>() {
        chunk.rows()
    } else {
        panic!("Unknown chunk type");
    }
}
