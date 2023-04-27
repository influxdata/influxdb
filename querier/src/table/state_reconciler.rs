//! Logic to reconcile the catalog and ingester state

mod interface;

use data_types::DeletePredicate;
use iox_query::QueryChunk;
use observability_deps::tracing::debug;
use snafu::Snafu;
use std::sync::Arc;
use trace::span::{Span, SpanRecorder};

use crate::{parquet::QuerierParquetChunk, IngesterPartition};

#[derive(Snafu, Debug)]
#[allow(missing_copy_implementations)]
pub enum ReconcileError {
    #[snafu(display("Compactor processed file that the querier would need to split apart which is not yet implemented"))]
    CompactorConflict,
}

/// Handles reconciling catalog and ingester state.
#[derive(Debug)]
pub struct Reconciler {
    table_name: Arc<str>,
    namespace_name: Arc<str>,
}

impl Reconciler {
    pub(crate) fn new(table_name: Arc<str>, namespace_name: Arc<str>) -> Self {
        Self {
            table_name,
            namespace_name,
        }
    }

    /// Reconciles ingester state (ingester_partitions) and catalog state (parquet_files),
    /// producing a list of chunks to query
    pub(crate) async fn reconcile(
        &self,
        ingester_partitions: Vec<IngesterPartition>,
        retention_delete_pred: Option<Arc<DeletePredicate>>,
        parquet_files: Vec<QuerierParquetChunk>,
        span: Option<Span>,
    ) -> Result<Vec<Arc<dyn QueryChunk>>, ReconcileError> {
        let span_recorder = SpanRecorder::new(span);
        let mut chunks = self
            .build_chunks_from_parquet(
                &ingester_partitions,
                retention_delete_pred.clone(),
                parquet_files,
                span_recorder.child_span("build_chunks_from_parquet"),
            )
            .await?;
        chunks.extend(self.build_ingester_chunks(ingester_partitions, retention_delete_pred));
        debug!(num_chunks=%chunks.len(), "Final chunk count after reconcilation");

        Ok(chunks)
    }

    async fn build_chunks_from_parquet(
        &self,
        ingester_partitions: &[IngesterPartition],
        retention_delete_pred: Option<Arc<DeletePredicate>>,
        parquet_files: Vec<QuerierParquetChunk>,
        _span: Option<Span>,
    ) -> Result<Vec<Arc<dyn QueryChunk>>, ReconcileError> {
        debug!(
            namespace=%self.namespace_name(),
            table_name=%self.table_name(),
            num_parquet_files=parquet_files.len(),
            "Reconciling "
        );

        debug!(num_chunks=%parquet_files.len(), "Created chunks from parquet files");

        let mut chunks: Vec<Arc<dyn QueryChunk>> =
            Vec::with_capacity(parquet_files.len() + ingester_partitions.len());

        let retention_expr_len = usize::from(retention_delete_pred.is_some());
        for chunk in parquet_files.into_iter() {
            let mut delete_predicates = Vec::with_capacity(retention_expr_len);

            if let Some(retention_delete_pred) = retention_delete_pred.clone() {
                delete_predicates.push(retention_delete_pred);
            }

            let chunk = chunk.with_delete_predicates(delete_predicates);

            chunks.push(Arc::new(chunk));
        }

        Ok(chunks)
    }

    fn build_ingester_chunks(
        &self,
        ingester_partitions: Vec<IngesterPartition>,
        retention_delete_pred: Option<Arc<DeletePredicate>>,
    ) -> impl Iterator<Item = Arc<dyn QueryChunk>> {
        // Add ingester chunks to the overall chunk list.
        // - filter out chunks that don't have any record batches
        ingester_partitions
            .into_iter()
            .flat_map(move |c| {
                let c = match &retention_delete_pred {
                    Some(pred) => c.with_delete_predicates(vec![Arc::clone(pred)]),
                    None => c,
                };
                c.into_chunks().into_iter()
            })
            .map(|c| Arc::new(c) as Arc<dyn QueryChunk>)
    }

    #[must_use]
    pub fn table_name(&self) -> &str {
        self.table_name.as_ref()
    }

    #[must_use]
    pub fn namespace_name(&self) -> &str {
        self.namespace_name.as_ref()
    }
}
