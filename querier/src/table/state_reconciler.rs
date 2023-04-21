//! Logic to reconcile the catalog and ingester state

mod interface;

use data_types::{DeletePredicate, PartitionId};
use iox_query::QueryChunk;
use observability_deps::tracing::debug;
use schema::sort::SortKey;
use snafu::Snafu;
use std::{
    collections::{hash_map::Entry, HashMap},
    sync::Arc,
};
use trace::span::{Span, SpanRecorder};

use crate::{ingester::IngesterChunk, parquet::QuerierParquetChunk, IngesterPartition};

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

        let chunks = self.sync_partition_sort_keys(chunks);

        let chunks: Vec<Arc<dyn QueryChunk>> = chunks
            .into_iter()
            .map(|c| c.upcast_to_querier_chunk().into())
            .collect();

        Ok(chunks)
    }

    async fn build_chunks_from_parquet(
        &self,
        ingester_partitions: &[IngesterPartition],
        retention_delete_pred: Option<Arc<DeletePredicate>>,
        parquet_files: Vec<QuerierParquetChunk>,
        _span: Option<Span>,
    ) -> Result<Vec<Box<dyn UpdatableQuerierChunk>>, ReconcileError> {
        debug!(
            namespace=%self.namespace_name(),
            table_name=%self.table_name(),
            num_parquet_files=parquet_files.len(),
            "Reconciling "
        );

        debug!(num_chunks=%parquet_files.len(), "Created chunks from parquet files");

        let mut chunks: Vec<Box<dyn UpdatableQuerierChunk>> =
            Vec::with_capacity(parquet_files.len() + ingester_partitions.len());

        let retention_expr_len = usize::from(retention_delete_pred.is_some());
        for chunk in parquet_files.into_iter() {
            let mut delete_predicates = Vec::with_capacity(retention_expr_len);

            if let Some(retention_delete_pred) = retention_delete_pred.clone() {
                delete_predicates.push(retention_delete_pred);
            }

            let chunk = chunk.with_delete_predicates(delete_predicates);

            chunks.push(Box::new(chunk) as Box<dyn UpdatableQuerierChunk>);
        }

        Ok(chunks)
    }

    fn build_ingester_chunks(
        &self,
        ingester_partitions: Vec<IngesterPartition>,
        retention_delete_pred: Option<Arc<DeletePredicate>>,
    ) -> impl Iterator<Item = Box<dyn UpdatableQuerierChunk>> {
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
            .map(|c| Box::new(c) as Box<dyn UpdatableQuerierChunk>)
    }

    fn sync_partition_sort_keys(
        &self,
        chunks: Vec<Box<dyn UpdatableQuerierChunk>>,
    ) -> Vec<Box<dyn UpdatableQuerierChunk>> {
        // collect latest (= longest) sort key
        // Note that the partition sort key may stale (only a subset of the most recent partition
        // sort key) because newer chunks have new columns.
        // However,  since the querier doesn't (yet) know about these chunks in the `chunks` list above
        // using the most up to date sort key from the chunks it does know about is sufficient.
        let mut sort_keys = HashMap::<PartitionId, Arc<SortKey>>::new();
        for c in &chunks {
            if let Some(sort_key) = c.partition_sort_key_arc() {
                match sort_keys.entry(c.partition_id()) {
                    Entry::Occupied(mut o) => {
                        if sort_key.len() > o.get().len() {
                            *o.get_mut() = sort_key;
                        }
                    }
                    Entry::Vacant(v) => {
                        v.insert(sort_key);
                    }
                }
            }
        }

        // write partition sort keys to chunks
        chunks
            .into_iter()
            .map(|chunk| {
                let partition_id = chunk.partition_id();
                let sort_key = sort_keys.get(&partition_id);
                chunk.update_partition_sort_key(sort_key.cloned())
            })
            .collect()
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

trait UpdatableQuerierChunk: QueryChunk {
    fn partition_sort_key_arc(&self) -> Option<Arc<SortKey>>;

    fn update_partition_sort_key(
        self: Box<Self>,
        sort_key: Option<Arc<SortKey>>,
    ) -> Box<dyn UpdatableQuerierChunk>;

    fn upcast_to_querier_chunk(self: Box<Self>) -> Box<dyn QueryChunk>;
}

impl UpdatableQuerierChunk for QuerierParquetChunk {
    fn partition_sort_key_arc(&self) -> Option<Arc<SortKey>> {
        self.partition_sort_key_arc()
    }

    fn update_partition_sort_key(
        self: Box<Self>,
        sort_key: Option<Arc<SortKey>>,
    ) -> Box<dyn UpdatableQuerierChunk> {
        Box::new(self.with_partition_sort_key(sort_key))
    }

    fn upcast_to_querier_chunk(self: Box<Self>) -> Box<dyn QueryChunk> {
        self as _
    }
}

impl UpdatableQuerierChunk for IngesterChunk {
    fn partition_sort_key_arc(&self) -> Option<Arc<SortKey>> {
        self.partition_sort_key_arc()
    }

    fn update_partition_sort_key(
        self: Box<Self>,
        sort_key: Option<Arc<SortKey>>,
    ) -> Box<dyn UpdatableQuerierChunk> {
        Box::new(self.with_partition_sort_key(sort_key))
    }

    fn upcast_to_querier_chunk(self: Box<Self>) -> Box<dyn QueryChunk> {
        self as _
    }
}

#[cfg(test)]
mod tests {
    use super::{
        interface::{IngesterPartitionInfo, ParquetFileInfo},
        *,
    };
    use data_types::{CompactionLevel, SequenceNumber, ShardId};

    #[derive(Debug)]
    struct MockIngesterPartitionInfo {
        partition_id: PartitionId,
        shard_id: ShardId,
        parquet_max_sequence_number: Option<SequenceNumber>,
    }

    impl IngesterPartitionInfo for MockIngesterPartitionInfo {
        fn partition_id(&self) -> PartitionId {
            self.partition_id
        }

        fn shard_id(&self) -> ShardId {
            self.shard_id
        }

        fn parquet_max_sequence_number(&self) -> Option<SequenceNumber> {
            self.parquet_max_sequence_number
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct MockParquetFileInfo {
        partition_id: PartitionId,
        max_sequence_number: SequenceNumber,
        compaction_level: CompactionLevel,
    }

    impl ParquetFileInfo for MockParquetFileInfo {
        fn partition_id(&self) -> PartitionId {
            self.partition_id
        }

        fn max_sequence_number(&self) -> SequenceNumber {
            self.max_sequence_number
        }

        fn compaction_level(&self) -> CompactionLevel {
            self.compaction_level
        }
    }
}
