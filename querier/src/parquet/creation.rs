use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use data_types::{ChunkId, ChunkOrder, ColumnId, ParquetFile, PartitionId, TimestampMinMax};
use futures::StreamExt;
use iox_catalog::interface::Catalog;
use iox_query::pruning::prune_summaries;
use observability_deps::tracing::debug;
use parquet_file::chunk::ParquetChunk;
use predicate::Predicate;
use rand::{rngs::StdRng, seq::SliceRandom, SeedableRng};
use schema::sort::SortKey;
use trace::span::{Span, SpanRecorder};
use uuid::Uuid;

use crate::{
    cache::{namespace::CachedTable, partition::CachedPartition, CatalogCache},
    df_stats::create_chunk_statistics,
    parquet::QuerierParquetChunkMeta,
    table::MetricPruningObserver,
    CONCURRENT_CHUNK_CREATION_JOBS,
};

use super::QuerierParquetChunk;

/// Adapter that can create chunks.
#[derive(Debug)]
pub struct ChunkAdapter {
    /// Cache
    catalog_cache: Arc<CatalogCache>,

    /// Metric registry.
    metric_registry: Arc<metric::Registry>,
}

impl ChunkAdapter {
    /// Create new adapter with empty cache.
    pub fn new(catalog_cache: Arc<CatalogCache>, metric_registry: Arc<metric::Registry>) -> Self {
        Self {
            catalog_cache,
            metric_registry,
        }
    }

    /// Metric registry getter.
    pub fn metric_registry(&self) -> Arc<metric::Registry> {
        Arc::clone(&self.metric_registry)
    }

    /// Get underlying catalog cache.
    pub fn catalog_cache(&self) -> &Arc<CatalogCache> {
        &self.catalog_cache
    }

    /// Get underlying catalog.
    pub fn catalog(&self) -> Arc<dyn Catalog> {
        self.catalog_cache.catalog()
    }

    pub(crate) async fn new_chunks(
        &self,
        cached_table: Arc<CachedTable>,
        files: Arc<[Arc<ParquetFile>]>,
        predicate: &Predicate,
        early_pruning_observer: MetricPruningObserver,
        cached_partitions: &HashMap<PartitionId, CachedPartition>,
        span: Option<Span>,
    ) -> Vec<QuerierParquetChunk> {
        let span_recorder = SpanRecorder::new(span);

        // throw out files that belong to removed partitions
        let files = files
            .iter()
            .filter(|f| cached_partitions.contains_key(&f.partition_id))
            .cloned()
            .collect::<Vec<_>>();

        let chunk_stats: Vec<_> = {
            let _span_recorder = span_recorder.child("create chunk stats");

            files
                .iter()
                .map(|p| {
                    let stats = Arc::new(create_chunk_statistics(
                        p.row_count as u64,
                        &cached_table.schema,
                        TimestampMinMax {
                            min: p.min_time.get(),
                            max: p.max_time.get(),
                        },
                        &cached_partitions
                            .get(&p.partition_id)
                            .expect("filter files down to existing partitions")
                            .column_ranges,
                    ));
                    let schema = Arc::clone(cached_table.schema.inner());

                    (stats, schema)
                })
                .collect()
        };

        // Prune on the most basic summary data (timestamps and column names) before trying to fully load the chunks
        let keeps = {
            let _span_recorder = span_recorder.child("prune summaries");

            match prune_summaries(&cached_table.schema, &chunk_stats, predicate) {
                Ok(keeps) => keeps,
                Err(reason) => {
                    // Ignore pruning failures here - the chunk pruner should have already logged them.
                    // Just skip pruning and gather all the metadata. We have another chance to prune them
                    // once all the metadata is available
                    debug!(?reason, "Could not prune before metadata fetch");
                    vec![true; chunk_stats.len()]
                }
            }
        };

        // Remove any unused parquet files up front to maximize the
        // concurrent catalog requests that could be outstanding
        let mut parquet_files = files
            .iter()
            .zip(keeps)
            .filter_map(|(pf, keep)| {
                if keep {
                    Some(Arc::clone(pf))
                } else {
                    early_pruning_observer
                        .was_pruned_early(pf.row_count as u64, pf.file_size_bytes as u64);
                    None
                }
            })
            .collect::<Vec<_>>();

        // de-correlate parquet files so that subsequent items likely don't block/wait on the same cache lookup
        // (they are likely ordered by partition)
        //
        // Note that we sort before shuffling to achieve a deterministic pseudo-random order
        {
            let _span_recorder = span_recorder.child("shuffle order");

            let mut rng = StdRng::seed_from_u64(cached_table.id.get() as u64);
            parquet_files.sort_by_key(|f| f.id);
            parquet_files.shuffle(&mut rng);
        }

        {
            let span_recorder = span_recorder.child("create individual chunks");

            futures::stream::iter(parquet_files)
                .map(|cached_parquet_file| {
                    let span_recorder = &span_recorder;
                    let cached_table = Arc::clone(&cached_table);
                    let cached_partition = cached_partitions
                        .get(&cached_parquet_file.partition_id)
                        .expect("filter files down to existing partitions");
                    async move {
                        let span = span_recorder.child_span("new_chunk");
                        self.new_chunk(cached_table, cached_parquet_file, cached_partition, span)
                            .await
                    }
                })
                .buffer_unordered(CONCURRENT_CHUNK_CREATION_JOBS)
                .filter_map(|x| async { x })
                .collect()
                .await
        }
    }

    async fn new_chunk(
        &self,
        cached_table: Arc<CachedTable>,
        parquet_file: Arc<ParquetFile>,
        cached_partition: &CachedPartition,
        span: Option<Span>,
    ) -> Option<QuerierParquetChunk> {
        let span_recorder = SpanRecorder::new(span);

        let parquet_file_cols: HashSet<ColumnId> =
            parquet_file.column_set.iter().copied().collect();

        let partition_sort_key = cached_partition
            .sort_key
            .as_ref()
            .expect("partition sort key should be set when a parquet file exists");

        // NOTE: Because we've looked up the sort key AFTER the namespace schema, it may contain columns for which we
        //       don't have any schema information yet. This is OK because we've ensured that all file columns are known
        //       withing the schema and if a column is NOT part of the file, it will also not be part of the chunk sort
        //       key, so we have consistency here.

        // calculate schema
        // IMPORTANT: Do NOT use the sort key to list columns because the sort key only contains primary-key columns.
        // NOTE: The schema that we calculate here may have a different column order than the actual parquet file. This
        //       is OK because the IOx parquet reader can deal with that (see #4921).
        let column_ids: Vec<_> = cached_table
            .column_id_map
            .keys()
            .filter(|id| parquet_file_cols.contains(id))
            .copied()
            .collect();
        let schema = self
            .catalog_cache
            .projected_schema()
            .get(
                Arc::clone(&cached_table),
                column_ids,
                span_recorder.child_span("cache GET projected schema"),
            )
            .await;

        // calculate sort key
        let sort_key = SortKey::from_columns(
            partition_sort_key
                .column_order
                .iter()
                .filter(|c_id| parquet_file_cols.contains(c_id))
                .filter_map(|c_id| cached_table.column_id_map.get(c_id))
                .cloned(),
        );
        assert!(
            !sort_key.is_empty(),
            "Sort key can never be empty because there should at least be a time column",
        );

        let chunk_id = ChunkId::from(Uuid::from_u128(parquet_file.id.get() as _));

        let order = ChunkOrder::new(parquet_file.max_l0_created_at.get());

        let meta = Arc::new(QuerierParquetChunkMeta {
            chunk_id,
            order,
            sort_key: Some(sort_key),
            partition_id: parquet_file.partition_id,
        });

        let parquet_chunk = Arc::new(ParquetChunk::new(
            parquet_file,
            schema,
            self.catalog_cache.parquet_store(),
        ));

        Some(QuerierParquetChunk::new(
            parquet_chunk,
            meta,
            Arc::clone(&cached_partition.column_ranges),
        ))
    }
}
