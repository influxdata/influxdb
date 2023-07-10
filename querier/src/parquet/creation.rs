use std::{collections::HashMap, sync::Arc};

use data_types::{ChunkId, ChunkOrder, ColumnId, ParquetFile, TransitionPartitionId};
use futures::StreamExt;
use hashbrown::HashSet;
use iox_catalog::interface::Catalog;
use parquet_file::chunk::ParquetChunk;
use rand::{rngs::StdRng, seq::SliceRandom, SeedableRng};
use schema::{sort::SortKey, Schema};
use trace::span::{Span, SpanRecorder};
use uuid::Uuid;

use crate::{
    cache::{namespace::CachedTable, partition::CachedPartition, CatalogCache},
    parquet::QuerierParquetChunkMeta,
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
        cached_partitions: &HashMap<TransitionPartitionId, CachedPartition>,
        span: Option<Span>,
    ) -> Vec<QuerierParquetChunk> {
        let span_recorder = SpanRecorder::new(span);

        // prepare files
        let files = {
            let _span_recorder = span_recorder.child("prepare files");

            files
                .iter()
                // throw out files that belong to removed partitions
                .filter(|f| cached_partitions.contains_key(&f.partition_id))
                .cloned()
                .map(|f| PreparedParquetFile::new(f, &cached_table))
                .collect::<Vec<_>>()
        };

        // find all projected schemas
        let projections = {
            let span_recorder = span_recorder.child("get projected schemas");
            let mut projections: HashSet<Box<[ColumnId]>> = HashSet::with_capacity(files.len());
            for f in &files {
                projections.get_or_insert_owned(&f.col_list);
            }

            // de-correlate projections so that subsequent items likely don't block/wait on the same cache lookup
            // (they are likely ordered by partition)
            //
            // Note that we sort before shuffling to achieve a deterministic pseudo-random order
            let mut projections = projections.into_iter().collect::<Vec<_>>();
            projections.sort();
            let mut rng = StdRng::seed_from_u64(cached_table.id.get() as u64);
            projections.shuffle(&mut rng);

            futures::stream::iter(projections)
                .map(|column_ids| {
                    let span_recorder = &span_recorder;
                    let cached_table = Arc::clone(&cached_table);
                    async move {
                        let schema = self
                            .catalog_cache
                            .projected_schema()
                            .get(
                                cached_table,
                                column_ids.clone(),
                                span_recorder.child_span("cache GET projected schema"),
                            )
                            .await;
                        (column_ids, schema)
                    }
                })
                .buffer_unordered(CONCURRENT_CHUNK_CREATION_JOBS)
                .collect::<HashMap<_, _>>()
                .await
        };

        {
            let _span_recorder = span_recorder.child("finalize chunks");

            files
                .into_iter()
                .map(|file| {
                    let cached_table = Arc::clone(&cached_table);
                    let schema = projections
                        .get(&file.col_list)
                        .expect("looked up all projections")
                        .clone();
                    let cached_partition = cached_partitions
                        .get(&file.file.partition_id)
                        .expect("filter files down to existing partitions");
                    self.new_chunk(cached_table, file, schema, cached_partition)
                })
                .collect()
        }
    }

    fn new_chunk(
        &self,
        cached_table: Arc<CachedTable>,
        parquet_file: PreparedParquetFile,
        schema: Schema,
        cached_partition: &CachedPartition,
    ) -> QuerierParquetChunk {
        // NOTE: Because we've looked up the sort key AFTER the namespace schema, it may contain columns for which we
        //       don't have any schema information yet. This is OK because we've ensured that all file columns are known
        //       withing the schema and if a column is NOT part of the file, it will also not be part of the chunk sort
        //       key, so we have consistency here.

        // NOTE: The schema that we've projected here may have a different column order than the actual parquet file. This
        //       is OK because the IOx parquet reader can deal with that (see #4921).

        // calculate sort key
        let partition_sort_key = cached_partition
            .sort_key
            .as_ref()
            .expect("partition sort key should be set when a parquet file exists");
        let sort_key = SortKey::from_columns(
            partition_sort_key
                .column_order
                .iter()
                .filter(|c_id| parquet_file.col_set.contains(*c_id))
                .filter_map(|c_id| cached_table.column_id_map.get(c_id))
                .cloned(),
        );
        assert!(
            !sort_key.is_empty(),
            "Sort key can never be empty because there should at least be a time column",
        );

        let chunk_id = ChunkId::from(Uuid::from_u128(parquet_file.file.id.get() as _));

        let order = ChunkOrder::new(parquet_file.file.max_l0_created_at.get());

        let meta = Arc::new(QuerierParquetChunkMeta {
            chunk_id,
            order,
            sort_key: Some(sort_key),
            partition_id: parquet_file.file.partition_id.clone(),
        });

        let parquet_chunk = Arc::new(ParquetChunk::new(
            parquet_file.file,
            schema,
            self.catalog_cache.parquet_store(),
        ));

        QuerierParquetChunk::new(
            parquet_chunk,
            meta,
            Arc::clone(&cached_partition.column_ranges),
        )
    }
}

/// [`ParquetFile`] with some additional fields.
struct PreparedParquetFile {
    /// The parquet file as received from the catalog.
    file: Arc<ParquetFile>,

    /// The set of columns in this file.
    col_set: HashSet<ColumnId>,

    /// The columns in this file as ordered in the schema.
    col_list: Box<[ColumnId]>,
}

impl PreparedParquetFile {
    fn new(file: Arc<ParquetFile>, cached_table: &CachedTable) -> Self {
        let col_set: HashSet<ColumnId> = file
            .column_set
            .iter()
            .filter(|id| cached_table.column_id_map.contains_key(*id))
            .copied()
            .collect();

        let mut col_list = col_set.iter().copied().collect::<Box<[ColumnId]>>();
        col_list.sort();

        Self {
            file,
            col_set,
            col_list,
        }
    }
}
