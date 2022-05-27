//! Querier Chunks

use crate::cache::CatalogCache;
use arrow::record_batch::RecordBatch;
use data_types::{
    ChunkId, ChunkOrder, DeletePredicate, ParquetFileId, ParquetFileWithMetadata, PartitionId,
    SequenceNumber, SequencerId, TableSummary, TimestampMinMax, TimestampRange,
};
use futures::StreamExt;
use iox_catalog::interface::Catalog;
use iox_query::{exec::IOxSessionContext, QueryChunk};
use iox_time::TimeProvider;
use parquet_file::{
    chunk::{ChunkMetrics as ParquetChunkMetrics, DecodedParquetFile, ParquetChunk},
    storage::ParquetStorage,
};
use read_buffer::RBChunk;
use schema::{selection::Selection, sort::SortKey, Schema};
use std::sync::Arc;
use uuid::Uuid;

mod query_access;

/// Immutable metadata attached to a [`QuerierParquetChunk`].
#[derive(Debug)]
pub struct ChunkMeta {
    /// The ID of the chunk
    chunk_id: ChunkId,

    /// Table name
    table_name: Arc<str>,

    /// Chunk order.
    order: ChunkOrder,

    /// Sort key.
    sort_key: Option<SortKey>,

    /// Sequencer that created the data within this chunk.
    sequencer_id: SequencerId,

    /// Partition ID.
    partition_id: PartitionId,

    /// The minimum sequence number within this chunk.
    min_sequence_number: SequenceNumber,

    /// The maximum sequence number within this chunk.
    max_sequence_number: SequenceNumber,
}

impl ChunkMeta {
    /// Chunk order.
    pub fn order(&self) -> ChunkOrder {
        self.order
    }

    /// Sort key.
    pub fn sort_key(&self) -> Option<&SortKey> {
        self.sort_key.as_ref()
    }

    /// Sequencer that created the data within this chunk.
    pub fn sequencer_id(&self) -> SequencerId {
        self.sequencer_id
    }

    /// Partition ID.
    pub fn partition_id(&self) -> PartitionId {
        self.partition_id
    }

    /// The minimum sequence number within this chunk.
    pub fn min_sequence_number(&self) -> SequenceNumber {
        self.min_sequence_number
    }

    /// The maximum sequence number within this chunk.
    pub fn max_sequence_number(&self) -> SequenceNumber {
        self.max_sequence_number
    }
}

/// Chunk representation of `read_buffer::RBChunk`s for the querier.
#[derive(Debug)]
pub struct QuerierRBChunk {
    /// ID of the Parquet file of the chunk
    parquet_file_id: ParquetFileId,

    /// Underlying read buffer chunk
    rb_chunk: Arc<RBChunk>,

    /// Table summary
    table_summary: TableSummary,

    /// min/max time range of this table (extracted from TableSummary), if known
    timestamp_min_max: Option<TimestampMinMax>,

    /// Immutable chunk metadata
    meta: Arc<ChunkMeta>,

    /// Schema of the chunk
    schema: Arc<Schema>,

    /// Delete predicates to be combined with the chunk
    delete_predicates: Vec<Arc<DeletePredicate>>,

    /// Partition sort key (how does the read buffer use this?)
    partition_sort_key: Arc<Option<SortKey>>,
}

impl QuerierRBChunk {
    /// Create new read-buffer-backed chunk
    pub fn new(
        parquet_file_id: ParquetFileId,
        rb_chunk: Arc<RBChunk>,
        meta: Arc<ChunkMeta>,
        schema: Arc<Schema>,
        partition_sort_key: Arc<Option<SortKey>>,
    ) -> Self {
        let table_summary = rb_chunk.table_summary();
        let timestamp_min_max = table_summary.time_range();

        Self {
            parquet_file_id,
            rb_chunk,
            table_summary,
            timestamp_min_max,
            meta,
            schema,
            delete_predicates: Vec::new(),
            partition_sort_key,
        }
    }

    /// Set delete predicates of the given chunk.
    pub fn with_delete_predicates(self, delete_predicates: Vec<Arc<DeletePredicate>>) -> Self {
        Self {
            delete_predicates,
            ..self
        }
    }

    /// Get metadata attached to the given chunk.
    pub fn meta(&self) -> &ChunkMeta {
        self.meta.as_ref()
    }

    /// Parquet file ID
    pub fn parquet_file_id(&self) -> ParquetFileId {
        self.parquet_file_id
    }

    /// Set partition sort key
    pub fn with_partition_sort_key(self, partition_sort_key: Arc<Option<SortKey>>) -> Self {
        Self {
            partition_sort_key,
            ..self
        }
    }

    /// Return true if this chunk contains values within the time range, or if the range is `None`.
    pub fn has_timerange(&self, timestamp_range: Option<&TimestampRange>) -> bool {
        match (self.timestamp_min_max, timestamp_range) {
            (Some(timestamp_min_max), Some(timestamp_range)) => {
                timestamp_min_max.overlaps(*timestamp_range)
            }
            // If this chunk doesn't have a time column it can't match
            (None, Some(_)) => false,
            // If there no range specified,
            (_, None) => true,
        }
    }
}

/// Chunk representation of Parquet file chunks for the querier.
///
/// These chunks are usually created on-demand. The querier cache system does not really have a
/// notion of chunks (rather it knows about parquet files, local FS caches, ingester data, cached
/// read buffers) but we need to combine all that knowledge into chunk objects because this is what
/// the query engine (DataFusion and InfluxRPC) expect.
#[derive(Debug)]
pub struct QuerierParquetChunk {
    /// ID of the Parquet file of the chunk
    parquet_file_id: ParquetFileId,

    /// Chunk of the Parquet file
    parquet_chunk: Arc<ParquetChunk>,

    /// Immutable metadata.
    meta: Arc<ChunkMeta>,

    /// Delete predicates of this chunk
    delete_predicates: Vec<Arc<DeletePredicate>>,

    /// Partition sort key
    partition_sort_key: Arc<Option<SortKey>>,
}

impl QuerierParquetChunk {
    /// Create new parquet-backed chunk (object store data).
    pub fn new(
        parquet_file_id: ParquetFileId,
        parquet_chunk: Arc<ParquetChunk>,
        meta: Arc<ChunkMeta>,
        partition_sort_key: Arc<Option<SortKey>>,
    ) -> Self {
        Self {
            parquet_file_id,
            parquet_chunk,
            meta,
            delete_predicates: Vec::new(),
            partition_sort_key,
        }
    }

    /// Set delete predicates of the given chunk.
    pub fn with_delete_predicates(self, delete_predicates: Vec<Arc<DeletePredicate>>) -> Self {
        Self {
            delete_predicates,
            ..self
        }
    }

    /// Set partition sort key
    pub fn with_partition_sort_key(self, partition_sort_key: Arc<Option<SortKey>>) -> Self {
        Self {
            partition_sort_key,
            ..self
        }
    }

    /// Get metadata attached to the given chunk.
    pub fn meta(&self) -> &ChunkMeta {
        self.meta.as_ref()
    }

    /// Parquet file ID
    pub fn parquet_file_id(&self) -> ParquetFileId {
        self.parquet_file_id
    }

    /// Return time range
    pub fn timestamp_min_max(&self) -> Option<TimestampMinMax> {
        self.parquet_chunk.timestamp_min_max()
    }

    /// Partition sort key
    pub fn partition_sort_key(&self) -> Option<&SortKey> {
        self.partition_sort_key.as_ref().as_ref()
    }
}

/// Adapter that can create chunks.
#[derive(Debug)]
pub struct ChunkAdapter {
    /// Cache
    catalog_cache: Arc<CatalogCache>,

    /// Object store.
    store: ParquetStorage,

    /// Metric registry.
    metric_registry: Arc<metric::Registry>,

    /// Time provider.
    #[allow(dead_code)]
    time_provider: Arc<dyn TimeProvider>,
}

impl ChunkAdapter {
    /// Create new adapter with empty cache.
    pub fn new(
        catalog_cache: Arc<CatalogCache>,
        store: ParquetStorage,
        metric_registry: Arc<metric::Registry>,
        time_provider: Arc<dyn TimeProvider>,
    ) -> Self {
        Self {
            catalog_cache,
            store,
            metric_registry,
            time_provider,
        }
    }

    /// Get underlying catalog cache.
    pub fn catalog_cache(&self) -> &Arc<CatalogCache> {
        &self.catalog_cache
    }

    /// Get underlying catalog.
    pub fn catalog(&self) -> Arc<dyn Catalog> {
        self.catalog_cache.catalog()
    }

    /// Create parquet chunk.
    ///
    /// Returns `None` if some data required to create this chunk is already gone from the catalog.
    async fn new_parquet_chunk(
        &self,
        decoded_parquet_file: &DecodedParquetFile,
    ) -> Option<ParquetChunk> {
        let metrics = ParquetChunkMetrics::new(self.metric_registry.as_ref());

        Some(ParquetChunk::new(
            decoded_parquet_file,
            metrics,
            self.store.clone(),
        ))
    }

    /// Create new querier Parquet chunk from a catalog record
    ///
    /// Returns `None` if some data required to create this chunk is already gone from the catalog.
    pub async fn new_querier_parquet_chunk_from_file_with_metadata(
        &self,
        parquet_file_with_metadata: ParquetFileWithMetadata,
    ) -> Option<QuerierParquetChunk> {
        let decoded_parquet_file = DecodedParquetFile::new(parquet_file_with_metadata);
        self.new_querier_parquet_chunk(&decoded_parquet_file).await
    }

    /// Create new querier Parquet chunk.
    ///
    /// Returns `None` if some data required to create this chunk is already gone from the catalog.
    pub async fn new_querier_parquet_chunk(
        &self,
        decoded_parquet_file: &DecodedParquetFile,
    ) -> Option<QuerierParquetChunk> {
        let parquet_file_id = decoded_parquet_file.parquet_file_id();
        let table_id = decoded_parquet_file.table_id();

        let chunk = Arc::new(self.new_parquet_chunk(decoded_parquet_file).await?);
        let chunk_id = ChunkId::from(Uuid::from_u128(parquet_file_id.get() as _));
        let table_name = self.catalog_cache.table().name(table_id).await?;

        // Somewhat hacky workaround because of implicit chunk orders, use min sequence number and
        // hope it doesn't overflow u32. Order is non-zero, se we need to add 1.
        let order = ChunkOrder::new(1 + decoded_parquet_file.min_sequence_number().get() as u32)
            .expect("cannot be zero");

        // Read partition sort key
        let partition_sort_key = self
            .catalog_cache()
            .partition()
            .sort_key(decoded_parquet_file.partition_id())
            .await;

        let meta = Arc::new(ChunkMeta {
            chunk_id,
            table_name,
            order,
            sort_key: decoded_parquet_file.sort_key().cloned(),
            sequencer_id: decoded_parquet_file.sequencer_id(),
            partition_id: decoded_parquet_file.partition_id(),
            min_sequence_number: decoded_parquet_file.min_sequence_number(),
            max_sequence_number: decoded_parquet_file.max_sequence_number(),
        });

        Some(QuerierParquetChunk::new(
            parquet_file_id,
            chunk,
            meta,
            partition_sort_key,
        ))
    }

    /// Create read buffer chunk. May be from the cache, may be from the parquet file.
    pub async fn new_rb_chunk(
        &self,
        decoded_parquet_file: &DecodedParquetFile,
    ) -> Option<QuerierRBChunk> {
        let parquet_file_id = decoded_parquet_file.parquet_file_id();
        let schema = decoded_parquet_file.schema();
        let chunk_id = ChunkId::from(Uuid::from_u128(parquet_file_id.get() as _));
        let table_name = self
            .catalog_cache
            .table()
            .name(decoded_parquet_file.table_id())
            .await?;

        let rb_chunk = self
            .catalog_cache()
            .read_buffer()
            .get(decoded_parquet_file)
            .await;

        // Somewhat hacky workaround because of implicit chunk orders, use min sequence number and
        // hope it doesn't overflow u32. Order is non-zero, se we need to add 1.
        let order = ChunkOrder::new(1 + decoded_parquet_file.min_sequence_number().get() as u32)
            .expect("cannot be zero");

        // Read partition sort key
        let partition_sort_key = self
            .catalog_cache()
            .partition()
            .sort_key(decoded_parquet_file.partition_id())
            .await;

        let meta = Arc::new(ChunkMeta {
            chunk_id,
            table_name,
            order,
            sort_key: decoded_parquet_file.sort_key().cloned(),
            sequencer_id: decoded_parquet_file.sequencer_id(),
            partition_id: decoded_parquet_file.partition_id(),
            min_sequence_number: decoded_parquet_file.min_sequence_number(),
            max_sequence_number: decoded_parquet_file.max_sequence_number(),
        });

        Some(QuerierRBChunk::new(
            parquet_file_id,
            rb_chunk,
            meta,
            schema,
            partition_sort_key,
        ))
    }
}

/// collect data for the given chunk
pub async fn collect_read_filter(chunk: &QuerierParquetChunk) -> Vec<RecordBatch> {
    chunk
        .read_filter(
            IOxSessionContext::default(),
            &Default::default(),
            Selection::All,
        )
        .unwrap()
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(Result::unwrap)
        .collect()
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use arrow::datatypes::DataType;
    use arrow_util::assert_batches_eq;
    use iox_query::QueryChunkMeta;
    use iox_tests::util::TestCatalog;
    use schema::{builder::SchemaBuilder, sort::SortKeyBuilder};

    #[tokio::test]
    async fn test_create_record() {
        let catalog = TestCatalog::new();

        let adapter = ChunkAdapter::new(
            Arc::new(CatalogCache::new(
                catalog.catalog(),
                catalog.time_provider(),
                catalog.metric_registry(),
                usize::MAX,
            )),
            ParquetStorage::new(catalog.object_store()),
            catalog.metric_registry(),
            catalog.time_provider(),
        );

        // set up catalog
        let lp = vec![
            "table,tag1=WA field_int=1000i 8000",
            "table,tag1=VT field_int=10i 10000",
            "table,tag1=UT field_int=70i 20000",
        ]
        .join("\n");
        let ns = catalog.create_namespace("ns").await;
        let sequencer = ns.create_sequencer(1).await;
        let table = ns.create_table("table").await;
        let parquet_file = table
            .with_sequencer(&sequencer)
            .create_partition("part")
            .await
            .create_parquet_file(&lp)
            .await
            .parquet_file;

        // create chunk
        let chunk = adapter
            .new_querier_parquet_chunk(&DecodedParquetFile::new(parquet_file))
            .await
            .unwrap();

        // check chunk schema
        let expected_schema = SchemaBuilder::new()
            .field("field_int", DataType::Int64)
            .tag("tag1")
            .timestamp()
            .build()
            .unwrap();
        let actual_schema = chunk.schema();
        assert_eq!(actual_schema.as_ref(), &expected_schema);

        // check sort key
        let expected_sort_key = SortKeyBuilder::new()
            .with_col("tag1")
            .with_col("time")
            .build();
        let actual_sort_key = chunk.sort_key().unwrap();
        assert_eq!(actual_sort_key, &expected_sort_key);

        // check if chunk can be queried
        let batches = collect_read_filter(&chunk).await;
        assert_batches_eq!(
            &[
                "+-----------+------+-----------------------------+",
                "| field_int | tag1 | time                        |",
                "+-----------+------+-----------------------------+",
                "| 70        | UT   | 1970-01-01T00:00:00.000020Z |",
                "| 10        | VT   | 1970-01-01T00:00:00.000010Z |",
                "| 1000      | WA   | 1970-01-01T00:00:00.000008Z |",
                "+-----------+------+-----------------------------+",
            ],
            &batches
        );
    }
}
