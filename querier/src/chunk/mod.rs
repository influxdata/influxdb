//! Querier Chunks

use crate::cache::namespace::CachedTable;
use crate::cache::CatalogCache;
use data_types::{
    ChunkId, ChunkOrder, CompactionLevel, DeletePredicate, ParquetFile, ParquetFileId, PartitionId,
    SequenceNumber, ShardId, TableSummary, TimestampMinMax,
};
use iox_catalog::interface::Catalog;
use parking_lot::RwLock;
use parquet_file::{chunk::ParquetChunk, storage::ParquetStorage};
use read_buffer::RBChunk;
use schema::{sort::SortKey, Schema};
use std::collections::HashSet;
use std::{collections::HashMap, sync::Arc};
use trace::span::{Span, SpanRecorder};
use uuid::Uuid;

use self::util::create_basic_summary;

mod query_access;
pub(crate) mod util;

/// Immutable metadata attached to a [`QuerierChunk`].
#[derive(Debug)]
pub struct ChunkMeta {
    /// ID of the Parquet file of the chunk
    parquet_file_id: ParquetFileId,

    /// The ID of the chunk
    chunk_id: ChunkId,

    /// Table name
    table_name: Arc<str>,

    /// Chunk order.
    order: ChunkOrder,

    /// Sort key.
    sort_key: Option<SortKey>,

    /// Shard that created the data within this chunk.
    shard_id: ShardId,

    /// Partition ID.
    partition_id: PartitionId,

    /// The maximum sequence number within this chunk.
    max_sequence_number: SequenceNumber,

    /// Compaction level of the parquet file of the chunk
    compaction_level: CompactionLevel,
}

impl ChunkMeta {
    /// ID of the Parquet file of the chunk
    pub fn parquet_file_id(&self) -> ParquetFileId {
        self.parquet_file_id
    }

    /// Chunk order.
    pub fn order(&self) -> ChunkOrder {
        self.order
    }

    /// Sort key.
    pub fn sort_key(&self) -> Option<&SortKey> {
        self.sort_key.as_ref()
    }

    /// Shard that created the data within this chunk.
    pub fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    /// Partition ID.
    pub fn partition_id(&self) -> PartitionId {
        self.partition_id
    }

    /// The maximum sequence number within this chunk.
    pub fn max_sequence_number(&self) -> SequenceNumber {
        self.max_sequence_number
    }

    /// Compaction level of the parquet file of the chunk
    pub fn compaction_level(&self) -> CompactionLevel {
        self.compaction_level
    }
}

/// Internal [`QuerierChunk`] stage, i.e. how the data is loaded/organized.
#[derive(Debug)]
enum ChunkStage {
    ReadBuffer {
        /// Underlying read buffer chunk
        rb_chunk: Arc<RBChunk>,

        /// Table summary
        table_summary: Arc<TableSummary>,
    },
    Parquet {
        /// Chunk of the Parquet file
        parquet_chunk: Arc<ParquetChunk>,

        /// Table summary
        table_summary: Arc<TableSummary>,
    },
}

impl ChunkStage {
    /// Get stage-specific table summary.
    ///
    /// The table summary may improve if we have more chunk information (e.g. when the actual data was loaded into
    /// memory).
    pub fn table_summary(&self) -> &Arc<TableSummary> {
        match self {
            Self::Parquet { table_summary, .. } => table_summary,
            Self::ReadBuffer { table_summary, .. } => table_summary,
        }
    }

    /// Machine- and human-readable name of the stage.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Parquet { .. } => "parquet",
            Self::ReadBuffer { .. } => "read_buffer",
        }
    }

    pub fn load_to_read_buffer(&mut self, rb_chunk: Arc<RBChunk>) {
        match self {
            Self::Parquet { .. } => {
                let table_summary = Arc::new(rb_chunk.table_summary());

                *self = Self::ReadBuffer {
                    rb_chunk,
                    table_summary,
                };
            }
            Self::ReadBuffer { .. } => {
                // RB already loaded
            }
        }
    }

    fn estimate_size(&self) -> usize {
        match self {
            Self::Parquet { parquet_chunk, .. } => {
                parquet_chunk.parquet_file().file_size_bytes as usize
            }
            Self::ReadBuffer { rb_chunk, .. } => rb_chunk.size(),
        }
    }

    fn rows(&self) -> usize {
        match self {
            Self::Parquet { parquet_chunk, .. } => parquet_chunk.rows(),
            Self::ReadBuffer { rb_chunk, .. } => rb_chunk.rows() as usize,
        }
    }
}

impl From<Arc<ParquetChunk>> for ChunkStage {
    fn from(parquet_chunk: Arc<ParquetChunk>) -> Self {
        let table_summary = Arc::new(create_basic_summary(
            parquet_chunk.rows() as u64,
            &parquet_chunk.schema(),
            parquet_chunk.timestamp_min_max(),
        ));
        Self::Parquet {
            parquet_chunk,
            table_summary,
        }
    }
}

impl From<Arc<RBChunk>> for ChunkStage {
    fn from(rb_chunk: Arc<RBChunk>) -> Self {
        let table_summary = Arc::new(rb_chunk.table_summary());
        Self::ReadBuffer {
            rb_chunk,
            table_summary,
        }
    }
}

/// Controls if/how data for a [`QuerierChunk`] is loaded
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum QuerierChunkLoadSetting {
    /// Only stay in parquet mode and never use read buffer data.
    ParquetOnly,

    /// Only use read buffer data.
    ///
    /// This forces the querier to load the read buffer for this chunk.
    ReadBufferOnly,

    /// Default "on-demand" handling.
    ///
    /// When the chunk is created, it will look up if there is already read buffer data loaded (or loading is already in
    /// progress) and is that. Otherwise it will parquet. If the actual row data is requested (via
    /// [`QueryChunk::read_filter`](iox_query::QueryChunk::read_filter)) then it will force-load the read buffer.
    #[default]
    OnDemand,
}

#[derive(Debug)]
pub struct QuerierChunk {
    /// Immutable chunk metadata
    meta: Arc<ChunkMeta>,

    /// Schema of the chunk
    schema: Arc<Schema>,

    /// min/max time range of this chunk
    timestamp_min_max: TimestampMinMax,

    /// Delete predicates to be combined with the chunk
    delete_predicates: Vec<Arc<DeletePredicate>>,

    /// Partition sort key (how does the read buffer use this?)
    partition_sort_key: Arc<Option<SortKey>>,

    /// Cache
    catalog_cache: Arc<CatalogCache>,

    /// Object store.
    ///
    /// Internally, `ParquetStorage` wraps the actual store implementation in an `Arc`, so
    /// `ParquetStorage` is cheap to clone.
    store: ParquetStorage,

    /// Current stage.
    ///
    /// Wrapped in an `Arc` so we can move it into a future/stream this is detached from `self`.
    stage: Arc<RwLock<ChunkStage>>,

    /// Load setting.
    load_setting: QuerierChunkLoadSetting,
}

impl QuerierChunk {
    /// Create new parquet-backed chunk (object store data).
    pub async fn new(
        parquet_chunk: Arc<ParquetChunk>,
        meta: Arc<ChunkMeta>,
        partition_sort_key: Arc<Option<SortKey>>,
        catalog_cache: Arc<CatalogCache>,
        store: ParquetStorage,
        load_setting: QuerierChunkLoadSetting,
        span: Option<Span>,
    ) -> Self {
        let span_recorder = SpanRecorder::new(span);
        let schema = parquet_chunk.schema();
        let timestamp_min_max = parquet_chunk.timestamp_min_max();

        // maybe load read buffer
        let rb_chunk = match load_setting {
            QuerierChunkLoadSetting::ParquetOnly => {
                // never load
                None
            }
            QuerierChunkLoadSetting::OnDemand => {
                // depends on cache state
                catalog_cache
                    .read_buffer()
                    .peek(
                        meta.parquet_file_id,
                        span_recorder.child_span("cache PEEK read_buffer"),
                    )
                    .await
            }
            QuerierChunkLoadSetting::ReadBufferOnly => {
                // force load
                Some(
                    catalog_cache
                        .read_buffer()
                        .get(
                            Arc::clone(parquet_chunk.parquet_file()),
                            Arc::clone(&schema),
                            store.clone(),
                            span_recorder.child_span("cache GET read_buffer"),
                        )
                        .await,
                )
            }
        };

        let stage: ChunkStage = if let Some(rb_chunk) = rb_chunk {
            rb_chunk.into()
        } else {
            parquet_chunk.into()
        };

        Self {
            meta,
            delete_predicates: Vec::new(),
            partition_sort_key,
            catalog_cache,
            schema,
            timestamp_min_max,
            stage: Arc::new(RwLock::new(stage)),
            store,
            load_setting,
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

    /// Set partition sort key
    pub fn with_partition_sort_key(self, partition_sort_key: Arc<Option<SortKey>>) -> Self {
        Self {
            partition_sort_key,
            ..self
        }
    }

    pub fn estimate_size(&self) -> usize {
        self.stage.read().estimate_size()
    }

    pub fn rows(&self) -> usize {
        self.stage.read().rows()
    }
}

/// Adapter that can create chunks.
#[derive(Debug)]
pub struct ChunkAdapter {
    /// Cache
    catalog_cache: Arc<CatalogCache>,

    /// Object store.
    ///
    /// Internally, `ParquetStorage` wraps the actual store implementation in an `Arc`, so
    /// `ParquetStorage` is cheap to clone.
    store: ParquetStorage,

    /// Metric registry.
    metric_registry: Arc<metric::Registry>,

    /// Load settings for chunks
    load_settings: HashMap<ParquetFileId, QuerierChunkLoadSetting>,
}

impl ChunkAdapter {
    /// Create new adapter with empty cache.
    pub fn new(
        catalog_cache: Arc<CatalogCache>,
        store: ParquetStorage,
        metric_registry: Arc<metric::Registry>,
        load_settings: HashMap<ParquetFileId, QuerierChunkLoadSetting>,
    ) -> Self {
        Self {
            catalog_cache,
            store,
            metric_registry,
            load_settings,
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

    pub async fn new_chunk(
        &self,
        cached_table: &CachedTable,
        table_name: Arc<str>,
        parquet_file: Arc<ParquetFile>,
        span: Option<Span>,
    ) -> Option<QuerierChunk> {
        let span_recorder = SpanRecorder::new(span);
        let parts = self
            .chunk_parts(
                cached_table,
                table_name,
                Arc::clone(&parquet_file),
                span_recorder.child_span("chunk_parts"),
            )
            .await?;

        let parquet_chunk = Arc::new(ParquetChunk::new(
            parquet_file,
            parts.schema,
            self.store.clone(),
        ));
        let load_settings = self
            .load_settings
            .get(&parts.meta.parquet_file_id)
            .copied()
            .unwrap_or_default();

        Some(
            QuerierChunk::new(
                parquet_chunk,
                parts.meta,
                parts.partition_sort_key,
                Arc::clone(&self.catalog_cache),
                self.store.clone(),
                load_settings,
                span_recorder.child_span("QuerierChunk::new"),
            )
            .await,
        )
    }

    async fn chunk_parts(
        &self,
        cached_table: &CachedTable,
        table_name: Arc<str>,
        parquet_file: Arc<ParquetFile>,
        span: Option<Span>,
    ) -> Option<ChunkParts> {
        let span_recorder = SpanRecorder::new(span);

        let parquet_file_cols: HashSet<_> = parquet_file
            .column_set
            .iter()
            .map(|id| {
                cached_table
                    .column_id_map
                    .get(id)
                    .expect("catalog has all columns")
                    .as_ref()
            })
            .collect();

        // relevant_pk_columns is everything from the primary key for the table, that is actually in this parquet file
        let relevant_pk_columns: Vec<_> = cached_table
            .schema
            .primary_key()
            .into_iter()
            .filter(|c| parquet_file_cols.contains(*c))
            .collect();
        let partition_sort_key = self
            .catalog_cache
            .partition()
            .sort_key(
                parquet_file.partition_id,
                &relevant_pk_columns,
                span_recorder.child_span("cache GET partition sort key"),
            )
            .await;
        let partition_sort_key_ref = partition_sort_key
            .as_ref()
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
        let column_names: Vec<_> = cached_table
            .schema
            .iter()
            .filter_map(|(_, field)| {
                let name = field.name();
                if parquet_file_cols.contains(name.as_str()) {
                    Some(name.clone())
                } else {
                    None
                }
            })
            .collect();
        let schema = self
            .catalog_cache
            .projected_schema()
            .get(
                parquet_file.table_id,
                Arc::clone(&cached_table.schema),
                column_names,
                span_recorder.child_span("cache GET projected schema"),
            )
            .await;

        // calculate sort key
        let pk_cols = schema.primary_key();
        let sort_key = partition_sort_key_ref.filter_to(&pk_cols);
        assert!(
            !sort_key.is_empty(),
            "Sort key can never be empty because there should at least be a time column",
        );

        let chunk_id = ChunkId::from(Uuid::from_u128(parquet_file.id.get() as _));

        let order = ChunkOrder::new(parquet_file.max_sequence_number.get());

        let meta = Arc::new(ChunkMeta {
            parquet_file_id: parquet_file.id,
            chunk_id,
            table_name,
            order,
            sort_key: Some(sort_key),
            shard_id: parquet_file.shard_id,
            partition_id: parquet_file.partition_id,
            max_sequence_number: parquet_file.max_sequence_number,
            compaction_level: parquet_file.compaction_level,
        });

        Some(ChunkParts {
            meta,
            schema,
            partition_sort_key,
        })
    }
}

struct ChunkParts {
    meta: Arc<ChunkMeta>,
    schema: Arc<Schema>,
    partition_sort_key: Arc<Option<SortKey>>,
}

#[cfg(test)]
pub mod tests {
    use crate::cache::namespace::CachedNamespace;

    use super::*;
    use arrow::{datatypes::DataType, record_batch::RecordBatch};
    use arrow_util::assert_batches_eq;
    use data_types::{ColumnType, NamespaceSchema};
    use futures::StreamExt;
    use iox_query::{exec::IOxSessionContext, QueryChunk, QueryChunkMeta};
    use iox_tests::util::{TestCatalog, TestNamespace, TestParquetFileBuilder};
    use metric::{Attributes, Observation, RawReporter};
    use schema::{builder::SchemaBuilder, selection::Selection, sort::SortKeyBuilder};
    use test_helpers::maybe_start_logging;
    use tokio::runtime::Handle;

    #[tokio::test]
    async fn test_new_rb_chunk() {
        maybe_start_logging();
        let test_data = TestData::new(QuerierChunkLoadSetting::ReadBufferOnly).await;
        let namespace_schema = Arc::new(test_data.ns.schema().await);

        // create chunk
        let chunk = test_data.chunk(Arc::clone(&namespace_schema)).await;

        // check state
        assert_eq!(chunk.chunk_type(), "read_buffer");

        // measure catalog access
        let catalog_metrics1 = test_data.get_catalog_access_metrics();

        // check chunk schema
        assert_schema(&chunk);

        // check sort key
        assert_sort_key(&chunk);

        // back up table summary
        let table_summary_1 = chunk.summary().unwrap();

        // check if chunk can be queried
        assert_content(&chunk).await;

        // check state again
        assert_eq!(chunk.chunk_type(), "read_buffer");

        // summary has NOT changed
        let table_summary_2 = chunk.summary().unwrap();
        assert_eq!(table_summary_1, table_summary_2);

        // retrieving the chunk again should not require any catalog requests
        test_data.chunk(namespace_schema).await;
        let catalog_metrics2 = test_data.get_catalog_access_metrics();
        assert_eq!(catalog_metrics1, catalog_metrics2);
    }

    #[tokio::test]
    async fn test_new_parquet_chunk() {
        maybe_start_logging();
        let test_data = TestData::new(QuerierChunkLoadSetting::ParquetOnly).await;
        let namespace_schema = Arc::new(test_data.ns.schema().await);

        // create chunk
        let chunk = test_data.chunk(Arc::clone(&namespace_schema)).await;

        // check state
        assert_eq!(chunk.chunk_type(), "parquet");

        // measure catalog access
        let catalog_metrics1 = test_data.get_catalog_access_metrics();

        // check chunk schema
        assert_schema(&chunk);

        // check sort key
        assert_sort_key(&chunk);

        // back up table summary
        let table_summary_1 = chunk.summary().unwrap();

        // check if chunk can be queried
        assert_content(&chunk).await;

        // check state again
        assert_eq!(chunk.chunk_type(), "parquet");

        // summary has NOT changed
        let table_summary_2 = chunk.summary().unwrap();
        assert_eq!(table_summary_1, table_summary_2);

        // retrieving the chunk again should not require any catalog requests
        test_data.chunk(namespace_schema).await;
        let catalog_metrics2 = test_data.get_catalog_access_metrics();
        assert_eq!(catalog_metrics1, catalog_metrics2);
    }

    #[tokio::test]
    async fn test_new_on_demand_chunk() {
        maybe_start_logging();
        let test_data = TestData::new(QuerierChunkLoadSetting::OnDemand).await;
        let namespace_schema = Arc::new(test_data.ns.schema().await);

        // create chunk
        let chunk = test_data.chunk(Arc::clone(&namespace_schema)).await;

        // check state
        assert_eq!(chunk.chunk_type(), "parquet");

        // measure catalog access
        let catalog_metrics1 = test_data.get_catalog_access_metrics();

        // check chunk schema
        assert_schema(&chunk);

        // check sort key
        assert_sort_key(&chunk);

        // back up table summary
        let table_summary_1 = chunk.summary().unwrap();

        // check if chunk can be queried
        assert_content(&chunk).await;

        // check state again
        assert_eq!(chunk.chunk_type(), "read_buffer");

        // summary has changed
        let table_summary_2 = chunk.summary().unwrap();
        assert_ne!(table_summary_1, table_summary_2);

        // retrieving the chunk again should not require any catalog requests
        test_data.chunk(namespace_schema).await;
        let catalog_metrics2 = test_data.get_catalog_access_metrics();
        assert_eq!(catalog_metrics1, catalog_metrics2);
    }

    /// collect data for the given chunk
    async fn collect_read_filter(chunk: &dyn QueryChunk) -> Vec<RecordBatch> {
        chunk
            .read_filter(
                IOxSessionContext::with_testing(),
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

    struct TestData {
        catalog: Arc<TestCatalog>,
        ns: Arc<TestNamespace>,
        parquet_file: Arc<ParquetFile>,
        adapter: ChunkAdapter,
    }

    impl TestData {
        async fn new(load_settings: QuerierChunkLoadSetting) -> Self {
            let catalog = TestCatalog::new();

            let lp = vec![
                "table,tag1=WA field_int=1000i 8000",
                "table,tag1=VT field_int=10i 10000",
                "table,tag1=UT field_int=70i 20000",
            ]
            .join("\n");
            let ns = catalog.create_namespace("ns").await;
            let shard = ns.create_shard(1).await;
            let table = ns.create_table("table").await;
            table.create_column("tag1", ColumnType::Tag).await;
            table.create_column("tag2", ColumnType::Tag).await;
            table.create_column("tag3", ColumnType::Tag).await;
            table.create_column("field_int", ColumnType::I64).await;
            table.create_column("field_float", ColumnType::F64).await;
            table.create_column("time", ColumnType::Time).await;
            let partition = table
                .with_shard(&shard)
                .create_partition("part")
                .await
                .update_sort_key(SortKey::from_columns(["tag1", "tag2", "tag4", "time"]))
                .await;
            let builder = TestParquetFileBuilder::default().with_line_protocol(&lp);
            let parquet_file = Arc::new(partition.create_parquet_file(builder).await.parquet_file);

            let adapter = ChunkAdapter::new(
                Arc::new(CatalogCache::new_testing(
                    catalog.catalog(),
                    catalog.time_provider(),
                    catalog.metric_registry(),
                    &Handle::current(),
                )),
                ParquetStorage::new(catalog.object_store()),
                catalog.metric_registry(),
                HashMap::from([(parquet_file.id, load_settings)]),
            );

            Self {
                catalog,
                ns,
                parquet_file,
                adapter,
            }
        }

        async fn chunk(&self, namespace_schema: Arc<NamespaceSchema>) -> QuerierChunk {
            let cached_namespace: CachedNamespace = namespace_schema.as_ref().into();
            let cached_table = cached_namespace.tables.get("table").expect("table exists");
            self.adapter
                .new_chunk(
                    cached_table,
                    Arc::from("table"),
                    Arc::clone(&self.parquet_file),
                    None,
                )
                .await
                .unwrap()
        }

        /// get catalog access metrics from metric registry
        fn get_catalog_access_metrics(&self) -> Vec<(Attributes, u64)> {
            let mut reporter = RawReporter::default();
            self.catalog.metric_registry.report(&mut reporter);

            let mut metrics: Vec<_> = reporter
                .observations()
                .iter()
                .find(|o| o.metric_name == "catalog_op_duration")
                .expect("failed to read metric")
                .observations
                .iter()
                .map(|(a, o)| {
                    if let Observation::DurationHistogram(h) = o {
                        (a.clone(), h.sample_count())
                    } else {
                        panic!("wrong metric type")
                    }
                })
                .collect();
            metrics.sort();
            assert!(!metrics.is_empty());
            metrics
        }
    }

    fn assert_schema(chunk: &QuerierChunk) {
        let expected_schema = SchemaBuilder::new()
            .field("field_int", DataType::Int64)
            .tag("tag1")
            .timestamp()
            .build()
            .unwrap();
        let actual_schema = chunk.schema();
        assert_eq!(actual_schema.as_ref(), &expected_schema);
    }

    fn assert_sort_key(chunk: &QuerierChunk) {
        let expected_sort_key = SortKeyBuilder::new()
            .with_col("tag1")
            .with_col("time")
            .build();
        let actual_sort_key = chunk.sort_key().unwrap();
        assert_eq!(actual_sort_key, &expected_sort_key);
    }

    async fn assert_content(chunk: &QuerierChunk) {
        let batches = collect_read_filter(chunk).await;

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
