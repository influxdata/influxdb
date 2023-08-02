//! Querier Chunks

use data_types::{ChunkId, ChunkOrder, TransitionPartitionId};
use datafusion::physical_plan::Statistics;
use iox_query::chunk_statistics::{create_chunk_statistics, ColumnRanges};
use parquet_file::chunk::ParquetChunk;
use schema::sort::SortKey;
use std::sync::Arc;

mod creation;
mod query_access;

pub use creation::ChunkAdapter;

/// Immutable metadata attached to a [`QuerierParquetChunk`].
#[derive(Debug)]
pub struct QuerierParquetChunkMeta {
    /// The ID of the chunk
    chunk_id: ChunkId,

    /// Chunk order.
    order: ChunkOrder,

    /// Sort key.
    sort_key: Option<SortKey>,

    /// Partition identifier.
    partition_id: TransitionPartitionId,
}

impl QuerierParquetChunkMeta {
    /// Chunk order.
    pub fn order(&self) -> ChunkOrder {
        self.order
    }

    /// Sort key.
    pub fn sort_key(&self) -> Option<&SortKey> {
        self.sort_key.as_ref()
    }

    /// Partition identifier.
    pub fn partition_id(&self) -> &TransitionPartitionId {
        &self.partition_id
    }
}

#[derive(Debug)]
pub struct QuerierParquetChunk {
    /// Immutable chunk metadata
    meta: Arc<QuerierParquetChunkMeta>,

    /// Chunk of the Parquet file
    parquet_chunk: Arc<ParquetChunk>,

    /// Stats
    stats: Arc<Statistics>,
}

impl QuerierParquetChunk {
    /// Create new parquet-backed chunk (object store data).
    pub fn new(
        parquet_chunk: Arc<ParquetChunk>,
        meta: Arc<QuerierParquetChunkMeta>,
        column_ranges: ColumnRanges,
    ) -> Self {
        let stats = Arc::new(create_chunk_statistics(
            parquet_chunk.rows() as u64,
            parquet_chunk.schema(),
            Some(parquet_chunk.timestamp_min_max()),
            &column_ranges,
        ));

        Self {
            meta,
            parquet_chunk,
            stats,
        }
    }

    /// Get metadata attached to the given chunk.
    pub fn meta(&self) -> &QuerierParquetChunkMeta {
        self.meta.as_ref()
    }

    pub fn estimate_size(&self) -> usize {
        self.parquet_chunk.parquet_file().file_size_bytes as usize
    }

    pub fn rows(&self) -> usize {
        self.parquet_chunk.rows()
    }
}

#[cfg(test)]
pub mod tests {
    use std::collections::HashMap;

    use crate::cache::{
        namespace::{CachedNamespace, CachedTable},
        partition::PartitionRequest,
        CatalogCache,
    };

    use super::*;
    use arrow::{datatypes::DataType, record_batch::RecordBatch};
    use arrow_util::assert_batches_eq;
    use data_types::{ColumnType, ParquetFile};
    use datafusion_util::config::register_iox_object_store;
    use iox_query::{
        exec::{ExecutorType, IOxSessionContext},
        QueryChunk,
    };
    use iox_tests::{TestCatalog, TestParquetFileBuilder};
    use metric::{Attributes, Observation, RawReporter};
    use schema::{builder::SchemaBuilder, sort::SortKeyBuilder};
    use test_helpers::maybe_start_logging;
    use tokio::runtime::Handle;

    #[tokio::test]
    async fn test_new_parquet_chunk() {
        maybe_start_logging();
        let test_data = TestData::new().await;

        // create chunk
        let chunk = test_data.chunk().await;

        // check state
        assert_eq!(chunk.chunk_type(), "parquet");

        // measure catalog access
        let catalog_metrics1 = test_data.get_catalog_access_metrics();

        // check chunk schema
        assert_schema(&chunk);

        // check sort key
        assert_sort_key(&chunk);

        // back up stats
        let stats_1 = chunk.stats();

        // check if chunk can be queried
        assert_content(&chunk, &test_data).await;

        // check state again
        assert_eq!(chunk.chunk_type(), "parquet");

        // stats have NOT changed
        let stats_2 = chunk.stats();
        assert_eq!(stats_1, stats_2);

        // retrieving the chunk again should not require any catalog requests
        test_data.chunk().await;
        let catalog_metrics2 = test_data.get_catalog_access_metrics();
        assert_eq!(catalog_metrics1, catalog_metrics2);
    }

    /// collect data for the given chunk
    async fn collect_read_filter(
        chunk: &dyn QueryChunk,
        ctx: IOxSessionContext,
    ) -> Vec<RecordBatch> {
        chunk
            .data()
            .read_to_batches(chunk.schema(), ctx.inner())
            .await
    }

    struct TestData {
        catalog: Arc<TestCatalog>,
        parquet_file: Arc<ParquetFile>,
        adapter: ChunkAdapter,
        cached_table: Arc<CachedTable>,
    }

    impl TestData {
        async fn new() -> Self {
            let catalog = TestCatalog::new();

            let lp = vec![
                "table,tag1=WA field_int=1000i 8000",
                "table,tag1=VT field_int=10i 10000",
                "table,tag1=UT field_int=70i 20000",
            ]
            .join("\n");
            let ns = catalog.create_namespace_1hr_retention("ns").await;
            let table = ns.create_table("table").await;
            table.create_column("tag1", ColumnType::Tag).await;
            table.create_column("tag2", ColumnType::Tag).await;
            table.create_column("tag3", ColumnType::Tag).await;
            table.create_column("tag4", ColumnType::Tag).await;
            table.create_column("field_int", ColumnType::I64).await;
            table.create_column("field_float", ColumnType::F64).await;
            table.create_column("time", ColumnType::Time).await;
            let partition = table
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
                    catalog.object_store(),
                    &Handle::current(),
                )),
                catalog.metric_registry(),
            );

            let mut repos = catalog.catalog.repositories().await;
            let tables = repos
                .tables()
                .list_by_namespace_id(ns.namespace.id)
                .await
                .unwrap();
            let columns = repos
                .columns()
                .list_by_namespace_id(ns.namespace.id)
                .await
                .unwrap();
            let cached_namespace = CachedNamespace::new(ns.namespace.clone(), tables, columns);
            let cached_table =
                Arc::clone(cached_namespace.tables.get("table").expect("table exists"));

            Self {
                catalog,
                parquet_file,
                adapter,
                cached_table,
            }
        }

        async fn chunk(&self) -> QuerierParquetChunk {
            let cached_partition = self
                .adapter
                .catalog_cache()
                .partition()
                .get(
                    Arc::clone(&self.cached_table),
                    vec![PartitionRequest {
                        partition_id: self.parquet_file.partition_id.clone(),
                        sort_key_should_cover: vec![],
                    }],
                    None,
                )
                .await
                .into_iter()
                .next()
                .unwrap();
            let cached_partitions =
                HashMap::from([(self.parquet_file.partition_id.clone(), cached_partition)]);
            self.adapter
                .new_chunks(
                    Arc::clone(&self.cached_table),
                    vec![Arc::clone(&self.parquet_file)].into(),
                    &cached_partitions,
                    None,
                )
                .await
                .remove(0)
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

    fn assert_schema(chunk: &QuerierParquetChunk) {
        let expected_schema = SchemaBuilder::new()
            .field("field_int", DataType::Int64)
            .unwrap()
            .tag("tag1")
            .timestamp()
            .build()
            .unwrap();
        let actual_schema = chunk.schema();
        assert_eq!(actual_schema, &expected_schema);
    }

    fn assert_sort_key(chunk: &QuerierParquetChunk) {
        let expected_sort_key = SortKeyBuilder::new()
            .with_col("tag1")
            .with_col("time")
            .build();
        let actual_sort_key = chunk.sort_key().unwrap();
        assert_eq!(actual_sort_key, &expected_sort_key);
    }

    async fn assert_content(chunk: &QuerierParquetChunk, test_data: &TestData) {
        let ctx = test_data.catalog.exec.new_context(ExecutorType::Query);
        let parquet_store = test_data.adapter.catalog_cache().parquet_store();
        register_iox_object_store(
            ctx.inner().runtime_env(),
            parquet_store.id(),
            Arc::clone(parquet_store.object_store()),
        );

        let batches = collect_read_filter(chunk, ctx).await;

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
