//! Cache Parquet file data in Read Buffer chunks.

use super::ram::RamSize;
use backoff::{Backoff, BackoffConfig};
use cache_system::{
    backend::{
        lru::{LruBackend, ResourcePool},
        resource_consumption::FunctionEstimator,
        shared::SharedBackend,
    },
    cache::{driver::CacheDriver, metrics::CacheWithMetrics, Cache},
    loader::{metrics::MetricsLoader, FunctionLoader},
};
use data_types::ParquetFileId;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;
use iox_time::TimeProvider;
use parquet_file::{chunk::DecodedParquetFile, storage::ParquetStorage};
use read_buffer::{ChunkMetrics, RBChunk};
use snafu::{ResultExt, Snafu};
use std::{collections::HashMap, mem, sync::Arc};

const CACHE_ID: &str = "read_buffer";

#[derive(Debug)]
struct ExtraFetchInfo {
    decoded_parquet_file: Arc<DecodedParquetFile>,
    table_name: Arc<str>,
    store: ParquetStorage,
}

type CacheT = Box<dyn Cache<K = ParquetFileId, V = Arc<RBChunk>, Extra = ExtraFetchInfo>>;

/// Cache for parquet file data decoded into read buffer chunks
#[derive(Debug)]
pub struct ReadBufferCache {
    cache: CacheT,

    /// Handle that allows clearing entries for existing cache entries
    _backend: SharedBackend<ParquetFileId, Arc<RBChunk>>,
}

impl ReadBufferCache {
    /// Create a new empty cache.
    pub fn new(
        backoff_config: BackoffConfig,
        time_provider: Arc<dyn TimeProvider>,
        metric_registry: Arc<metric::Registry>,
        ram_pool: Arc<ResourcePool<RamSize>>,
    ) -> Self {
        let metric_registry_captured = Arc::clone(&metric_registry);
        let loader = Box::new(FunctionLoader::new(
            move |_parquet_file_id, extra_fetch_info: ExtraFetchInfo| {
                let backoff_config = backoff_config.clone();
                let metric_registry = Arc::clone(&metric_registry_captured);

                async move {
                    let rb_chunk = Backoff::new(&backoff_config)
                        .retry_all_errors("get read buffer chunk from parquet file", || {
                            let decoded_parquet_file_for_load =
                                Arc::clone(&extra_fetch_info.decoded_parquet_file);
                            let table_name_for_load = Arc::clone(&extra_fetch_info.table_name);
                            let store_for_load = extra_fetch_info.store.clone();

                            async {
                                load_from_parquet_file(
                                    decoded_parquet_file_for_load,
                                    table_name_for_load,
                                    store_for_load,
                                    &metric_registry,
                                )
                                .await
                            }
                        })
                        .await
                        .expect("retry forever");

                    Arc::new(rb_chunk)
                }
            },
        ));

        let loader = Arc::new(MetricsLoader::new(
            loader,
            CACHE_ID,
            Arc::clone(&time_provider),
            &metric_registry,
        ));

        // add to memory pool
        let backend = Box::new(LruBackend::new(
            Box::new(HashMap::new()),
            Arc::clone(&ram_pool),
            CACHE_ID,
            Arc::new(FunctionEstimator::new(
                |k: &ParquetFileId, v: &Arc<RBChunk>| {
                    RamSize(mem::size_of_val(k) + mem::size_of_val(v) + v.size())
                },
            )),
        ));

        // get a direct handle so we can clear out entries as needed
        let _backend = SharedBackend::new(backend);

        let cache = Box::new(CacheDriver::new(loader, Box::new(_backend.clone())));
        let cache = Box::new(CacheWithMetrics::new(
            cache,
            CACHE_ID,
            time_provider,
            &metric_registry,
        ));

        Self { cache, _backend }
    }

    /// Get read buffer chunks from the cache or the Parquet file
    pub async fn get(
        &self,
        decoded_parquet_file: Arc<DecodedParquetFile>,
        table_name: Arc<str>,
        store: ParquetStorage,
    ) -> Arc<RBChunk> {
        self.cache
            .get(
                decoded_parquet_file.parquet_file_id(),
                ExtraFetchInfo {
                    decoded_parquet_file,
                    table_name,
                    store,
                },
            )
            .await
    }
}

#[derive(Debug, Snafu)]
enum LoadError {
    #[snafu(display("Error reading from storage: {}", source))]
    ReadingFromStorage {
        source: parquet_file::storage::ReadError,
    },

    #[snafu(display("Error building read buffer chunk: {}", source))]
    BuildingChunk { source: RBChunkError },
}

async fn load_from_parquet_file(
    decoded_parquet_file: Arc<DecodedParquetFile>,
    table_name: Arc<str>,
    store: ParquetStorage,
    metric_registry: &metric::Registry,
) -> Result<RBChunk, LoadError> {
    let record_batch_stream =
        record_batches_stream(decoded_parquet_file, store).context(ReadingFromStorageSnafu)?;
    read_buffer_chunk_from_stream(table_name, record_batch_stream, metric_registry)
        .await
        .context(BuildingChunkSnafu)
}

fn record_batches_stream(
    decoded_parquet_file: Arc<DecodedParquetFile>,
    store: ParquetStorage,
) -> Result<SendableRecordBatchStream, parquet_file::storage::ReadError> {
    let schema = decoded_parquet_file.schema().as_arrow();
    let iox_metadata = &decoded_parquet_file.iox_metadata;

    store.read_all(schema, iox_metadata)
}

#[derive(Debug, Snafu)]
enum RBChunkError {
    #[snafu(display("Error streaming record batches: {}", source))]
    Streaming { source: arrow::error::ArrowError },

    #[snafu(display("Error pushing record batch into chunk: {}", source))]
    Pushing { source: arrow::error::ArrowError },

    #[snafu(display("Read buffer error: {}", source))]
    ReadBuffer { source: read_buffer::Error },
}

async fn read_buffer_chunk_from_stream(
    table_name: Arc<str>,
    mut stream: SendableRecordBatchStream,
    metric_registry: &metric::Registry,
) -> Result<RBChunk, RBChunkError> {
    let schema = stream.schema();

    // create "global" metric object, so that we don't blow up prometheus w/ too many metrics
    let metrics = ChunkMetrics::new(metric_registry, "iox_shared");

    let mut builder =
        read_buffer::RBChunkBuilder::new(table_name.as_ref(), schema).with_metrics(metrics);

    while let Some(record_batch) = stream.next().await {
        builder
            .push_record_batch(record_batch.context(StreamingSnafu)?)
            .context(PushingSnafu)?;
    }

    builder.build().context(ReadBufferSnafu)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::ram::test_util::test_ram_pool;
    use arrow::record_batch::RecordBatch;
    use arrow_util::assert_batches_eq;
    use datafusion_util::stream_from_batches;
    use iox_tests::util::{TestCatalog, TestPartition};
    use metric::{Attributes, CumulativeGauge, Metric, U64Counter};
    use mutable_batch_lp::test_helpers::lp_to_mutable_batch;
    use read_buffer::Predicate;
    use schema::selection::Selection;
    use std::time::Duration;

    fn make_cache(catalog: &TestCatalog) -> ReadBufferCache {
        ReadBufferCache::new(
            BackoffConfig::default(),
            catalog.time_provider(),
            catalog.metric_registry(),
            test_ram_pool(),
        )
    }

    async fn make_catalog() -> (Arc<TestCatalog>, Arc<TestPartition>) {
        let catalog = TestCatalog::new();
        let ns = catalog.create_namespace("ns").await;

        let table = ns.create_table("table1").await;
        let sequencer1 = ns.create_sequencer(1).await;

        let partition = table
            .with_sequencer(&sequencer1)
            .create_partition("k")
            .await;

        (catalog, partition)
    }

    #[tokio::test]
    async fn test_rb_chunks() {
        let (catalog, partition) = make_catalog().await;

        let test_parquet_file = partition.create_parquet_file("table1 foo=1 11").await;
        let parquet_file = test_parquet_file.parquet_file;
        let decoded = Arc::new(DecodedParquetFile::new(parquet_file));
        let storage = ParquetStorage::new(Arc::clone(&catalog.object_store));

        let cache = make_cache(&catalog);

        let rb = cache
            .get(Arc::clone(&decoded), "table1".into(), storage.clone())
            .await;

        let rb_batches: Vec<RecordBatch> = rb
            .read_filter(Predicate::default(), Selection::All, vec![])
            .unwrap()
            .collect();

        let expected = [
            "+-----+--------------------------------+",
            "| foo | time                           |",
            "+-----+--------------------------------+",
            "| 1   | 1970-01-01T00:00:00.000000011Z |",
            "+-----+--------------------------------+",
        ];

        assert_batches_eq!(expected, &rb_batches);

        // This should fetch from the cache
        let _rb_again = cache.get(decoded, "table1".into(), storage).await;

        let m: Metric<U64Counter> = catalog
            .metric_registry
            .get_instrument("cache_load_function_calls")
            .unwrap();
        let v = m
            .get_observer(&Attributes::from(&[("name", "read_buffer")]))
            .unwrap()
            .fetch();

        // Load is only called once
        assert_eq!(v, 1);
    }

    #[tokio::test]
    async fn test_rb_chunks_lru() {
        let (catalog, _partition) = make_catalog().await;

        let mut decoded_parquet_files = Vec::with_capacity(3);
        let ns = catalog.create_namespace("lru_ns").await;

        for i in 1..=3 {
            let table_name = format!("cached_table{i}");
            let table = ns.create_table(&table_name).await;
            let sequencer1 = ns.create_sequencer(1).await;

            let partition = table
                .with_sequencer(&sequencer1)
                .create_partition("k")
                .await;

            let test_parquet_file = partition
                .create_parquet_file(&format!("{table_name} foo=1 11"))
                .await;
            let parquet_file = test_parquet_file.parquet_file;
            decoded_parquet_files.push(Arc::new(DecodedParquetFile::new(parquet_file)));
        }

        let storage = ParquetStorage::new(Arc::clone(&catalog.object_store));

        // Create a ram pool big enough to hold 2 read buffer chunks
        let ram_pool = Arc::new(ResourcePool::new(
            "pool",
            RamSize(3600),
            catalog.time_provider(),
            Arc::clone(&catalog.metric_registry()),
        ));
        let cache = ReadBufferCache::new(
            BackoffConfig::default(),
            catalog.time_provider(),
            catalog.metric_registry(),
            ram_pool,
        );

        // load 1: Fetch table1 from storage
        cache
            .get(
                Arc::clone(&decoded_parquet_files[0]),
                "cached_table1".into(),
                storage.clone(),
            )
            .await;
        catalog.mock_time_provider().inc(Duration::from_millis(1));

        // load 2: Fetch table2 from storage
        cache
            .get(
                Arc::clone(&decoded_parquet_files[1]),
                "cached_table2".into(),
                storage.clone(),
            )
            .await;
        catalog.mock_time_provider().inc(Duration::from_millis(1));

        // Fetch table1 from cache, which should update its last used
        cache
            .get(
                Arc::clone(&decoded_parquet_files[0]),
                "cached_table1".into(),
                storage.clone(),
            )
            .await;
        catalog.mock_time_provider().inc(Duration::from_millis(1));

        // load 3: Fetch table3 from storage, which should evict table2
        cache
            .get(
                Arc::clone(&decoded_parquet_files[2]),
                "cached_table3".into(),
                storage.clone(),
            )
            .await;
        catalog.mock_time_provider().inc(Duration::from_millis(1));

        // load 4: Fetch table2, which will be from storage again, and will evict table1
        cache
            .get(
                Arc::clone(&decoded_parquet_files[1]),
                "cached_table2".into(),
                storage.clone(),
            )
            .await;
        catalog.mock_time_provider().inc(Duration::from_millis(1));

        // Fetch table2 from cache
        cache
            .get(
                Arc::clone(&decoded_parquet_files[1]),
                "cached_table2".into(),
                storage.clone(),
            )
            .await;
        catalog.mock_time_provider().inc(Duration::from_millis(1));

        // Fetch table3 from cache
        cache
            .get(
                Arc::clone(&decoded_parquet_files[2]),
                "cached_table3".into(),
                storage.clone(),
            )
            .await;

        let m: Metric<U64Counter> = catalog
            .metric_registry
            .get_instrument("cache_load_function_calls")
            .unwrap();
        let v = m
            .get_observer(&Attributes::from(&[("name", "read_buffer")]))
            .unwrap()
            .fetch();

        // Load is called 4x
        assert_eq!(v, 4);
    }

    fn lp_to_record_batch(lp: &str) -> RecordBatch {
        let (_table, batch) = lp_to_mutable_batch(lp);

        batch.to_arrow(Selection::All).unwrap()
    }

    #[tokio::test]
    async fn build_read_buffer_chunk_from_stream_of_record_batches() {
        let lines = ["cpu,host=a load=1 11", "cpu,host=a load=2 22"];
        let batches = lines
            .into_iter()
            .map(lp_to_record_batch)
            .map(Arc::new)
            .collect();

        let stream = stream_from_batches(batches);

        let metric_registry = metric::Registry::new();

        let rb = read_buffer_chunk_from_stream("cpu".into(), stream, &metric_registry)
            .await
            .unwrap();

        let rb_batches: Vec<RecordBatch> = rb
            .read_filter(Predicate::default(), Selection::All, vec![])
            .unwrap()
            .collect();

        let expected = [
            "+------+------+--------------------------------+",
            "| host | load | time                           |",
            "+------+------+--------------------------------+",
            "| a    | 1    | 1970-01-01T00:00:00.000000011Z |",
            "| a    | 2    | 1970-01-01T00:00:00.000000022Z |",
            "+------+------+--------------------------------+",
        ];

        assert_batches_eq!(expected, &rb_batches);
    }

    #[tokio::test]
    async fn test_rb_metrics() {
        let (catalog, partition) = make_catalog().await;

        let test_parquet_file = partition.create_parquet_file("table1 foo=1 11").await;
        let parquet_file = test_parquet_file.parquet_file;
        let decoded = Arc::new(DecodedParquetFile::new(parquet_file));
        let storage = ParquetStorage::new(Arc::clone(&catalog.object_store));

        let cache = make_cache(&catalog);

        let _rb = cache
            .get(Arc::clone(&decoded), "table1".into(), storage.clone())
            .await;

        let g: Metric<CumulativeGauge> = catalog
            .metric_registry
            .get_instrument("read_buffer_row_group_total")
            .unwrap();
        let v = g
            .get_observer(&Attributes::from(&[("db_name", "iox_shared")]))
            .unwrap()
            .fetch();

        // Load is only called once
        assert_eq!(v, 1);
    }
}
