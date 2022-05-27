//! Cache Parquet file data in Read Buffer chunks.

use super::ram::RamSize;
use backoff::{Backoff, BackoffConfig};
use cache_system::{
    backend::{
        lru::{LruBackend, ResourcePool},
        resource_consumption::FunctionEstimator,
        shared::SharedBackend,
    },
    driver::Cache,
    loader::{metrics::MetricsLoader, FunctionLoader},
};
use data_types::ParquetFileId;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;
use iox_time::TimeProvider;
use parquet_file::{chunk::DecodedParquetFile, storage::ParquetStorage};
use read_buffer::RBChunk;
use snafu::{ResultExt, Snafu};
use std::{collections::HashMap, mem, sync::Arc};

const CACHE_ID: &str = "read_buffer";

type ExtraFetchInfo = (Arc<DecodedParquetFile>, Arc<str>, ParquetStorage);

/// Cache for parquet file data decoded into read buffer chunks
#[derive(Debug)]
pub struct ReadBufferCache {
    cache: Cache<ParquetFileId, Arc<RBChunk>, ExtraFetchInfo>,

    /// Handle that allows clearing entries for existing cache entries
    _backend: SharedBackend<ParquetFileId, Arc<RBChunk>>,
}

impl ReadBufferCache {
    /// Create a new empty cache.
    pub fn new(
        time_provider: Arc<dyn TimeProvider>,
        metric_registry: &metric::Registry,
        ram_pool: Arc<ResourcePool<RamSize>>,
    ) -> Self {
        let loader = Box::new(FunctionLoader::new(
            move |_parquet_file_id, (decoded_parquet_file, table_name, store): ExtraFetchInfo| {
                let backoff_config = BackoffConfig::default();

                async move {
                    let rb_chunk = Backoff::new(&backoff_config)
                        .retry_all_errors("get read buffer chunk from parquet file", || {
                            let decoded_parquet_file_for_load = Arc::clone(&decoded_parquet_file);
                            let table_name_for_load = Arc::clone(&table_name);
                            let store_for_load = store.clone();

                            async {
                                load_from_parquet_file(
                                    decoded_parquet_file_for_load,
                                    table_name_for_load,
                                    store_for_load,
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
            metric_registry,
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

        let cache = Cache::new(loader, Box::new(_backend.clone()));

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
                (decoded_parquet_file, table_name, store),
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
) -> Result<RBChunk, LoadError> {
    let record_batch_stream =
        record_batches_stream(decoded_parquet_file, store).context(ReadingFromStorageSnafu)?;
    read_buffer_chunk_from_stream(table_name, record_batch_stream)
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
) -> Result<RBChunk, RBChunkError> {
    let schema = stream.schema();

    let mut builder = read_buffer::RBChunkBuilder::new(table_name.as_ref(), schema);

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
    use metric::{Attributes, Metric, U64Counter};
    use mutable_batch_lp::test_helpers::lp_to_mutable_batch;
    use read_buffer::Predicate;
    use schema::selection::Selection;

    fn make_cache(catalog: &TestCatalog) -> ReadBufferCache {
        ReadBufferCache::new(
            catalog.time_provider(),
            &catalog.metric_registry(),
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

        let rb = read_buffer_chunk_from_stream("cpu".into(), stream)
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
}
