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
use data_types::{ParquetFile, ParquetFileId};
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;
use iox_time::TimeProvider;
use parquet_file::chunk::DecodedParquetFile;
use read_buffer::RBChunk;
use snafu::{ResultExt, Snafu};
use std::{collections::HashMap, mem, sync::Arc};

const CACHE_ID: &str = "read_buffer";

/// Cache for parquet file data decoded into read buffer chunks
#[derive(Debug)]
pub struct ReadBufferCache {
    cache: Cache<ParquetFileId, Arc<RBChunk>, ()>,

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
            move |parquet_file_id: ParquetFileId, _extra| {
                let backoff_config = BackoffConfig::default();

                async move {
                    let rb_chunk = Backoff::new(&backoff_config)
                        .retry_all_errors("get read buffer chunk by parquet file ID", || async {
                            let parquet_file = parquet_file_by_id(parquet_file_id);
                            let table_name = parquet_file_table_name(&parquet_file).to_string();
                            let record_batch_stream = record_batches_stream(&parquet_file);
                            read_buffer_chunk_from_stream(table_name, record_batch_stream).await
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
    pub async fn get(&self, decoded_parquet_file: &DecodedParquetFile) -> Arc<RBChunk> {
        let parquet_file = &decoded_parquet_file.parquet_file;

        self.cache.get(parquet_file.id, ()).await
    }
}

fn parquet_file_by_id(_parquet_file_id: ParquetFileId) -> ParquetFile {
    unimplemented!()
}

fn parquet_file_table_name(_parquet_file: &ParquetFile) -> &str {
    unimplemented!()
}

fn record_batches_stream(_parquet_file: &ParquetFile) -> SendableRecordBatchStream {
    unimplemented!()
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
    table_name: String,
    mut stream: SendableRecordBatchStream,
) -> Result<RBChunk, RBChunkError> {
    let schema = stream.schema();

    let mut builder = read_buffer::RBChunkBuilder::new(table_name, schema);

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
    use iox_tests::util::TestCatalog;
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

    async fn make_catalog() -> Arc<TestCatalog> {
        TestCatalog::new()
    }

    #[tokio::test]
    async fn test_rb_chunks() {
        let catalog = make_catalog().await;
        let _cache = make_cache(&catalog);
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

        let rb = read_buffer_chunk_from_stream("cpu".to_string(), stream)
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
