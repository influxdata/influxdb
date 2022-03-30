use crate::chunk::{ChunkMetrics, ParquetChunk};
use crate::metadata::IoxMetadataOld;
use crate::storage::Storage;
use crate::test_utils::{
    create_partition_and_database_checkpoint, make_iox_object_store, make_record_batch, TestSize,
};
use data_types::chunk_metadata::{ChunkAddr, ChunkId, ChunkOrder};
use data_types::partition_metadata::{PartitionAddr, TableSummary};
use datafusion_util::MemoryStream;
use iox_object_store::IoxObjectStore;
use std::sync::Arc;
use time::Time;

/// Controls the number of row groups to generate for chunks
#[derive(Debug, Copy, Clone)]
pub enum GeneratorConfig {
    /// Generates schema but skips generating data
    NoData,
    /// Generates 3 row groups with a limited selection of columns
    Simple,
    /// Generates 3 row groups with a wide variety of different columns
    Full,
}

/// A generator of persisted chunks for use in tests
#[derive(Debug)]
pub struct ChunkGenerator {
    iox_object_store: Arc<IoxObjectStore>,
    storage: Storage,
    column_prefix: String,
    config: GeneratorConfig,
    partition: PartitionAddr,
    next_chunk: u32,
}

impl ChunkGenerator {
    pub async fn new() -> Self {
        Self::new_with_store(make_iox_object_store().await)
    }

    pub fn new_with_store(iox_object_store: Arc<IoxObjectStore>) -> Self {
        let storage = Storage::new(Arc::clone(&iox_object_store));
        Self {
            iox_object_store,
            storage,
            column_prefix: "foo".to_string(),
            config: GeneratorConfig::Full,
            partition: PartitionAddr {
                db_name: Arc::from("db1"),
                table_name: Arc::from("table1"),
                partition_key: Arc::from("part1"),
            },
            next_chunk: 1,
        }
    }

    pub fn store(&self) -> &Arc<IoxObjectStore> {
        &self.iox_object_store
    }

    pub fn set_config(&mut self, config: GeneratorConfig) {
        self.config = config;
    }

    pub fn partition(&self) -> &PartitionAddr {
        &self.partition
    }

    pub async fn generate(&mut self) -> Option<(ParquetChunk, IoxMetadataOld)> {
        let id = self.next_chunk;
        self.next_chunk += 1;
        self.generate_id(id).await
    }

    pub async fn generate_id(&mut self, id: u32) -> Option<(ParquetChunk, IoxMetadataOld)> {
        let (partition_checkpoint, database_checkpoint) = create_partition_and_database_checkpoint(
            Arc::clone(&self.partition.table_name),
            Arc::clone(&self.partition.partition_key),
        );

        let chunk_id = ChunkId::new_test(id as _);
        let chunk_order = ChunkOrder::new(id).unwrap();
        let chunk_addr = ChunkAddr::new(&self.partition, chunk_id);

        let metadata = IoxMetadataOld {
            creation_timestamp: Time::from_timestamp(10, 20),
            table_name: Arc::clone(&self.partition.table_name),
            partition_key: Arc::clone(&self.partition.partition_key),
            chunk_id,
            chunk_order,
            partition_checkpoint,
            database_checkpoint,
            time_of_first_write: Time::from_timestamp(30, 40),
            time_of_last_write: Time::from_timestamp(50, 60),
            sort_key: None,
        };

        let (record_batches, schema, column_summaries, rows) = match self.config {
            GeneratorConfig::NoData => {
                // Generating an entire row group just for its metadata seems wasteful
                let (_, schema, column_summaries, _) =
                    make_record_batch(&self.column_prefix, TestSize::Minimal);
                // Note: column summaries here are inconsistent with the actual data?
                (vec![], schema, column_summaries, 0)
            }
            GeneratorConfig::Simple => make_record_batch(&self.column_prefix, TestSize::Minimal),
            GeneratorConfig::Full => make_record_batch(&self.column_prefix, TestSize::Full),
        };

        // ensure we make multiple row groups if we have more than one
        // record batch
        if let Some(batch) = record_batches.get(0) {
            self.storage.set_max_row_group_size(batch.num_rows());
        }

        let table_summary = TableSummary {
            columns: column_summaries,
        };

        let stream = Box::pin(MemoryStream::new_with_schema(
            record_batches,
            Arc::clone(schema.inner()),
        ));

        let written_result = self
            .storage
            .write_to_object_store(chunk_addr, stream, metadata.clone())
            .await
            .unwrap();

        written_result.as_ref()?;
        let (path, file_size_bytes, parquet_metadata) = written_result.unwrap();

        let chunk = ParquetChunk::new_from_parts(
            Arc::new(table_summary),
            Arc::new(schema),
            &path,
            Arc::clone(&self.iox_object_store),
            file_size_bytes,
            Arc::new(parquet_metadata),
            rows,
            ChunkMetrics::new_unregistered(),
        );

        Some((chunk, metadata))
    }
}
