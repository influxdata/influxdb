use std::{collections::HashMap, sync::Arc};

use backoff::{Backoff, BackoffConfig};
use iox_catalog::interface::{Catalog, ParquetFile, PartitionId, TableId};
use iox_object_store::{IoxObjectStore, ParquetFilePath};
use object_store::ObjectStore;
use parking_lot::RwLock;
use parquet_file::{
    chunk::{ChunkMetrics, ParquetChunk},
    metadata::IoxParquetMetaData,
};

/// Adapter that can create old-gen chunks for the new-gen catalog.
#[derive(Debug)]
pub struct ParquetChunkAdapter {
    /// Backoff config for IO operations.
    backoff_config: BackoffConfig,

    /// Catalog.
    catalog: Arc<dyn Catalog>,

    /// Old-gen object store.
    iox_object_store: Arc<IoxObjectStore>,

    /// Metric registry.
    metric_registry: Arc<metric::Registry>,

    /// Partition key cache.
    partition_keys: RwLock<HashMap<PartitionId, Arc<str>>>,

    /// Table name cache.
    table_names: RwLock<HashMap<TableId, Arc<str>>>,
}

impl ParquetChunkAdapter {
    /// Create new adapter with empty cache.
    pub fn new(
        catalog: Arc<dyn Catalog>,
        object_store: Arc<ObjectStore>,
        metric_registry: Arc<metric::Registry>,
    ) -> Self {
        // create a virtual IOx object store, the UUID won't be used anyways
        let iox_object_store = Arc::new(IoxObjectStore::existing(
            Arc::clone(&object_store),
            IoxObjectStore::root_path_for(&object_store, uuid::Uuid::new_v4()),
        ));

        Self {
            backoff_config: BackoffConfig::default(),
            catalog,
            iox_object_store,
            metric_registry,
            partition_keys: RwLock::new(HashMap::default()),
            table_names: RwLock::new(HashMap::default()),
        }
    }

    /// Create parquet chunk.
    pub async fn new_parquet_chunk(&self, parquet_file: ParquetFile) -> ParquetChunk {
        let parquet_metadata = Arc::new(IoxParquetMetaData::from_thrift_bytes(
            parquet_file.parquet_metadata.clone(),
        ));
        let iox_metadata = parquet_metadata
            .decode()
            .expect("parquet metadata broken")
            .read_iox_metadata_new()
            .expect("cannot read IOx metadata from parquet MD");
        let path = ParquetFilePath::new_new_gen(
            iox_metadata.namespace_id,
            iox_metadata.table_id,
            iox_metadata.sequencer_id,
            iox_metadata.partition_id,
            iox_metadata.object_store_id,
        );
        let file_size_bytes = parquet_file.file_size_bytes as usize;
        let table_name = self.table_name(parquet_file.table_id).await;
        let partition_key = self.partition_key(parquet_file.partition_id).await;
        let metrics = ChunkMetrics::new(self.metric_registry.as_ref());

        ParquetChunk::new(
            &path,
            Arc::clone(&self.iox_object_store),
            file_size_bytes,
            parquet_metadata,
            table_name,
            partition_key,
            metrics,
        )
        .expect("cannot create chunk")
    }

    /// Get the partition key for the given partition ID.
    ///
    /// This either uses a cached value or -- if required -- fetches the mapping from the catalog.
    async fn partition_key(&self, partition_id: PartitionId) -> Arc<str> {
        if let Some(key) = self.partition_keys.read().get(&partition_id) {
            return Arc::clone(key);
        }

        let partition = Backoff::new(&self.backoff_config)
            .retry_all_errors("get partition_key", || async {
                self.catalog
                    .repositories()
                    .await
                    .partitions()
                    .get_by_id(partition_id)
                    .await
            })
            .await
            .expect("retry forever")
            .expect("partition gone from catalog?!");

        let key = Arc::from(partition.partition_key);

        self.partition_keys
            .write()
            .insert(partition_id, Arc::clone(&key));

        key
    }

    /// Get the table name for the given table ID.
    ///
    /// This either uses a cached value or -- if required -- fetches the mapping from the catalog.
    async fn table_name(&self, table_id: TableId) -> Arc<str> {
        if let Some(name) = self.table_names.read().get(&table_id) {
            return Arc::clone(name);
        }

        let table = Backoff::new(&self.backoff_config)
            .retry_all_errors("get table_name", || async {
                self.catalog
                    .repositories()
                    .await
                    .tables()
                    .get_by_id(table_id)
                    .await
            })
            .await
            .expect("retry forever")
            .expect("table gone from catalog?!");

        let name = Arc::from(table.name);

        self.table_names.write().insert(table_id, Arc::clone(&name));

        name
    }
}

#[cfg(test)]
mod tests {
    use iox_catalog::{
        interface::{KafkaPartition, SequenceNumber, Timestamp},
        mem::MemCatalog,
    };
    use parquet_file::metadata::IoxMetadata;
    use query::test::{raw_data, TestChunk};
    use time::Time;
    use uuid::Uuid;

    use super::*;

    #[tokio::test]
    async fn test() {
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new());
        let object_store = Arc::new(ObjectStore::new_in_memory());
        let metric_registry = Arc::new(metric::Registry::new());

        let adapter = ParquetChunkAdapter::new(
            Arc::clone(&catalog),
            Arc::clone(&object_store),
            metric_registry,
        );
        let parquet_file = parquet_file(catalog.as_ref(), &object_store).await;
        let _chunk = adapter.new_parquet_chunk(parquet_file).await;
    }

    async fn parquet_file(catalog: &dyn Catalog, object_store: &Arc<ObjectStore>) -> ParquetFile {
        let mut repos = catalog.repositories().await;
        let kafka_topic = repos
            .kafka_topics()
            .create_or_get("kafka_topic")
            .await
            .unwrap();
        let sequencer = repos
            .sequencers()
            .create_or_get(&kafka_topic, KafkaPartition::new(1))
            .await
            .unwrap();
        let query_pool = repos.query_pools().create_or_get("pool").await.unwrap();
        let namespace = repos
            .namespaces()
            .create("ns", "1y", kafka_topic.id, query_pool.id)
            .await
            .unwrap();
        let table = repos
            .tables()
            .create_or_get("table", namespace.id)
            .await
            .unwrap();
        let partition = repos
            .partitions()
            .create_or_get("part", sequencer.id, table.id)
            .await
            .unwrap();
        let object_store_id = Uuid::nil();
        let min_sequence_number = SequenceNumber::new(1);
        let max_sequence_number = SequenceNumber::new(100);
        let min_time = Timestamp::new(1000);
        let max_time = Timestamp::new(2000);
        let file_size_bytes = 1337;
        let row_count = 3;
        let metadata = IoxMetadata {
            object_store_id,
            creation_timestamp: now(),
            namespace_id: namespace.id,
            namespace_name: namespace.name.into(),
            sequencer_id: sequencer.id,
            table_id: table.id,
            table_name: table.name.into(),
            partition_id: partition.id,
            partition_key: partition.partition_key.into(),
            time_of_first_write: now(),
            time_of_last_write: now(),
            min_sequence_number,
            max_sequence_number,
            row_count,
        };
        let parquet_metadata = parquet_metadata(object_store, &metadata).await;
        let parquet_file = repos
            .parquet_files()
            .create(
                sequencer.id,
                table.id,
                partition.id,
                object_store_id,
                min_sequence_number,
                max_sequence_number,
                min_time,
                max_time,
                file_size_bytes,
                parquet_metadata,
                row_count,
            )
            .await
            .unwrap();

        parquet_file
    }

    async fn parquet_metadata(object_store: &Arc<ObjectStore>, metadata: &IoxMetadata) -> Vec<u8> {
        let iox_object_store = Arc::new(IoxObjectStore::existing(
            Arc::clone(object_store),
            IoxObjectStore::root_path_for(object_store, uuid::Uuid::new_v4()),
        ));

        let chunk1 = Arc::new(
            TestChunk::new("t")
                .with_id(1)
                .with_time_column()
                .with_tag_column("tag1")
                .with_i64_field_column("field_int")
                .with_three_rows_of_data(),
        );
        let record_batches = raw_data(&[chunk1]).await;
        let schema = record_batches.first().unwrap().schema();

        let data = parquet_file::storage::Storage::new(Arc::clone(&iox_object_store))
            .parquet_bytes(record_batches, schema, metadata)
            .await
            .unwrap();
        let data = Arc::new(data);
        let md = IoxParquetMetaData::from_file_bytes(Arc::clone(&data))
            .unwrap()
            .unwrap();
        md.thrift_bytes().to_vec()
    }

    fn now() -> Time {
        Time::from_timestamp(0, 0)
    }
}
