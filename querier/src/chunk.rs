use std::{collections::HashMap, sync::Arc};

use backoff::{Backoff, BackoffConfig};
use data_types::chunk_metadata::{ChunkAddr, ChunkOrder};
use db::catalog::chunk::{CatalogChunk, ChunkMetadata, ChunkMetrics as CatalogChunkMetrics};
use iox_catalog::interface::{
    Catalog, NamespaceId, ParquetFile, PartitionId, SequencerId, TableId,
};
use iox_object_store::{IoxObjectStore, ParquetFilePath};
use object_store::ObjectStore;
use parking_lot::RwLock;
use parquet_file::{
    chunk::{ChunkMetrics as ParquetChunkMetrics, ParquetChunk},
    metadata::{DecodedIoxParquetMetaData, IoxMetadata, IoxParquetMetaData},
};
use time::TimeProvider;
use uuid::Uuid;

/// Parquet file with decoded metadata.
struct DecodedParquetFile {
    parquet_file: ParquetFile,
    parquet_metadata: Arc<IoxParquetMetaData>,
    decoded_metadata: DecodedIoxParquetMetaData,
    iox_metadata: IoxMetadata,
}

impl DecodedParquetFile {
    fn new(parquet_file: ParquetFile) -> Self {
        let parquet_metadata = Arc::new(IoxParquetMetaData::from_thrift_bytes(
            parquet_file.parquet_metadata.clone(),
        ));
        let decoded_metadata = parquet_metadata.decode().expect("parquet metadata broken");
        let iox_metadata = decoded_metadata
            .read_iox_metadata_new()
            .expect("cannot read IOx metadata from parquet MD");

        Self {
            parquet_file,
            parquet_metadata,
            decoded_metadata,
            iox_metadata,
        }
    }
}

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

    /// Time provider.
    time_provider: Arc<dyn TimeProvider>,

    /// Partition keys cache for old gen.
    old_gen_partition_key_cache: RwLock<HashMap<(SequencerId, PartitionId), Arc<str>>>,

    /// Partition key cache.
    partition_cache: RwLock<HashMap<PartitionId, Arc<str>>>,

    /// Table name and namespace cache.
    table_cache: RwLock<HashMap<TableId, (Arc<str>, NamespaceId)>>,

    /// Namespace name cache.
    namespace_cache: RwLock<HashMap<NamespaceId, Arc<str>>>,
}

impl ParquetChunkAdapter {
    /// Create new adapter with empty cache.
    pub fn new(
        catalog: Arc<dyn Catalog>,
        object_store: Arc<ObjectStore>,
        metric_registry: Arc<metric::Registry>,
        time_provider: Arc<dyn TimeProvider>,
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
            time_provider,
            old_gen_partition_key_cache: RwLock::new(HashMap::default()),
            partition_cache: RwLock::new(HashMap::default()),
            table_cache: RwLock::new(HashMap::default()),
            namespace_cache: RwLock::new(HashMap::default()),
        }
    }

    /// Create parquet chunk.
    async fn new_parquet_chunk(&self, decoded_parquet_file: &DecodedParquetFile) -> ParquetChunk {
        let iox_metadata = &decoded_parquet_file.iox_metadata;
        let path = ParquetFilePath::new_new_gen(
            iox_metadata.namespace_id,
            iox_metadata.table_id,
            iox_metadata.sequencer_id,
            iox_metadata.partition_id,
            iox_metadata.object_store_id,
        );

        let parquet_file = &decoded_parquet_file.parquet_file;
        let file_size_bytes = parquet_file.file_size_bytes as usize;
        let table_name = self.table_name(parquet_file.table_id).await;
        let partition_key = self.partition_key(parquet_file.partition_id).await;
        let metrics = ParquetChunkMetrics::new(self.metric_registry.as_ref());

        ParquetChunk::new(
            &path,
            Arc::clone(&self.iox_object_store),
            file_size_bytes,
            Arc::clone(&decoded_parquet_file.parquet_metadata),
            table_name,
            partition_key,
            metrics,
        )
        .expect("cannot create chunk")
    }

    /// Create a catalog chunk.
    pub async fn new_catalog_chunk(&self, parquet_file: ParquetFile) -> CatalogChunk {
        let decoded_parquet_file = DecodedParquetFile::new(parquet_file);
        let chunk = Arc::new(self.new_parquet_chunk(&decoded_parquet_file).await);

        let addr = self
            .old_gen_chunk_addr(&decoded_parquet_file.parquet_file)
            .await;

        // TODO: register metrics w/ catalog registry
        let metrics = CatalogChunkMetrics::new_unregistered();

        let iox_metadata = &decoded_parquet_file.iox_metadata;

        // Somewhat hacky workaround because NG has implicit chunk orders, use min sequence number and hope it doesn't
        // overflow u32. Order is non-zero, se we need to add 1.
        let order = ChunkOrder::new(1 + iox_metadata.min_sequence_number.get() as u32)
            .expect("cannot be zero");

        let metadata = ChunkMetadata {
            table_summary: Arc::clone(chunk.table_summary()),
            schema: chunk.schema(),
            // delete predicates will be set/synced by a dedicated process
            delete_predicates: vec![],
            time_of_first_write: iox_metadata.time_of_first_write,
            time_of_last_write: iox_metadata.time_of_last_write,
            // TODO(marco): get sort key wired up (needs to come via IoxMetadata)
            sort_key: None,
        };

        CatalogChunk::new_object_store_only(
            addr,
            order,
            metadata,
            chunk,
            metrics,
            Arc::clone(&self.time_provider),
        )
    }

    /// Get chunk addr for old gen.
    ///
    /// Mapping of NG->old:
    /// - `namespace.name -> db_name`
    /// - `table.name -> table_name`
    /// - `sequencer.id X partition.name -> partition_key`
    /// - `parquet_file.id -> chunk_id`
    async fn old_gen_chunk_addr(&self, parquet_file: &ParquetFile) -> ChunkAddr {
        ChunkAddr {
            db_name: self
                .namespace_name(self.table_namespace_id(parquet_file.table_id).await)
                .await,
            table_name: self.table_name(parquet_file.table_id).await,
            partition_key: self
                .old_gen_partition_key(parquet_file.sequencer_id, parquet_file.partition_id)
                .await,
            chunk_id: data_types::chunk_metadata::ChunkId::from(Uuid::from_u128(
                parquet_file.id.get() as _,
            )),
        }
    }

    /// Get partition key for old gen.
    ///
    /// This either uses a cached value or -- if required -- creates a fresh string.
    async fn old_gen_partition_key(
        &self,
        sequencer_id: SequencerId,
        partition_id: PartitionId,
    ) -> Arc<str> {
        if let Some(key) = self
            .old_gen_partition_key_cache
            .read()
            .get(&(sequencer_id, partition_id))
        {
            return Arc::clone(key);
        }

        let partition_key = self.partition_key(partition_id).await;
        let og_partition_key = Arc::from(format!("{}-{}", sequencer_id.get(), partition_key));

        Arc::clone(
            self.old_gen_partition_key_cache
                .write()
                .entry((sequencer_id, partition_id))
                .or_insert(og_partition_key),
        )
    }

    /// Get the partition key for the given partition ID.
    ///
    /// This either uses a cached value or -- if required -- fetches the mapping from the catalog.
    async fn partition_key(&self, partition_id: PartitionId) -> Arc<str> {
        if let Some(key) = self.partition_cache.read().get(&partition_id) {
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

        Arc::clone(
            self.partition_cache
                .write()
                .entry(partition_id)
                .or_insert(key),
        )
    }

    /// Get the table name and namespace ID for the given table ID.
    ///
    /// This either uses a cached value or -- if required -- fetches the mapping from the catalog.
    async fn cached_table(&self, table_id: TableId) -> (Arc<str>, NamespaceId) {
        if let Some((name, ns)) = self.table_cache.read().get(&table_id) {
            return (Arc::clone(name), *ns);
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

        let mut table_cache = self.table_cache.write();
        let (name, ns) = table_cache
            .entry(table_id)
            .or_insert((name, table.namespace_id));
        (Arc::clone(name), *ns)
    }

    /// Get the table name for the given table ID.
    ///
    /// This either uses a cached value or -- if required -- fetches the mapping from the catalog.
    async fn table_name(&self, table_id: TableId) -> Arc<str> {
        self.cached_table(table_id).await.0
    }

    /// Get the table namespace ID for the given table ID.
    ///
    /// This either uses a cached value or -- if required -- fetches the mapping from the catalog.
    async fn table_namespace_id(&self, table_id: TableId) -> NamespaceId {
        self.cached_table(table_id).await.1
    }

    /// Get the namespace name for the given namespace ID.
    ///
    /// This either uses a cached value or -- if required -- fetches the mapping from the catalog.
    async fn namespace_name(&self, namespace_id: NamespaceId) -> Arc<str> {
        if let Some(name) = self.namespace_cache.read().get(&namespace_id) {
            return Arc::clone(name);
        }

        let namespace = Backoff::new(&self.backoff_config)
            .retry_all_errors("get namespace_name", || async {
                self.catalog
                    .repositories()
                    .await
                    .namespaces()
                    .get_by_id(namespace_id)
                    .await
            })
            .await
            .expect("retry forever")
            .expect("namespace gone from catalog?!");

        let name = Arc::from(namespace.name);

        Arc::clone(
            self.namespace_cache
                .write()
                .entry(namespace_id)
                .or_insert(name),
        )
    }
}

#[cfg(test)]
mod tests {
    use arrow::record_batch::RecordBatch;
    use arrow_util::assert_batches_eq;
    use bytes::Bytes;
    use data_types::partition_metadata::InfluxDbType;
    use db::chunk::DbChunk;
    use futures::StreamExt;
    use iox_catalog::{
        interface::{KafkaPartition, SequenceNumber, Timestamp},
        mem::MemCatalog,
    };
    use parquet_file::metadata::IoxMetadata;
    use query::{
        test::{raw_data, TestChunk},
        QueryChunk,
    };
    use schema::selection::Selection;
    use time::{MockProvider, Time};
    use uuid::Uuid;

    use super::*;

    #[tokio::test]
    async fn test_create_record() {
        let metric_registry = Arc::new(metric::Registry::new());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metric_registry)));
        let object_store = Arc::new(ObjectStore::new_in_memory());
        let time_provider = Arc::new(MockProvider::new(now()));

        let adapter = ParquetChunkAdapter::new(
            Arc::clone(&catalog),
            Arc::clone(&object_store),
            metric_registry,
            time_provider,
        );

        let parquet_file = parquet_file(catalog.as_ref(), &object_store).await;

        let catalog_chunk = adapter.new_catalog_chunk(parquet_file).await;
        assert_eq!(
            catalog_chunk.addr().to_string(),
            "Chunk('ns':'table':'1-part':00000000-0000-0000-0000-000000000001)",
        );

        let db_chunk = DbChunk::snapshot(&catalog_chunk);
        let batches = collect_read_filter(&db_chunk).await;
        assert_batches_eq!(
            &[
                "+-----------+------+-----------------------------+",
                "| field_int | tag1 | time                        |",
                "+-----------+------+-----------------------------+",
                "| 1000      | WA   | 1970-01-01T00:00:00.000008Z |",
                "| 10        | VT   | 1970-01-01T00:00:00.000010Z |",
                "| 70        | UT   | 1970-01-01T00:00:00.000020Z |",
                "+-----------+------+-----------------------------+",
            ],
            &batches
        );
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
        let row_count_expected = 3;
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
            row_count: row_count_expected,
        };
        let (parquet_metadata_bin, file_size_bytes) =
            create_parquet_file(object_store, &metadata).await;

        // decode metadata because we need to store them within the catalog
        let parquet_metadata = Arc::new(IoxParquetMetaData::from_thrift_bytes(
            parquet_metadata_bin.clone(),
        ));
        let decoded_metadata = parquet_metadata.decode().unwrap();
        let schema = decoded_metadata.read_schema().unwrap();
        let stats = decoded_metadata.read_statistics(&schema).unwrap();
        let row_count = stats[0].total_count();
        assert_eq!(row_count as i64, row_count_expected);
        let ts_min_max = stats
            .iter()
            .find_map(|stat| {
                (stat.influxdb_type == Some(InfluxDbType::Timestamp))
                    .then(|| stat.stats.timestamp_min_max().unwrap())
            })
            .unwrap();

        let parquet_file = repos
            .parquet_files()
            .create(
                sequencer.id,
                table.id,
                partition.id,
                object_store_id,
                min_sequence_number,
                max_sequence_number,
                Timestamp::new(ts_min_max.min),
                Timestamp::new(ts_min_max.max),
                file_size_bytes as i64,
                parquet_metadata_bin,
                row_count as i64,
            )
            .await
            .unwrap();

        parquet_file
    }

    /// Create parquet file and return thrift-encoded and zstd-compressed parquet metadata as well as the file size.
    async fn create_parquet_file(
        object_store: &Arc<ObjectStore>,
        metadata: &IoxMetadata,
    ) -> (Vec<u8>, usize) {
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
        let parquet_md = md.thrift_bytes().to_vec();
        let data = Arc::try_unwrap(data).expect("dangling reference to data");

        let file_size = data.len();
        let bytes = Bytes::from(data);

        let path = ParquetFilePath::new_new_gen(
            metadata.namespace_id,
            metadata.table_id,
            metadata.sequencer_id,
            metadata.partition_id,
            metadata.object_store_id,
        );

        iox_object_store
            .put_parquet_file(&path, bytes)
            .await
            .unwrap();

        (parquet_md, file_size)
    }

    async fn collect_read_filter(chunk: &DbChunk) -> Vec<RecordBatch> {
        chunk
            .read_filter(&Default::default(), Selection::All)
            .unwrap()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(Result::unwrap)
            .collect()
    }

    fn now() -> Time {
        Time::from_timestamp(0, 0)
    }
}
