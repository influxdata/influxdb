use std::sync::Arc;

use bytes::Bytes;
use data_types::partition_metadata::InfluxDbType;
use iox_catalog::{
    interface::{
        Catalog, KafkaPartition, KafkaTopic, Namespace, ParquetFile, Partition, QueryPool,
        SequenceNumber, Sequencer, Table, Timestamp,
    },
    mem::MemCatalog,
};
use iox_object_store::{IoxObjectStore, ParquetFilePath};
use object_store::ObjectStore;
use parquet_file::metadata::{IoxMetadata, IoxParquetMetaData};
use query::test::{raw_data, TestChunk};
use time::{MockProvider, Time, TimeProvider};
use uuid::Uuid;

pub struct TestCatalog {
    pub catalog: Arc<dyn Catalog>,
    pub metric_registry: Arc<metric::Registry>,
    pub object_store: Arc<ObjectStore>,
    pub time_provider: Arc<dyn TimeProvider>,
}

impl TestCatalog {
    pub fn new() -> Arc<Self> {
        let metric_registry = Arc::new(metric::Registry::new());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metric_registry)));
        let object_store = Arc::new(ObjectStore::new_in_memory());
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp(0, 0)));

        Arc::new(Self {
            metric_registry,
            catalog,
            object_store,
            time_provider,
        })
    }

    pub fn catalog(&self) -> Arc<dyn Catalog> {
        Arc::clone(&self.catalog)
    }

    pub fn metric_registry(&self) -> Arc<metric::Registry> {
        Arc::clone(&self.metric_registry)
    }

    pub fn object_store(&self) -> Arc<ObjectStore> {
        Arc::clone(&self.object_store)
    }

    pub fn time_provider(&self) -> Arc<dyn TimeProvider> {
        Arc::clone(&self.time_provider)
    }

    pub async fn create_namespace(self: &Arc<Self>, name: &str) -> Arc<TestNamespace> {
        let mut repos = self.catalog.repositories().await;

        let kafka_topic = repos
            .kafka_topics()
            .create_or_get("kafka_topic")
            .await
            .unwrap();
        let query_pool = repos.query_pools().create_or_get("pool").await.unwrap();
        let namespace = repos
            .namespaces()
            .create(name, "1y", kafka_topic.id, query_pool.id)
            .await
            .unwrap();

        Arc::new(TestNamespace {
            catalog: Arc::clone(self),
            kafka_topic,
            query_pool,
            namespace,
        })
    }
}

pub struct TestNamespace {
    pub catalog: Arc<TestCatalog>,
    pub kafka_topic: KafkaTopic,
    pub query_pool: QueryPool,
    pub namespace: Namespace,
}

impl TestNamespace {
    pub async fn create_table(self: &Arc<Self>, name: &str) -> Arc<TestTable> {
        let mut repos = self.catalog.catalog.repositories().await;

        let table = repos
            .tables()
            .create_or_get(name, self.namespace.id)
            .await
            .unwrap();

        Arc::new(TestTable {
            catalog: Arc::clone(&self.catalog),
            namespace: Arc::clone(self),
            table,
        })
    }
}

pub struct TestTable {
    pub catalog: Arc<TestCatalog>,
    pub namespace: Arc<TestNamespace>,
    pub table: Table,
}

impl TestTable {
    pub async fn create_partition(
        self: &Arc<Self>,
        key: &str,
        sequencer: i32,
    ) -> Arc<TestPartition> {
        let mut repos = self.catalog.catalog.repositories().await;

        let sequencer = repos
            .sequencers()
            .create_or_get(&self.namespace.kafka_topic, KafkaPartition::new(sequencer))
            .await
            .unwrap();
        let partition = repos
            .partitions()
            .create_or_get(key, sequencer.id, self.table.id)
            .await
            .unwrap();

        Arc::new(TestPartition {
            catalog: Arc::clone(&self.catalog),
            namespace: Arc::clone(&self.namespace),
            table: Arc::clone(self),
            sequencer,
            partition,
        })
    }
}

pub struct TestPartition {
    pub catalog: Arc<TestCatalog>,
    pub namespace: Arc<TestNamespace>,
    pub table: Arc<TestTable>,
    pub sequencer: Sequencer,
    pub partition: Partition,
}

impl TestPartition {
    pub async fn create_parquet_file(self: &Arc<Self>) -> Arc<TestParquetFile> {
        let mut repos = self.catalog.catalog.repositories().await;

        let object_store_id = Uuid::nil();
        let min_sequence_number = SequenceNumber::new(1);
        let max_sequence_number = SequenceNumber::new(100);
        let row_count_expected = 3;
        let metadata = IoxMetadata {
            object_store_id,
            creation_timestamp: now(),
            namespace_id: self.namespace.namespace.id,
            namespace_name: self.namespace.namespace.name.clone().into(),
            sequencer_id: self.sequencer.id,
            table_id: self.table.table.id,
            table_name: self.table.table.name.clone().into(),
            partition_id: self.partition.id,
            partition_key: self.partition.partition_key.clone().into(),
            time_of_first_write: now(),
            time_of_last_write: now(),
            min_sequence_number,
            max_sequence_number,
            row_count: row_count_expected,
        };
        let (parquet_metadata_bin, file_size_bytes) =
            create_parquet_file(&self.catalog.object_store, &metadata).await;

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
                self.sequencer.id,
                self.table.table.id,
                self.partition.id,
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

        Arc::new(TestParquetFile { parquet_file })
    }
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

pub struct TestParquetFile {
    pub parquet_file: ParquetFile,
}

fn now() -> Time {
    Time::from_timestamp(0, 0)
}
