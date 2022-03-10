//! Utils of the tests

use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use data_types2::{
    ColumnType, InfluxDbType, KafkaPartition, KafkaTopic, Namespace, ParquetFile,
    ParquetFileParams, Partition, QueryPool, SequenceNumber, Sequencer, Table, Timestamp,
    Tombstone,
};
use iox_catalog::{interface::Catalog, mem::MemCatalog};
use iox_object_store::{IoxObjectStore, ParquetFilePath};
use mutable_batch_lp::test_helpers::lp_to_mutable_batch;
use object_store::ObjectStore;
use parquet_file::metadata::{IoxMetadata, IoxParquetMetaData};
use query::exec::Executor;
use schema::selection::Selection;
use std::sync::Arc;
use time::{MockProvider, Time, TimeProvider};
use uuid::Uuid;

/// Catalog for tests
#[allow(missing_docs)]
pub struct TestCatalog {
    pub catalog: Arc<dyn Catalog>,
    pub metric_registry: Arc<metric::Registry>,
    pub object_store: Arc<ObjectStore>,
    pub time_provider: Arc<dyn TimeProvider>,
    pub exec: Arc<Executor>,
}

impl TestCatalog {
    /// Initialize the catalog
    pub fn new() -> Arc<Self> {
        let metric_registry = Arc::new(metric::Registry::new());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metric_registry)));
        let object_store = Arc::new(ObjectStore::new_in_memory());
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp(0, 0)));
        let exec = Arc::new(Executor::new(1));

        Arc::new(Self {
            metric_registry,
            catalog,
            object_store,
            time_provider,
            exec,
        })
    }

    /// Return the catalog
    pub fn catalog(&self) -> Arc<dyn Catalog> {
        Arc::clone(&self.catalog)
    }

    /// Return the catalog's metric registry
    pub fn metric_registry(&self) -> Arc<metric::Registry> {
        Arc::clone(&self.metric_registry)
    }

    /// Return the catalog's  object store
    pub fn object_store(&self) -> Arc<ObjectStore> {
        Arc::clone(&self.object_store)
    }

    /// Return the catalog's time provider
    pub fn time_provider(&self) -> Arc<dyn TimeProvider> {
        Arc::clone(&self.time_provider)
    }

    /// Return the catalog's executor
    pub fn exec(&self) -> Arc<Executor> {
        Arc::clone(&self.exec)
    }

    /// Create a namesapce in teh catalog
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

/// A test namespace
#[allow(missing_docs)]
pub struct TestNamespace {
    pub catalog: Arc<TestCatalog>,
    pub kafka_topic: KafkaTopic,
    pub query_pool: QueryPool,
    pub namespace: Namespace,
}

impl TestNamespace {
    /// Create a table in this namespace
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

    /// Create a sequencer for this namespace
    pub async fn create_sequencer(self: &Arc<Self>, sequencer: i32) -> Arc<TestSequencer> {
        let mut repos = self.catalog.catalog.repositories().await;

        let sequencer = repos
            .sequencers()
            .create_or_get(&self.kafka_topic, KafkaPartition::new(sequencer))
            .await
            .unwrap();

        Arc::new(TestSequencer {
            catalog: Arc::clone(&self.catalog),
            namespace: Arc::clone(self),
            sequencer,
        })
    }
}

/// A test sequencer with ist namespace in the catalog
#[allow(missing_docs)]
pub struct TestSequencer {
    pub catalog: Arc<TestCatalog>,
    pub namespace: Arc<TestNamespace>,
    pub sequencer: Sequencer,
}

/// A test table of a namespace in the catalog
#[allow(missing_docs)]
pub struct TestTable {
    pub catalog: Arc<TestCatalog>,
    pub namespace: Arc<TestNamespace>,
    pub table: Table,
}

impl TestTable {
    /// Attach a sequncer to the table
    pub fn with_sequencer(
        self: &Arc<Self>,
        sequencer: &Arc<TestSequencer>,
    ) -> Arc<TestTableBoundSequencer> {
        assert!(Arc::ptr_eq(&self.catalog, &sequencer.catalog));
        assert!(Arc::ptr_eq(&self.namespace, &sequencer.namespace));

        Arc::new(TestTableBoundSequencer {
            catalog: Arc::clone(&self.catalog),
            namespace: Arc::clone(&self.namespace),
            table: Arc::clone(self),
            sequencer: Arc::clone(sequencer),
        })
    }

    /// Create a column for the table
    pub async fn create_column(self: &Arc<Self>, name: &str, column_type: ColumnType) {
        let mut repos = self.catalog.catalog.repositories().await;

        repos
            .columns()
            .create_or_get(name, self.table.id, column_type)
            .await
            .unwrap();
    }
}

/// A test catalog with specified namespace, sequencer, and table
#[allow(missing_docs)]
pub struct TestTableBoundSequencer {
    pub catalog: Arc<TestCatalog>,
    pub namespace: Arc<TestNamespace>,
    pub table: Arc<TestTable>,
    pub sequencer: Arc<TestSequencer>,
}

impl TestTableBoundSequencer {
    /// Creat a partition for the table
    pub async fn create_partition(self: &Arc<Self>, key: &str) -> Arc<TestPartition> {
        let mut repos = self.catalog.catalog.repositories().await;

        let partition = repos
            .partitions()
            .create_or_get(key, self.sequencer.sequencer.id, self.table.table.id)
            .await
            .unwrap();

        Arc::new(TestPartition {
            catalog: Arc::clone(&self.catalog),
            namespace: Arc::clone(&self.namespace),
            table: Arc::clone(&self.table),
            sequencer: Arc::clone(&self.sequencer),
            partition,
        })
    }

    /// Create a tombstone
    pub async fn create_tombstone(
        self: &Arc<Self>,
        sequence_number: i64,
        min_time: i64,
        max_time: i64,
        predicate: &str,
    ) -> Arc<TestTombstone> {
        let mut repos = self.catalog.catalog.repositories().await;

        let tombstone = repos
            .tombstones()
            .create_or_get(
                self.table.table.id,
                self.sequencer.sequencer.id,
                SequenceNumber::new(sequence_number),
                Timestamp::new(min_time),
                Timestamp::new(max_time),
                predicate,
            )
            .await
            .unwrap();

        Arc::new(TestTombstone {
            catalog: Arc::clone(&self.catalog),
            namespace: Arc::clone(&self.namespace),
            tombstone,
        })
    }
}

/// A test catalog with specified namespace, sequencer, table, partition
#[allow(missing_docs)]
pub struct TestPartition {
    pub catalog: Arc<TestCatalog>,
    pub namespace: Arc<TestNamespace>,
    pub table: Arc<TestTable>,
    pub sequencer: Arc<TestSequencer>,
    pub partition: Partition,
}

impl TestPartition {
    /// CReate a parquet for the partition
    pub async fn create_parquet_file(self: &Arc<Self>, lp: &str) -> Arc<TestParquetFile> {
        let mut repos = self.catalog.catalog.repositories().await;

        let (table, batch) = lp_to_mutable_batch(lp);
        assert_eq!(table, self.table.table.name);
        let row_count = batch.rows();
        let record_batch = batch.to_arrow(Selection::All).unwrap();

        let object_store_id = Uuid::new_v4();
        let min_sequence_number = SequenceNumber::new(1);
        let max_sequence_number = SequenceNumber::new(100);
        let metadata = IoxMetadata {
            object_store_id,
            creation_timestamp: now(),
            namespace_id: self.namespace.namespace.id,
            namespace_name: self.namespace.namespace.name.clone().into(),
            sequencer_id: self.sequencer.sequencer.id,
            table_id: self.table.table.id,
            table_name: self.table.table.name.clone().into(),
            partition_id: self.partition.id,
            partition_key: self.partition.partition_key.clone().into(),
            time_of_first_write: now(),
            time_of_last_write: now(),
            min_sequence_number,
            max_sequence_number,
            row_count: row_count as i64,
        };
        let (parquet_metadata_bin, file_size_bytes) =
            create_parquet_file(&self.catalog.object_store, &metadata, record_batch).await;

        // decode metadata because we need to store them within the catalog
        let parquet_metadata = Arc::new(IoxParquetMetaData::from_thrift_bytes(
            parquet_metadata_bin.clone(),
        ));
        let decoded_metadata = parquet_metadata.decode().unwrap();
        let schema = decoded_metadata.read_schema().unwrap();
        let stats = decoded_metadata.read_statistics(&schema).unwrap();
        let ts_min_max = stats
            .iter()
            .find_map(|stat| {
                (stat.influxdb_type == Some(InfluxDbType::Timestamp))
                    .then(|| stat.stats.timestamp_min_max().unwrap())
            })
            .unwrap();

        let parquet_file_params = ParquetFileParams {
            sequencer_id: self.sequencer.sequencer.id,
            table_id: self.table.table.id,
            partition_id: self.partition.id,
            object_store_id,
            min_sequence_number,
            max_sequence_number,
            min_time: Timestamp::new(ts_min_max.min),
            max_time: Timestamp::new(ts_min_max.max),
            file_size_bytes: file_size_bytes as i64,
            parquet_metadata: parquet_metadata_bin,
            row_count: row_count as i64,
            created_at: Timestamp::new(1),
        };
        let parquet_file = repos
            .parquet_files()
            .create(parquet_file_params)
            .await
            .unwrap();

        Arc::new(TestParquetFile {
            catalog: Arc::clone(&self.catalog),
            namespace: Arc::clone(&self.namespace),
            parquet_file,
        })
    }
}

/// Create parquet file and return thrift-encoded and zstd-compressed parquet metadata as well as the file size.
async fn create_parquet_file(
    object_store: &Arc<ObjectStore>,
    metadata: &IoxMetadata,
    record_batch: RecordBatch,
) -> (Vec<u8>, usize) {
    let iox_object_store = Arc::new(IoxObjectStore::existing(
        Arc::clone(object_store),
        IoxObjectStore::root_path_for(object_store, uuid::Uuid::new_v4()),
    ));

    let schema = record_batch.schema();

    let data = parquet_file::storage::Storage::new(Arc::clone(&iox_object_store))
        .parquet_bytes(vec![record_batch], schema, metadata)
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

/// A test parquet file of the catalog
#[allow(missing_docs)]
pub struct TestParquetFile {
    pub catalog: Arc<TestCatalog>,
    pub namespace: Arc<TestNamespace>,
    pub parquet_file: ParquetFile,
}

impl TestParquetFile {
    /// Make the parquet file deletable
    pub async fn flag_for_delete(self: &Arc<Self>) {
        let mut repos = self.catalog.catalog.repositories().await;

        repos
            .parquet_files()
            .flag_for_delete(self.parquet_file.id)
            .await
            .unwrap()
    }
}

/// A catalog test tombstone
#[allow(missing_docs)]
pub struct TestTombstone {
    pub catalog: Arc<TestCatalog>,
    pub namespace: Arc<TestNamespace>,
    pub tombstone: Tombstone,
}

impl TestTombstone {
    /// mark the tombstone proccesed
    pub async fn mark_processed(self: &Arc<Self>, parquet_file: &Arc<TestParquetFile>) {
        assert!(Arc::ptr_eq(&self.catalog, &parquet_file.catalog));
        assert!(Arc::ptr_eq(&self.namespace, &parquet_file.namespace));

        let mut repos = self.catalog.catalog.repositories().await;

        repos
            .processed_tombstones()
            .create(parquet_file.parquet_file.id, self.tombstone.id)
            .await
            .unwrap();
    }
}

fn now() -> Time {
    Time::from_timestamp(0, 0)
}
