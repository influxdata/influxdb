//! Utils of the tests

use arrow::{
    compute::{lexsort, SortColumn, SortOptions},
    record_batch::RecordBatch,
};
use data_types::{
    Column, ColumnType, KafkaPartition, KafkaTopic, Namespace, ParquetFile, ParquetFileId,
    ParquetFileParams, ParquetFileWithMetadata, Partition, PartitionId, QueryPool, SequenceNumber,
    Sequencer, SequencerId, Table, TableId, Timestamp, Tombstone, TombstoneId,
};
use datafusion::physical_plan::metrics::Count;
use iox_catalog::{
    interface::{Catalog, PartitionRepo, INITIAL_COMPACTION_LEVEL},
    mem::MemCatalog,
};
use iox_query::{exec::Executor, provider::RecordBatchDeduplicator, util::arrow_sort_key_exprs};
use iox_time::{MockProvider, Time, TimeProvider};
use mutable_batch_lp::test_helpers::lp_to_mutable_batch;
use object_store::{memory::InMemory, DynObjectStore};
use parquet_file::{metadata::IoxMetadata, storage::ParquetStorage};
use schema::{
    selection::Selection,
    sort::{adjust_sort_key_columns, SortKey, SortKeyBuilder},
    Schema,
};
use std::sync::Arc;
use uuid::Uuid;

/// Catalog for tests
#[derive(Debug)]
#[allow(missing_docs)]
pub struct TestCatalog {
    pub catalog: Arc<dyn Catalog>,
    pub metric_registry: Arc<metric::Registry>,
    pub object_store: Arc<DynObjectStore>,
    pub time_provider: Arc<MockProvider>,
    pub exec: Arc<Executor>,
}

impl TestCatalog {
    /// Initialize the catalog
    pub fn new() -> Arc<Self> {
        let metric_registry = Arc::new(metric::Registry::new());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metric_registry)));
        let object_store = Arc::new(InMemory::new());
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
    pub fn object_store(&self) -> Arc<DynObjectStore> {
        Arc::clone(&self.object_store)
    }

    /// Return the mockable version of the catalog's time provider.
    ///
    /// If you need a generic time provider, use [`time_provider`](Self::time_provider) instead.
    pub fn mock_time_provider(&self) -> &MockProvider {
        self.time_provider.as_ref()
    }

    /// Return the catalog's time provider
    ///
    /// If you need to mock the time, use [`mock_time_provider`](Self::mock_time_provider) instead.
    pub fn time_provider(&self) -> Arc<dyn TimeProvider> {
        Arc::clone(&self.time_provider) as _
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

    /// return tombstones of a given table
    pub async fn list_tombstones_by_table(self: &Arc<Self>, table_id: TableId) -> Vec<Tombstone> {
        self.catalog
            .repositories()
            .await
            .tombstones()
            .list_by_table(table_id)
            .await
            .unwrap()
    }

    /// return number of tombstones of a given table
    pub async fn count_tombstones_for_table(self: &Arc<Self>, table_id: TableId) -> usize {
        let ts = self
            .catalog
            .repositories()
            .await
            .tombstones()
            .list_by_table(table_id)
            .await
            .unwrap();
        ts.len()
    }

    /// return number of processed tombstones of a tombstones
    pub async fn count_processed_tombstones(self: &Arc<Self>, tombstone_id: TombstoneId) -> i64 {
        self.catalog
            .repositories()
            .await
            .processed_tombstones()
            .count_by_tombstone_id(tombstone_id)
            .await
            .unwrap()
    }

    /// List level 0 files
    pub async fn list_level_0_files(
        self: &Arc<Self>,
        sequencer_id: SequencerId,
    ) -> Vec<ParquetFile> {
        self.catalog
            .repositories()
            .await
            .parquet_files()
            .level_0(sequencer_id)
            .await
            .unwrap()
    }

    /// Count level 0 files
    pub async fn count_level_0_files(self: &Arc<Self>, sequencer_id: SequencerId) -> usize {
        let level_0 = self
            .catalog
            .repositories()
            .await
            .parquet_files()
            .level_0(sequencer_id)
            .await
            .unwrap();
        level_0.len()
    }

    /// List all non-deleted files
    pub async fn list_by_table_not_to_delete(
        self: &Arc<Self>,
        table_id: TableId,
    ) -> Vec<ParquetFile> {
        self.catalog
            .repositories()
            .await
            .parquet_files()
            .list_by_table_not_to_delete(table_id)
            .await
            .unwrap()
    }

    /// List all non-deleted files with their metadata
    pub async fn list_by_table_not_to_delete_with_metadata(
        self: &Arc<Self>,
        table_id: TableId,
    ) -> Vec<ParquetFileWithMetadata> {
        self.catalog
            .repositories()
            .await
            .parquet_files()
            .list_by_table_not_to_delete_with_metadata(table_id)
            .await
            .unwrap()
    }

    /// Get a parquet file's metadata bytes
    pub async fn parquet_metadata(&self, parquet_file_id: ParquetFileId) -> Vec<u8> {
        self.catalog
            .repositories()
            .await
            .parquet_files()
            .parquet_metadata(parquet_file_id)
            .await
            .unwrap()
    }
}

/// A test namespace
#[derive(Debug)]
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
#[derive(Debug)]
#[allow(missing_docs)]
pub struct TestSequencer {
    pub catalog: Arc<TestCatalog>,
    pub namespace: Arc<TestNamespace>,
    pub sequencer: Sequencer,
}

/// A test table of a namespace in the catalog
#[allow(missing_docs)]
#[derive(Debug)]
pub struct TestTable {
    pub catalog: Arc<TestCatalog>,
    pub namespace: Arc<TestNamespace>,
    pub table: Table,
}

impl TestTable {
    /// Attach a sequencer to the table
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
    pub async fn create_column(
        self: &Arc<Self>,
        name: &str,
        column_type: ColumnType,
    ) -> Arc<TestColumn> {
        let mut repos = self.catalog.catalog.repositories().await;

        let column = repos
            .columns()
            .create_or_get(name, self.table.id, column_type)
            .await
            .unwrap();

        Arc::new(TestColumn {
            catalog: Arc::clone(&self.catalog),
            namespace: Arc::clone(&self.namespace),
            table: Arc::clone(self),
            column,
        })
    }
}

/// A test column.
#[allow(missing_docs)]
pub struct TestColumn {
    pub catalog: Arc<TestCatalog>,
    pub namespace: Arc<TestNamespace>,
    pub table: Arc<TestTable>,
    pub column: Column,
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

    /// Creat a partition with a specifiyed sory key for the table
    pub async fn create_partition_with_sort_key(
        self: &Arc<Self>,
        key: &str,
        sort_key: &str,
    ) -> Arc<TestPartition> {
        let mut repos = self.catalog.catalog.repositories().await;

        let partition = repos
            .partitions()
            .create_or_get(key, self.sequencer.sequencer.id, self.table.table.id)
            .await
            .unwrap();

        let partition = repos
            .partitions()
            .update_sort_key(partition.id, sort_key)
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
#[derive(Debug)]
pub struct TestPartition {
    pub catalog: Arc<TestCatalog>,
    pub namespace: Arc<TestNamespace>,
    pub table: Arc<TestTable>,
    pub sequencer: Arc<TestSequencer>,
    pub partition: Partition,
}

impl TestPartition {
    /// Update sort key.
    pub async fn update_sort_key(self: &Arc<Self>, sort_key: SortKey) -> Self {
        let partition = self
            .catalog
            .catalog
            .repositories()
            .await
            .partitions()
            .update_sort_key(self.partition.id, &sort_key.to_columns())
            .await
            .unwrap();

        Self {
            catalog: Arc::clone(&self.catalog),
            namespace: Arc::clone(&self.namespace),
            table: Arc::clone(&self.table),
            sequencer: Arc::clone(&self.sequencer),
            partition,
        }
    }

    /// Create a parquet for the partition
    pub async fn create_parquet_file(self: &Arc<Self>, lp: &str) -> TestParquetFile {
        self.create_parquet_file_with_min_max(
            lp,
            1,
            100,
            now().timestamp_nanos(),
            now().timestamp_nanos(),
        )
        .await
    }

    /// Create a parquet for the partition with the given min/max sequence numbers and min/max time
    pub async fn create_parquet_file_with_min_max(
        self: &Arc<Self>,
        lp: &str,
        min_seq: i64,
        max_seq: i64,
        min_time: i64,
        max_time: i64,
    ) -> TestParquetFile {
        self.create_parquet_file_with_min_max_and_creation_time(
            lp, min_seq, max_seq, min_time, max_time, 1,
        )
        .await
    }

    /// Create a parquet for the partition
    pub async fn create_parquet_file_with_min_max_and_creation_time(
        self: &Arc<Self>,
        lp: &str,
        min_seq: i64,
        max_seq: i64,
        min_time: i64,
        max_time: i64,
        creation_time: i64,
    ) -> TestParquetFile {
        let (table, batch) = lp_to_mutable_batch(lp);
        assert_eq!(table, self.table.table.name);

        let schema = batch.schema(Selection::All).unwrap();
        let record_batch = batch.to_arrow(Selection::All).unwrap();

        self.create_parquet_file_with_batch(
            record_batch,
            schema,
            min_seq,
            max_seq,
            min_time,
            max_time,
            None,
            creation_time,
        )
        .await
    }

    /// Create a parquet with the data in the specified `record_batch` for the partition
    #[allow(clippy::too_many_arguments)]
    pub async fn create_parquet_file_with_batch(
        self: &Arc<Self>,
        record_batch: RecordBatch,
        schema: Schema,
        min_seq: i64,
        max_seq: i64,
        min_time: i64,
        max_time: i64,
        file_size_bytes: Option<i64>,
        creation_time: i64,
    ) -> TestParquetFile {
        let mut repos = self.catalog.catalog.repositories().await;

        let row_count = record_batch.num_rows();
        assert!(row_count > 0, "Parquet file must have at least 1 row");
        let (record_batch, sort_key) = sort_batch(record_batch, schema);
        let record_batch = dedup_batch(record_batch, &sort_key);

        let object_store_id = Uuid::new_v4();
        let min_sequence_number = SequenceNumber::new(min_seq);
        let max_sequence_number = SequenceNumber::new(max_seq);
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
            min_sequence_number,
            max_sequence_number,
            compaction_level: INITIAL_COMPACTION_LEVEL,
            sort_key: Some(sort_key.clone()),
        };
        let (parquet_metadata_bin, real_file_size_bytes) = create_parquet_file(
            ParquetStorage::new(Arc::clone(&self.catalog.object_store)),
            &metadata,
            record_batch,
        )
        .await;

        let parquet_file_params = ParquetFileParams {
            sequencer_id: self.sequencer.sequencer.id,
            namespace_id: self.namespace.namespace.id,
            table_id: self.table.table.id,
            partition_id: self.partition.id,
            object_store_id,
            min_sequence_number,
            max_sequence_number,
            min_time: Timestamp::new(min_time),
            max_time: Timestamp::new(max_time),
            file_size_bytes: file_size_bytes.unwrap_or(real_file_size_bytes as i64),
            parquet_metadata: parquet_metadata_bin.clone(),
            row_count: row_count as i64,
            created_at: Timestamp::new(creation_time),
            compaction_level: INITIAL_COMPACTION_LEVEL,
        };
        let parquet_file = repos
            .parquet_files()
            .create(parquet_file_params)
            .await
            .unwrap();

        let parquet_file = ParquetFileWithMetadata::new(parquet_file, parquet_metadata_bin);

        update_catalog_sort_key_if_needed(repos.partitions(), self.partition.id, sort_key).await;

        TestParquetFile {
            catalog: Arc::clone(&self.catalog),
            namespace: Arc::clone(&self.namespace),
            parquet_file,
        }
    }

    /// Create a parquet for the partition with fake sizew for testing
    #[allow(clippy::too_many_arguments)]
    pub async fn create_parquet_file_with_min_max_size_and_creation_time(
        self: &Arc<Self>,
        lp: &str,
        min_seq: i64,
        max_seq: i64,
        min_time: i64,
        max_time: i64,
        file_size_bytes: i64,
        creation_time: i64,
    ) -> TestParquetFile {
        let (table, batch) = lp_to_mutable_batch(lp);
        assert_eq!(table, self.table.table.name);

        let schema = batch.schema(Selection::All).unwrap();
        let record_batch = batch.to_arrow(Selection::All).unwrap();

        self.create_parquet_file_with_batch(
            record_batch,
            schema,
            min_seq,
            max_seq,
            min_time,
            max_time,
            Some(file_size_bytes),
            creation_time,
        )
        .await
    }
}

async fn update_catalog_sort_key_if_needed(
    partitions_catalog: &mut dyn PartitionRepo,
    partition_id: PartitionId,
    sort_key: SortKey,
) {
    // Fetch the latest partition info from the catalog
    let partition = partitions_catalog
        .get_by_id(partition_id)
        .await
        .unwrap()
        .unwrap();

    // Similarly to what the ingester does, if there's an existing sort key in the catalog, add new
    // columns onto the end
    match partition.sort_key() {
        Some(catalog_sort_key) => {
            let sort_key_string = sort_key.to_columns();
            let new_sort_key: Vec<_> = sort_key_string.split(',').collect();
            let (_metadata, update) = adjust_sort_key_columns(&catalog_sort_key, &new_sort_key);
            if let Some(new_sort_key) = update {
                let new_columns = new_sort_key.to_columns();
                dbg!(
                    "Updating sort key from {:?} to {:?}",
                    catalog_sort_key.to_columns(),
                    &new_columns,
                );
                partitions_catalog
                    .update_sort_key(partition_id, &new_columns)
                    .await
                    .unwrap();
            }
        }
        None => {
            let new_columns = sort_key.to_columns();
            dbg!("Updating sort key from None to {:?}", &new_columns,);
            partitions_catalog
                .update_sort_key(partition_id, &new_columns)
                .await
                .unwrap();
        }
    }
}

/// Create parquet file and return thrift-encoded and zstd-compressed parquet metadata as well as the file size.
async fn create_parquet_file(
    store: ParquetStorage,
    metadata: &IoxMetadata,
    record_batch: RecordBatch,
) -> (Vec<u8>, usize) {
    let stream = futures::stream::once(async { Ok(record_batch) });
    let (meta, file_size) = store
        .upload(stream, metadata)
        .await
        .expect("persisting parquet file should succeed");
    (meta.thrift_bytes().to_vec(), file_size)
}

/// A test parquet file of the catalog
#[allow(missing_docs)]
pub struct TestParquetFile {
    pub catalog: Arc<TestCatalog>,
    pub namespace: Arc<TestNamespace>,
    pub parquet_file: ParquetFileWithMetadata,
}

impl TestParquetFile {
    /// Make the parquet file deletable
    pub async fn flag_for_delete(&self) {
        let mut repos = self.catalog.catalog.repositories().await;

        repos
            .parquet_files()
            .flag_for_delete(self.parquet_file.id)
            .await
            .unwrap()
    }

    /// When only the ParquetFile is needed without the metadata, use this instead of the field
    pub fn parquet_file_no_metadata(self) -> ParquetFile {
        self.parquet_file.split_off_metadata().0
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
    pub async fn mark_processed(self: &Arc<Self>, parquet_file: &TestParquetFile) {
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

/// Return the current time
pub fn now() -> Time {
    Time::from_timestamp(0, 0)
}

/// Sort arrow record batch into arrow record batch and sort key.
fn sort_batch(record_batch: RecordBatch, schema: Schema) -> (RecordBatch, SortKey) {
    // build dummy sort key
    let mut sort_key_builder = SortKeyBuilder::new();
    for field in schema.tags_iter() {
        sort_key_builder = sort_key_builder.with_col(field.name().clone());
    }
    for field in schema.time_iter() {
        sort_key_builder = sort_key_builder.with_col(field.name().clone());
    }
    let sort_key = sort_key_builder.build();

    // set up sorting
    let mut sort_columns = Vec::with_capacity(record_batch.num_columns());
    let mut reverse_index: Vec<_> = (0..record_batch.num_columns()).map(|_| None).collect();
    for (column_name, _options) in sort_key.iter() {
        let index = record_batch
            .schema()
            .column_with_name(column_name.as_ref())
            .unwrap()
            .0;
        reverse_index[index] = Some(sort_columns.len());
        sort_columns.push(SortColumn {
            values: Arc::clone(record_batch.column(index)),
            options: Some(SortOptions::default()),
        });
    }
    for (index, reverse_index) in reverse_index.iter_mut().enumerate() {
        if reverse_index.is_none() {
            *reverse_index = Some(sort_columns.len());
            sort_columns.push(SortColumn {
                values: Arc::clone(record_batch.column(index)),
                options: None,
            });
        }
    }

    // execute sorting
    let arrays = lexsort(&sort_columns, None).unwrap();

    // re-create record batch
    let arrays: Vec<_> = reverse_index
        .into_iter()
        .map(|index| {
            let index = index.unwrap();
            Arc::clone(&arrays[index])
        })
        .collect();
    let record_batch = RecordBatch::try_new(record_batch.schema(), arrays).unwrap();

    (record_batch, sort_key)
}

fn dedup_batch(record_batch: RecordBatch, sort_key: &SortKey) -> RecordBatch {
    let schema = record_batch.schema();
    let sort_keys = arrow_sort_key_exprs(sort_key, &schema);
    let mut deduplicator = RecordBatchDeduplicator::new(sort_keys, Count::default(), None);

    let mut batches = vec![deduplicator.push(record_batch).unwrap()];
    if let Some(batch) = deduplicator.finish().unwrap() {
        batches.push(batch);
    }

    RecordBatch::concat(&schema, &batches).unwrap()
}
