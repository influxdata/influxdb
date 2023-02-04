use std::{
    collections::{BTreeMap, HashSet},
    future::Future,
    num::NonZeroUsize,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use backoff::BackoffConfig;
use data_types::{
    ColumnId, ColumnSchema, ColumnSet, ColumnType, CompactionLevel, Namespace, NamespaceId,
    NamespaceSchema, ParquetFile, ParquetFileId, Partition, PartitionId, PartitionKey, QueryPoolId,
    SequenceNumber, ShardId, SkippedCompaction, Table, TableId, TableSchema, Timestamp, TopicId,
    TRANSITION_SHARD_NUMBER,
};
use datafusion::arrow::record_batch::RecordBatch;
use futures::TryStreamExt;
use iox_tests::util::{TestCatalog, TestParquetFileBuilder, TestTable};
use iox_time::TimeProvider;
use object_store::{path::Path, DynObjectStore};
use parquet_file::storage::{ParquetStorage, StorageId};
use schema::sort::SortKey;
use uuid::Uuid;

use crate::{
    components::namespaces_source::mock::NamespaceWrapper,
    config::{AlgoVersion, Config},
    partition_info::PartitionInfo,
};

#[derive(Debug)]
pub struct ParquetFileBuilder {
    file: ParquetFile,
}

impl ParquetFileBuilder {
    pub fn new(id: i64) -> Self {
        Self {
            file: ParquetFile {
                id: ParquetFileId::new(id),
                shard_id: ShardId::new(0),
                namespace_id: NamespaceId::new(0),
                table_id: TableId::new(0),
                partition_id: PartitionId::new(0),
                object_store_id: Uuid::from_u128(id.try_into().expect("invalid id")),
                max_sequence_number: SequenceNumber::new(0),
                min_time: Timestamp::new(0),
                max_time: Timestamp::new(0),
                to_delete: None,
                file_size_bytes: 1,
                row_count: 1,
                compaction_level: CompactionLevel::FileNonOverlapped,
                created_at: Timestamp::new(0),
                column_set: ColumnSet::new(vec![]),
                max_l0_created_at: Timestamp::new(0),
            },
        }
    }

    pub fn with_partition(self, id: i64) -> Self {
        Self {
            file: ParquetFile {
                partition_id: PartitionId::new(id),
                ..self.file
            },
        }
    }

    pub fn with_compaction_level(self, level: CompactionLevel) -> Self {
        Self {
            file: ParquetFile {
                compaction_level: level,
                ..self.file
            },
        }
    }

    pub fn with_file_size_bytes(self, file_size_bytes: i64) -> Self {
        Self {
            file: ParquetFile {
                file_size_bytes,
                ..self.file
            },
        }
    }

    pub fn with_time_range(self, min_time: i64, max_time: i64) -> Self {
        Self {
            file: ParquetFile {
                min_time: Timestamp::new(min_time),
                max_time: Timestamp::new(max_time),
                ..self.file
            },
        }
    }

    pub fn build(self) -> ParquetFile {
        self.file
    }
}

#[derive(Debug)]
pub struct TableBuilder {
    table: Table,
}

impl TableBuilder {
    pub fn new(id: i64) -> Self {
        Self {
            table: Table {
                id: TableId::new(id),
                namespace_id: NamespaceId::new(0),
                name: "table".to_string(),
            },
        }
    }

    pub fn with_name(self, name: &str) -> Self {
        Self {
            table: Table {
                name: name.to_string(),
                ..self.table
            },
        }
    }

    pub fn build(self) -> Table {
        self.table
    }
}

#[derive(Debug)]
pub struct NamespaceBuilder {
    namespace: NamespaceWrapper,
}

impl NamespaceBuilder {
    pub fn new(id: i64) -> Self {
        let tables = BTreeMap::from([
            (
                "table1".to_string(),
                TableSchema {
                    id: TableId::new(1),
                    columns: BTreeMap::from([
                        (
                            "col1".to_string(),
                            ColumnSchema {
                                id: ColumnId::new(1),
                                column_type: ColumnType::I64,
                            },
                        ),
                        (
                            "col2".to_string(),
                            ColumnSchema {
                                id: ColumnId::new(2),
                                column_type: ColumnType::String,
                            },
                        ),
                    ]),
                },
            ),
            (
                "table2".to_string(),
                TableSchema {
                    id: TableId::new(2),
                    columns: BTreeMap::from([
                        (
                            "col1".to_string(),
                            ColumnSchema {
                                id: ColumnId::new(3),
                                column_type: ColumnType::I64,
                            },
                        ),
                        (
                            "col2".to_string(),
                            ColumnSchema {
                                id: ColumnId::new(4),
                                column_type: ColumnType::String,
                            },
                        ),
                        (
                            "col3".to_string(),
                            ColumnSchema {
                                id: ColumnId::new(5),
                                column_type: ColumnType::F64,
                            },
                        ),
                    ]),
                },
            ),
        ]);

        let id = NamespaceId::new(id);
        let topic_id = TopicId::new(0);
        let query_pool_id = QueryPoolId::new(0);
        Self {
            namespace: NamespaceWrapper {
                ns: Namespace {
                    id,
                    name: "ns".to_string(),
                    topic_id,
                    query_pool_id,
                    max_tables: 10,
                    max_columns_per_table: 10,
                    retention_period_ns: None,
                },
                schema: NamespaceSchema {
                    id,
                    topic_id,
                    query_pool_id,
                    tables,
                    max_columns_per_table: 10,
                    retention_period_ns: None,
                },
            },
        }
    }

    pub fn build(self) -> NamespaceWrapper {
        self.namespace
    }
}

#[derive(Debug)]
pub struct PartitionBuilder {
    partition: Partition,
}

impl PartitionBuilder {
    pub fn new(id: i64) -> Self {
        Self {
            partition: Partition {
                id: PartitionId::new(id),
                shard_id: ShardId::new(0),
                table_id: TableId::new(0),
                partition_key: PartitionKey::from("key"),
                sort_key: vec![],
                persisted_sequence_number: None,
                new_file_at: None,
            },
        }
    }

    pub fn build(self) -> Partition {
        self.partition
    }
}

#[derive(Debug)]
pub struct SkippedCompactionBuilder {
    skipped_compaction: SkippedCompaction,
}

impl SkippedCompactionBuilder {
    pub fn new(id: i64) -> Self {
        Self {
            skipped_compaction: SkippedCompaction {
                partition_id: PartitionId::new(id),
                reason: "test skipped compaction".to_string(),
                skipped_at: Timestamp::new(0),
                num_files: 0,
                limit_num_files: 0,
                estimated_bytes: 0,
                limit_bytes: 0,
                limit_num_files_first_in_partition: 0,
            },
        }
    }

    pub fn with_reason(self, reason: &str) -> Self {
        Self {
            skipped_compaction: SkippedCompaction {
                reason: reason.to_string(),
                ..self.skipped_compaction
            },
        }
    }

    pub fn build(self) -> SkippedCompaction {
        self.skipped_compaction
    }
}

const SHARD_INDEX: i32 = TRANSITION_SHARD_NUMBER;
const PARTITION_THRESHOLD: Duration = Duration::from_secs(10 * 60); // 10min
const MAX_DESIRE_FILE_SIZE: u64 = 100 * 1024;
const PERCENTAGE_MAX_FILE_SIZE: u16 = 5;
const SPLIT_PERCENTAGE: u16 = 80;

#[derive(Debug)]
pub struct TestSetupBuilder {
    with_files: bool,
    shadow_mode: bool,
    compact_version: AlgoVersion,
}

impl Default for TestSetupBuilder {
    fn default() -> Self {
        Self {
            with_files: false,
            shadow_mode: false,
            compact_version: AlgoVersion::AllAtOnce,
        }
    }
}

impl TestSetupBuilder {
    pub fn with_files(self) -> Self {
        Self {
            with_files: true,
            ..self
        }
    }

    pub fn with_shadow_mode(self) -> Self {
        Self {
            shadow_mode: true,
            ..self
        }
    }

    pub async fn build(self) -> TestSetup {
        let catalog = TestCatalog::new();
        let ns = catalog.create_namespace_1hr_retention("ns").await;
        let shard = ns.create_shard(SHARD_INDEX).await;
        let table = ns.create_table("table").await;
        table.create_column("field_int", ColumnType::I64).await;
        table.create_column("tag1", ColumnType::Tag).await;
        table.create_column("tag2", ColumnType::Tag).await;
        table.create_column("tag3", ColumnType::Tag).await;
        table.create_column("time", ColumnType::Time).await;
        let table_schema = table.catalog_schema().await;

        let partition = table
            .with_shard(&shard)
            .create_partition("2022-07-13")
            .await;

        // The sort key comes from the catalog and should be the union of all tags the
        // ingester has seen
        let sort_key = SortKey::from_columns(["tag1", "tag2", "tag3", "time"]);
        let partition = partition.update_sort_key(sort_key.clone()).await;

        let candidate_partition = Arc::new(PartitionInfo {
            partition_id: partition.partition.id,
            namespace_id: ns.namespace.id,
            namespace_name: ns.namespace.name.clone(),
            table: Arc::new(table.table.clone()),
            table_schema: Arc::new(table_schema),
            sort_key: partition.partition.sort_key(),
            partition_key: partition.partition.partition_key.clone(),
        });

        let time_provider = Arc::<iox_time::MockProvider>::clone(&catalog.time_provider);
        let mut parquet_files = vec![];
        if self.with_files {
            let time_1_minute_future = time_provider.minutes_into_future(1);
            let time_2_minutes_future = time_provider.minutes_into_future(2);
            let time_3_minutes_future = time_provider.minutes_into_future(3);
            let time_5_minutes_future = time_provider.minutes_into_future(5);

            // L1 file
            let lp = vec![
                "table,tag2=PA,tag3=15 field_int=1601i 30000",
                "table,tag2=OH,tag3=21 field_int=21i 36000", // will be eliminated due to duplicate
            ]
            .join("\n");
            let builder = TestParquetFileBuilder::default()
                .with_line_protocol(&lp)
                .with_creation_time(time_3_minutes_future)
                .with_max_l0_created_at(time_1_minute_future)
                .with_compaction_level(CompactionLevel::FileNonOverlapped); // Prev compaction
            let level_1_file_1_minute_ago = partition.create_parquet_file(builder).await.into();

            // L0 file
            let lp = vec![
                "table,tag1=WA field_int=1000i 8000", // will be eliminated due to duplicate
                "table,tag1=VT field_int=10i 10000", // latest L0 compared with duplicate in level_1_file_1_minute_ago_with_duplicates
                // keep it
                "table,tag1=UT field_int=70i 20000",
            ]
            .join("\n");
            let builder = TestParquetFileBuilder::default()
                .with_line_protocol(&lp)
                .with_creation_time(time_2_minutes_future)
                .with_max_l0_created_at(time_2_minutes_future)
                .with_compaction_level(CompactionLevel::Initial);
            let level_0_file_16_minutes_ago = partition.create_parquet_file(builder).await.into();

            // L0 file
            let lp = vec![
                "table,tag1=WA field_int=1500i 8000", // latest duplicate and kept
                "table,tag1=VT field_int=10i 6000",
                "table,tag1=UT field_int=270i 25000",
            ]
            .join("\n");
            let builder = TestParquetFileBuilder::default()
                .with_line_protocol(&lp)
                .with_creation_time(time_5_minutes_future)
                .with_max_l0_created_at(time_5_minutes_future)
                .with_compaction_level(CompactionLevel::Initial);
            let level_0_file_5_minutes_ago = partition.create_parquet_file(builder).await.into();

            // L1 file
            let lp = vec![
                "table,tag1=VT field_int=88i 10000", //  will be eliminated due to duplicate.
                // Note: created time more recent than level_0_file_16_minutes_ago
                // but always considered older ingested data
                "table,tag1=OR field_int=99i 12000",
            ]
            .join("\n");
            let builder = TestParquetFileBuilder::default()
                .with_line_protocol(&lp)
                .with_creation_time(time_5_minutes_future)
                .with_max_l0_created_at(time_3_minutes_future)
                .with_compaction_level(CompactionLevel::FileNonOverlapped); // Prev compaction
            let level_1_file_1_minute_ago_with_duplicates: ParquetFile =
                partition.create_parquet_file(builder).await.into();

            // L0 file
            let lp = vec!["table,tag2=OH,tag3=21 field_int=22i 36000"].join("\n");
            let builder = TestParquetFileBuilder::default()
                .with_line_protocol(&lp)
                .with_min_time(0)
                .with_max_time(36000)
                .with_creation_time(time_5_minutes_future)
                .with_max_l0_created_at(time_5_minutes_future)
                // Will put the group size between "small" and "large"
                .with_size_override(50 * 1024 * 1024)
                .with_compaction_level(CompactionLevel::Initial);
            let medium_level_0_file_time_now = partition.create_parquet_file(builder).await.into();

            // L0 file
            let lp = vec![
                "table,tag1=VT field_int=10i 68000",
                "table,tag2=OH,tag3=21 field_int=210i 136000",
            ]
            .join("\n");
            let builder = TestParquetFileBuilder::default()
                .with_line_protocol(&lp)
                .with_min_time(36001)
                .with_max_time(136000)
                .with_creation_time(time_2_minutes_future)
                .with_max_l0_created_at(time_2_minutes_future)
                // Will put the group size two multiples over "large"
                .with_size_override(180 * 1024 * 1024)
                .with_compaction_level(CompactionLevel::Initial);
            let large_level_0_file_2_2_minutes_ago =
                partition.create_parquet_file(builder).await.into();

            // Order here isn't relevant; the chunk order should ensure the level 1 files are ordered
            // first, then the other files by max seq num.
            parquet_files = vec![
                level_1_file_1_minute_ago,
                level_0_file_16_minutes_ago,
                level_0_file_5_minutes_ago,
                level_1_file_1_minute_ago_with_duplicates,
                medium_level_0_file_time_now,
                large_level_0_file_2_2_minutes_ago,
            ];
        }

        let config = Arc::new(Config {
            shard_id: shard.shard.id,
            metric_registry: catalog.metric_registry(),
            catalog: catalog.catalog(),
            parquet_store_real: catalog.parquet_store.clone(),
            parquet_store_scratchpad: ParquetStorage::new(
                Arc::new(object_store::memory::InMemory::new()),
                StorageId::from("scratchpad"),
            ),
            time_provider,
            exec: Arc::clone(&catalog.exec),
            backoff_config: BackoffConfig::default(),
            partition_concurrency: NonZeroUsize::new(1).unwrap(),
            job_concurrency: NonZeroUsize::new(1).unwrap(),
            partition_scratchpad_concurrency: NonZeroUsize::new(1).unwrap(),
            partition_threshold: PARTITION_THRESHOLD,
            max_desired_file_size_bytes: MAX_DESIRE_FILE_SIZE,
            percentage_max_file_size: PERCENTAGE_MAX_FILE_SIZE,
            split_percentage: SPLIT_PERCENTAGE,
            partition_timeout: Duration::from_secs(3_600),
            partition_filter: None,
            shadow_mode: self.shadow_mode,
            ignore_partition_skip_marker: false,
            max_input_files_per_partition: usize::MAX,
            max_input_parquet_bytes_per_partition: usize::MAX,
            shard_config: None,
            compact_version: self.compact_version,
        });

        TestSetup {
            files: Arc::new(parquet_files),
            partition_info: candidate_partition,
            catalog,
            table,
            config,
        }
    }
}

pub struct TestSetup {
    pub files: Arc<Vec<ParquetFile>>,
    pub partition_info: Arc<PartitionInfo>,
    pub catalog: Arc<TestCatalog>,
    pub table: Arc<TestTable>,
    pub config: Arc<Config>,
}

impl TestSetup {
    pub fn builder() -> TestSetupBuilder {
        TestSetupBuilder::default()
    }

    /// Get the catalog files stored in the catalog
    pub async fn list_by_table_not_to_delete(&self) -> Vec<ParquetFile> {
        self.catalog
            .list_by_table_not_to_delete(self.table.table.id)
            .await
    }

    /// Reads the specified parquet file out of object store
    pub async fn read_parquet_file(&self, file: ParquetFile) -> Vec<RecordBatch> {
        assert_eq!(file.table_id, self.table.table.id);
        self.table.read_parquet_file(file).await
    }
}

pub async fn list_object_store(store: &Arc<DynObjectStore>) -> HashSet<Path> {
    store
        .list(None)
        .await
        .unwrap()
        .map_ok(|f| f.location)
        .try_collect::<HashSet<_>>()
        .await
        .unwrap()
}

pub fn partition_info() -> Arc<PartitionInfo> {
    let namespace_id = NamespaceId::new(2);
    let table_id = TableId::new(3);

    Arc::new(PartitionInfo {
        partition_id: PartitionId::new(1),
        namespace_id,
        namespace_name: String::from("ns"),
        table: Arc::new(Table {
            id: table_id,
            namespace_id,
            name: String::from("table"),
        }),
        table_schema: Arc::new(TableSchema {
            id: table_id,
            columns: BTreeMap::from([]),
        }),
        sort_key: None,
        partition_key: PartitionKey::from("pk"),
    })
}

#[async_trait]
pub trait AssertFutureExt {
    type Output;

    async fn assert_pending(&mut self);
    async fn poll_timeout(self) -> Self::Output;
}

#[async_trait]
impl<F> AssertFutureExt for F
where
    F: Future + Send + Unpin,
{
    type Output = F::Output;

    async fn assert_pending(&mut self) {
        tokio::select! {
            biased;
            _ = self => {
                panic!("not pending")
            }
            _ = tokio::time::sleep(Duration::from_millis(10)) => {}
        }
    }

    async fn poll_timeout(self) -> Self::Output {
        tokio::time::timeout(Duration::from_millis(10), self)
            .await
            .expect("timeout")
    }
}

// This setup will return files with ranges as follows:
//                                  |--L0.1--|   |--L0.2--| |--L0.3--|
pub fn create_l0_files(size: i64) -> Vec<ParquetFile> {
    let l0_1 = ParquetFileBuilder::new(1)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(450, 620)
        .with_file_size_bytes(size)
        .build();
    let l0_2 = ParquetFileBuilder::new(2)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(650, 750)
        .with_file_size_bytes(size)
        .build();
    let l0_3 = ParquetFileBuilder::new(3)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(800, 900)
        .with_file_size_bytes(size)
        .build();

    // Put the files in random order
    vec![l0_2, l0_1, l0_3]
}

// This setup will return files with ranges as follows:
//              |--L0.1-----|
//                |--L0.2--|   |--L0.3--|
// L0.1 and L0.2 overlap, L0.3 is not overlapping
pub fn create_overlapping_l0_files(size: i64) -> Vec<ParquetFile> {
    let l0_1 = ParquetFileBuilder::new(1)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(100, 200)
        .with_file_size_bytes(size)
        .build();
    let l0_2 = ParquetFileBuilder::new(2)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(150, 180)
        .with_file_size_bytes(size)
        .build();
    let l0_3 = ParquetFileBuilder::new(3)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(800, 900)
        .with_file_size_bytes(size)
        .build();

    // Put the files in random order
    vec![l0_2, l0_1, l0_3]
}

// This setup will return files with ranges as follows:
//                  |--L1.1--|  |--L1.2--|  |--L1.3--|
pub fn create_l1_files(size: i64) -> Vec<ParquetFile> {
    let l1_1 = ParquetFileBuilder::new(11)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(250, 350)
        .with_file_size_bytes(size)
        .build();
    let l1_2 = ParquetFileBuilder::new(12)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(400, 500)
        .with_file_size_bytes(size)
        .build();
    let l1_3 = ParquetFileBuilder::new(13)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(600, 700)
        .with_file_size_bytes(size)
        .build();
    // Put the files in random order
    vec![l1_3, l1_2, l1_1]
}

// This setup will return files with ranges as follows:
//           |--L1.1--|  |--L1.2--|  |--L1.3--| |--L1.4--|  |--L1.5--|
//  . small files (< size ): L1.1, L1.3
//  . Large files (.= size): L1.2, L1.4, L1.5
pub fn create_l1_files_mix_size(size: i64) -> Vec<ParquetFile> {
    let l1_1 = ParquetFileBuilder::new(11)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(250, 350)
        .with_file_size_bytes(size - 1)
        .build();
    let l1_2 = ParquetFileBuilder::new(12)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(400, 500)
        .with_file_size_bytes(size + 1)
        .build();
    let l1_3 = ParquetFileBuilder::new(13)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(600, 700)
        .with_file_size_bytes(size - 10)
        .build();
    let l1_4 = ParquetFileBuilder::new(14)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(800, 900)
        .with_file_size_bytes(size)
        .build();
    let l1_5 = ParquetFileBuilder::new(15)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(1000, 1100)
        .with_file_size_bytes(size + 100)
        .build();

    // Put the files in random order
    vec![l1_5, l1_3, l1_2, l1_1, l1_4]
}

// This setup will return files with ranges as follows:
//    |--L2.1--|  |--L2.2--|
pub fn create_l2_files() -> Vec<ParquetFile> {
    let l2_1 = ParquetFileBuilder::new(21)
        .with_compaction_level(CompactionLevel::Final)
        .with_time_range(0, 100)
        .build();
    let l2_2 = ParquetFileBuilder::new(22)
        .with_compaction_level(CompactionLevel::Final)
        .with_time_range(200, 300)
        .build();

    // Put the files in random order
    vec![l2_1, l2_2]
}

// This setup will return files with ranges as follows:
//                  |--L1.1--|  |--L1.2--|  |--L1.3--|
//                                  |--L0.1--|   |--L0.2--| |--L0.3--|
pub fn create_overlapped_l0_l1_files(size: i64) -> Vec<ParquetFile> {
    let l1_1 = ParquetFileBuilder::new(11)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(250, 350)
        .with_file_size_bytes(size)
        .build();
    let l1_2 = ParquetFileBuilder::new(12)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(400, 500)
        .with_file_size_bytes(size)
        .build();
    let l1_3 = ParquetFileBuilder::new(13)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(600, 700)
        .with_file_size_bytes(size)
        .build();

    // L0_1 overlaps with L1_2 and L1_3
    let l0_1 = ParquetFileBuilder::new(1)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(450, 620)
        .with_file_size_bytes(size)
        .build();
    // L0_2 overlaps with L1_3
    let l0_2 = ParquetFileBuilder::new(2)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(650, 750)
        .with_file_size_bytes(size)
        .build();
    // L0_3 overlaps with nothing
    let l0_3 = ParquetFileBuilder::new(3)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(800, 900)
        .with_file_size_bytes(size)
        .build();

    // Put the files in random order
    vec![l1_3, l1_2, l0_2, l1_1, l0_1, l0_3]
}

// This setup will return files with ranges as follows:
//    |--L2.1--|  |--L2.2--|
//                  |--L1.1--|  |--L1.2--|  |--L1.3--|
pub fn create_overlapped_l1_l2_files(size: i64) -> Vec<ParquetFile> {
    let l2_1 = ParquetFileBuilder::new(21)
        .with_compaction_level(CompactionLevel::Final)
        .with_time_range(0, 100)
        .with_file_size_bytes(size)
        .build();
    let l2_2 = ParquetFileBuilder::new(22)
        .with_compaction_level(CompactionLevel::Final)
        .with_time_range(200, 300)
        .with_file_size_bytes(size)
        .build();

    // L1_1 overlaps with L2_1
    let l1_1 = ParquetFileBuilder::new(11)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(250, 350)
        .with_file_size_bytes(size)
        .build();
    let l1_2 = ParquetFileBuilder::new(12)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(400, 500)
        .with_file_size_bytes(size)
        .build();
    let l1_3 = ParquetFileBuilder::new(13)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(600, 700)
        .with_file_size_bytes(size)
        .build();

    // Put the files in random order
    vec![l1_3, l1_2, l2_1, l2_2, l1_1]
}

// This setup will return files with ranges as follows with mixed sizes:
//    |--L2.1--|  |--L2.2--|
//                  |--L1.1--|  |--L1.2--|  |--L1.3--|
//  Small files (< size): [L1.3]
//  Large files: [L2.1, L2.2, L1.1, L1.2]
pub fn create_overlapped_l1_l2_files_mix_size(size: i64) -> Vec<ParquetFile> {
    let l2_1 = ParquetFileBuilder::new(21)
        .with_compaction_level(CompactionLevel::Final)
        .with_time_range(0, 100)
        .with_file_size_bytes(size)
        .build();
    let l2_2 = ParquetFileBuilder::new(22)
        .with_compaction_level(CompactionLevel::Final)
        .with_time_range(200, 300)
        .with_file_size_bytes(size)
        .build();

    // L1_1 overlaps with L2_1
    let l1_1 = ParquetFileBuilder::new(11)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(250, 350)
        .with_file_size_bytes(size)
        .build();
    let l1_2 = ParquetFileBuilder::new(12)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(400, 500)
        .with_file_size_bytes(size)
        .build();
    let l1_3 = ParquetFileBuilder::new(13)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(600, 700)
        .with_file_size_bytes(size - 1)
        .build();

    // Put the files in random order
    vec![l1_3, l1_2, l2_1, l2_2, l1_1]
}

// This setup will return files with ranges as follows with mixed sizes:
//    |--L2.1--|  |--L2.2--|
//                  |--L1.1--|  |--L1.2--|  |--L1.3--|
//  Small files (< size): [L1.2]
//  Large files: [L2.1, L2.2, L1.1, L1.3]
pub fn create_overlapped_l1_l2_files_mix_size_2(size: i64) -> Vec<ParquetFile> {
    let l2_1 = ParquetFileBuilder::new(21)
        .with_compaction_level(CompactionLevel::Final)
        .with_time_range(0, 100)
        .with_file_size_bytes(size)
        .build();
    let l2_2 = ParquetFileBuilder::new(22)
        .with_compaction_level(CompactionLevel::Final)
        .with_time_range(200, 300)
        .with_file_size_bytes(size)
        .build();

    // L1_1 overlaps with L2_1
    let l1_1 = ParquetFileBuilder::new(11)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(250, 350)
        .with_file_size_bytes(size)
        .build();
    let l1_2 = ParquetFileBuilder::new(12)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(400, 500)
        .with_file_size_bytes(size - 1)
        .build();
    let l1_3 = ParquetFileBuilder::new(13)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(600, 700)
        .with_file_size_bytes(size)
        .build();

    // Put the files in random order
    vec![l1_3, l1_2, l2_1, l2_2, l1_1]
}

// This setup will return files with ranges as follows:
//    |--L2.1--|  |--L2.2--|
//                  |--L1.1--|  |--L1.2--|  |--L1.3--|
//                                  |--L0.1--|   |--L0.2--| |--L0.3--|
// Sizes of L1.3 and L0.3 are set large (100), the rest is default (1)
pub fn create_overlapped_files() -> Vec<ParquetFile> {
    let l2_1 = ParquetFileBuilder::new(21)
        .with_compaction_level(CompactionLevel::Final)
        .with_time_range(0, 100)
        .build();
    let l2_2 = ParquetFileBuilder::new(22)
        .with_compaction_level(CompactionLevel::Final)
        .with_time_range(200, 300)
        .build();

    // L1_1 overlaps with L2_1
    let l1_1 = ParquetFileBuilder::new(11)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(250, 350)
        .build();
    let l1_2 = ParquetFileBuilder::new(12)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(400, 500)
        .build();
    let l1_3 = ParquetFileBuilder::new(13)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(600, 700)
        .with_file_size_bytes(100)
        .build();

    // L0_1 overlaps with L1_2 and L1_3
    let l0_1 = ParquetFileBuilder::new(1)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(450, 620)
        .build();
    // L0_2 overlaps with L1_3
    let l0_2 = ParquetFileBuilder::new(2)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(650, 750)
        .build();
    // L0_3 overlaps with nothing
    let l0_3 = ParquetFileBuilder::new(3)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(800, 900)
        .with_file_size_bytes(100)
        .build();

    // Put the files in random order
    vec![l1_3, l1_2, l2_1, l2_2, l0_2, l1_1, l0_1, l0_3]
}

// This setup will return files with ranges as follows:
///                          |--L0.1--|             |--L0.2--|
///            |--L1.1--| |--L1.2--|    |--L1.3--|              |--L1.4--|
pub fn create_overlapped_files_2(size: i64) -> Vec<ParquetFile> {
    let l1_1 = ParquetFileBuilder::new(11)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(0, 100)
        .with_file_size_bytes(size)
        .build();
    let l1_2 = ParquetFileBuilder::new(12)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(200, 300)
        .with_file_size_bytes(size)
        .build();
    let l1_3 = ParquetFileBuilder::new(13)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(400, 500)
        .with_file_size_bytes(size)
        .build();
    let l1_4 = ParquetFileBuilder::new(14)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(600, 700)
        .with_file_size_bytes(size)
        .build();
    // L0_1 overlaps with L1_2
    let l0_1 = ParquetFileBuilder::new(1)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(250, 350)
        .with_file_size_bytes(size)
        .build();
    // L0_2 not overlap but in the middle of L1_3 and L1_4
    let l0_2 = ParquetFileBuilder::new(2)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(520, 550)
        .with_file_size_bytes(size)
        .build();

    // Put the files in random order
    vec![l1_3, l1_2, l1_1, l1_4, l0_2, l0_1]
}

// This setup will return files with ranges as follows:
//             |--L0.1--| |--L0.2--| |--L0.3--|
//                                              |--L0.4--|     |--L0.5--| |--L0.6--|
//                        |--L1.1--|              |--L1.2--|
pub fn create_overlapped_files_3(size: i64) -> Vec<ParquetFile> {
    let l0_1 = ParquetFileBuilder::new(1)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(0, 100)
        .with_file_size_bytes(size)
        .build();
    let l0_2 = ParquetFileBuilder::new(2)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(200, 300)
        .with_file_size_bytes(size)
        .build();
    let l0_3 = ParquetFileBuilder::new(3)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(400, 500)
        .with_file_size_bytes(size)
        .build();
    let l0_4 = ParquetFileBuilder::new(4)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(600, 700)
        .with_file_size_bytes(size)
        .build();
    let l0_5 = ParquetFileBuilder::new(5)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(800, 900)
        .with_file_size_bytes(size)
        .build();
    let l0_6 = ParquetFileBuilder::new(6)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(1000, 1100)
        .with_file_size_bytes(size)
        .build();
    // L1_1 overlaps with L0_2
    let l1_1 = ParquetFileBuilder::new(11)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(250, 350)
        .with_file_size_bytes(size)
        .build();
    // L1_2 overlaps with L0_4
    let l1_2 = ParquetFileBuilder::new(12)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(650, 750)
        .with_file_size_bytes(size)
        .build();

    // Put the files in random order
    vec![l0_3, l0_2, l0_1, l0_4, l0_5, l0_6, l1_1, l1_2]
}

// This setup will return files with ranges as follows:
//             |--L0.1--| |--L0.2--| |--L0.3--|
//                                              |--L0.4--|     |--L0.5--| |--L0.6--|
//                        |--L1.1--|              |--L1.2--|
// Small files (< size): L0.6
// Large files: the rest
pub fn create_overlapped_files_3_mix_size(size: i64) -> Vec<ParquetFile> {
    let l0_1 = ParquetFileBuilder::new(1)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(0, 100)
        .with_file_size_bytes(size)
        .build();
    let l0_2 = ParquetFileBuilder::new(2)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(200, 300)
        .with_file_size_bytes(size)
        .build();
    let l0_3 = ParquetFileBuilder::new(3)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(400, 500)
        .with_file_size_bytes(size)
        .build();
    let l0_4 = ParquetFileBuilder::new(4)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(600, 700)
        .with_file_size_bytes(size)
        .build();
    let l0_5 = ParquetFileBuilder::new(5)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(800, 900)
        .with_file_size_bytes(size)
        .build();
    let l0_6 = ParquetFileBuilder::new(6)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(1000, 1100)
        .with_file_size_bytes(size - 1)
        .build();
    // L1_1 overlaps with L0_2
    let l1_1 = ParquetFileBuilder::new(11)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(250, 350)
        .with_file_size_bytes(size)
        .build();
    // L1_2 overlaps with L0_4
    let l1_2 = ParquetFileBuilder::new(12)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(650, 750)
        .with_file_size_bytes(size)
        .build();

    // Put the files in random order
    vec![l0_3, l0_2, l0_1, l0_4, l0_5, l0_6, l1_1, l1_2]
}
