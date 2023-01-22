use std::{collections::BTreeMap, num::NonZeroUsize, sync::Arc};

use backoff::BackoffConfig;
use data_types::{
    ColumnId, ColumnSchema, ColumnSet, ColumnType, CompactionLevel, Namespace, NamespaceId,
    NamespaceSchema, ParquetFile, ParquetFileId, Partition, PartitionId, PartitionKey, QueryPoolId,
    SequenceNumber, ShardId, Table, TableId, TableSchema, Timestamp, TopicId,
};
use datafusion::arrow::record_batch::RecordBatch;
use iox_tests::util::{TestCatalog, TestParquetFileBuilder, TestTable};
use iox_time::{SystemProvider, TimeProvider};
use schema::sort::SortKey;
use uuid::Uuid;

use crate::{
    components::{compact::partition::PartitionInfo, namespaces_source::mock::NamespaceWrapper},
    config::Config,
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

const SHARD_INDEX: i32 = 1;
const PARTITION_MINUTE_THRESHOLD: u64 = 10;
const MAX_DESIRE_FILE_SIZE: u64 = 100 * 1024;
const PERCENTAGE_MAX_FILE_SIZE: u16 = 5;
const SPLIT_PERCENTAGE: u16 = 80;

pub struct TestSetup {
    pub files: Arc<Vec<ParquetFile>>,
    pub partition_info: Arc<crate::components::compact::partition::PartitionInfo>,
    pub catalog: Arc<TestCatalog>,
    pub table: Arc<TestTable>,
    pub config: Arc<Config>,
}

impl TestSetup {
    pub async fn new(with_files: bool) -> Self {
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

        let candidate_partition = Arc::new(PartitionInfo::new(
            partition.partition.id,
            ns.namespace.id,
            ns.namespace.name.clone(),
            Arc::new(table.table.clone()),
            Arc::new(table_schema),
            partition.partition.sort_key(),
            partition.partition.partition_key.clone(),
        ));

        let mut parquet_files = vec![];
        if with_files {
            let time = SystemProvider::new();
            let time_16_minutes_ago = time.minutes_ago(16);
            let time_5_minutes_ago = time.minutes_ago(5);
            let time_2_minutes_ago = time.minutes_ago(2);
            let time_1_minute_ago = time.minutes_ago(1);
            let time_now = time.now();

            // L1 file
            let lp = vec![
                "table,tag2=PA,tag3=15 field_int=1601i 30000",
                "table,tag2=OH,tag3=21 field_int=21i 36000", // will be eliminated due to duplicate
            ]
            .join("\n");
            let builder = TestParquetFileBuilder::default()
                .with_line_protocol(&lp)
                .with_creation_time(time_1_minute_ago)
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
                .with_creation_time(time_16_minutes_ago)
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
                .with_creation_time(time_5_minutes_ago)
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
                .with_creation_time(time_1_minute_ago)
                .with_compaction_level(CompactionLevel::FileNonOverlapped); // Prev compaction
            let level_1_file_1_minute_ago_with_duplicates =
                partition.create_parquet_file(builder).await.into();

            // L0 file
            let lp = vec!["table,tag2=OH,tag3=21 field_int=22i 36000"].join("\n");
            let builder = TestParquetFileBuilder::default()
                .with_line_protocol(&lp)
                .with_min_time(0)
                .with_max_time(36000)
                .with_creation_time(time_now)
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
                .with_creation_time(time_2_minutes_ago)
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
            parquet_store: catalog.parquet_store.clone(),
            time_provider: Arc::<iox_time::MockProvider>::clone(&catalog.time_provider),
            exec: Arc::clone(&catalog.exec),
            backoff_config: BackoffConfig::default(),
            partition_concurrency: NonZeroUsize::new(1).unwrap(),
            partition_minute_threshold: PARTITION_MINUTE_THRESHOLD,
            max_desired_file_size_bytes: MAX_DESIRE_FILE_SIZE,
            percentage_max_file_size: PERCENTAGE_MAX_FILE_SIZE,
            split_percentage: SPLIT_PERCENTAGE,
        });

        Self {
            files: Arc::new(parquet_files),
            partition_info: candidate_partition,
            catalog,
            table,
            config,
        }
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
