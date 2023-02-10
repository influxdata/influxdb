//! Logic for finding relevant Parquet files in the catalog to be considered during a compaction
//! operation.

use data_types::{CompactionLevel, ParquetFileId, PartitionId, Timestamp};
use observability_deps::tracing::*;
use snafu::{ResultExt, Snafu};
use std::{collections::HashMap, fmt::Display, sync::Arc};

use crate::{
    compact::Compactor, parquet_file::CompactorParquetFile, PartitionCompactionCandidateWithInfo,
};

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub(crate) enum PartitionFilesFromPartitionError {
    #[snafu(display(
        "Error getting parquet files for partition {}: {}",
        partition_id,
        source
    ))]
    ListParquetFiles {
        partition_id: PartitionId,
        source: iox_catalog::interface::Error,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CompactionType {
    Hot,
    Warm,
    Cold,
}

impl Display for CompactionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Hot => write!(f, "hot"),
            Self::Warm => write!(f, "warm"),
            Self::Cold => write!(f, "cold"),
        }
    }
}

/// Collection of Parquet files relevant to compacting a partition. Separated by compaction level.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ParquetFilesForCompaction {
    /// Parquet files for a partition with `CompactionLevel::Initial`. Ordered by ascending max
    /// sequence number.
    pub(crate) level_0: Vec<CompactorParquetFile>,

    /// Parquet files for a partition with `CompactionLevel::FileNonOverlapped`. Ordered by
    /// ascending max sequence number.
    pub(crate) level_1: Vec<CompactorParquetFile>,

    /// Parquet files for a partition with `CompactionLevel::Final`. Arbitrary order.
    pub(crate) level_2: Vec<CompactorParquetFile>,
}

impl ParquetFilesForCompaction {
    /// Given a catalog and a partition ID, find the Parquet files in the catalog relevant to a
    /// compaction operation.
    pub(crate) async fn for_partition(
        compactor: Arc<Compactor>,
        partition: Arc<PartitionCompactionCandidateWithInfo>,
        compaction_type: CompactionType,
    ) -> Result<Option<Self>, PartitionFilesFromPartitionError> {
        Self::for_partition_with_size_overrides(
            compactor,
            partition,
            compaction_type,
            &Default::default(),
        )
        .await
    }

    /// Given a catalog and a partition ID, find the Parquet files in the catalog relevant to a
    /// compaction operation, also taking into account size overrides for testing purposes.
    ///
    /// Takes a hash-map `size_overrides` that mocks the size of the detected
    /// [`CompactorParquetFile`]s. This will influence the size calculation of the compactor (i.e.
    /// when/how to compact files), but leave the actual physical size of the file as it is (i.e.
    /// file deserialization can still rely on the original value).
    pub(crate) async fn for_partition_with_size_overrides(
        compactor: Arc<Compactor>,
        partition: Arc<PartitionCompactionCandidateWithInfo>,
        compaction_type: CompactionType,
        size_overrides: &HashMap<ParquetFileId, i64>,
    ) -> Result<Option<Self>, PartitionFilesFromPartitionError> {
        let partition_id = partition.id();
        debug!(
            partition_id = partition_id.get(),
            "finding parquet files for compaction"
        );

        let catalog = Arc::clone(&compactor.catalog);
        let min_num_rows_allocated_per_record_batch_to_datafusion_plan = compactor
            .config
            .min_num_rows_allocated_per_record_batch_to_datafusion_plan;
        let minutes_without_new_writes_to_be_cold =
            compactor.config.minutes_without_new_writes_to_be_cold;

        // List all valid (not soft deleted) files of the partition
        let parquet_files = catalog
            .repositories()
            .await
            .parquet_files()
            .list_by_partition_not_to_delete(partition_id)
            .await
            .context(ListParquetFilesSnafu { partition_id })?;

        let mut level_0 = Vec::with_capacity(parquet_files.len());
        let mut level_1 = Vec::with_capacity(parquet_files.len());
        let mut level_2 = Vec::with_capacity(parquet_files.len());

        if parquet_files.is_empty() {
            return Ok(None);
        }

        let mut count_small_l1 = 0;
        for parquet_file in parquet_files {
            match compaction_type {
                CompactionType::Warm => {
                    // Count number of small L1 files
                    if parquet_file.compaction_level == CompactionLevel::FileNonOverlapped
                        && parquet_file.file_size_bytes
                            < compactor.config.warm_compaction_small_size_threshold_bytes
                    {
                        count_small_l1 += 1;
                    }
                }
                CompactionType::Cold => {
                    // won't proceed if at least one L0 file created  after
                    // minutes_without_new_writes_to_be_cold
                    if parquet_file.compaction_level == CompactionLevel::Initial
                        && compaction_type == CompactionType::Cold
                        && parquet_file.created_at
                            > Timestamp::from(
                                compactor
                                    .time_provider
                                    .minutes_ago(minutes_without_new_writes_to_be_cold),
                            )
                    {
                        return Ok(None);
                    }
                }
                CompactionType::Hot => {}
            }

            // Estimate the bytes DataFusion needs when scan this file
            let estimated_arrow_bytes = partition
                .estimated_arrow_bytes(min_num_rows_allocated_per_record_batch_to_datafusion_plan);
            // Estimated bytes to store this file in memory
            // Since we ignore the size allocated in IOx and DF, let us make this large enough to cover those
            let estimated_bytes_to_store_in_memory = 5 * parquet_file.file_size_bytes as u64;
            let parquet_file = match size_overrides.get(&parquet_file.id) {
                Some(size) => CompactorParquetFile::new_with_size_override(
                    parquet_file,
                    estimated_arrow_bytes,
                    estimated_bytes_to_store_in_memory,
                    *size,
                ),
                None => CompactorParquetFile::new(
                    parquet_file,
                    estimated_arrow_bytes,
                    estimated_bytes_to_store_in_memory,
                ),
            };
            match parquet_file.compaction_level() {
                CompactionLevel::Initial => level_0.push(parquet_file),
                CompactionLevel::FileNonOverlapped => level_1.push(parquet_file),
                CompactionLevel::Final => level_2.push(parquet_file),
            }
        }

        // No L0 and L1, nothing to compact
        if level_0.is_empty() && level_1.is_empty() {
            return Ok(None);
        }

        match compaction_type {
            CompactionType::Hot => {
                // Only do hot compaction if there are L0 files
                if level_0.is_empty() {
                    return Ok(None);
                }
            }
            CompactionType::Warm => {
                // Only do warm compaction if there are certain small L1 files
                if count_small_l1 < compactor.config.warm_compaction_min_small_file_count {
                    return Ok(None);
                }
            }
            CompactionType::Cold => {}
        }

        level_0.sort_by_key(|pf| pf.created_at());
        level_1.sort_by_key(|pf| pf.min_time());

        Ok(Some(Self {
            level_0,
            level_1,
            level_2,
        }))
    }
}

#[cfg(test)]
mod tests {
    use crate::{compact::ShardAssignment, handler::CompactorConfig};

    use super::*;
    use backoff::BackoffConfig;
    use data_types::ColumnType;
    use iox_tests::{TestCatalog, TestParquetFileBuilder, TestPartition};
    use iox_time::{SystemProvider, TimeProvider};
    use parquet_file::storage::{ParquetStorage, StorageId};

    const DEFAULT_HOT_COMPACTION_HOURS_THRESHOLD_1: u64 = 4;
    const DEFAULT_HOT_COMPACTION_HOURS_THRESHOLD_2: u64 = 24;
    const DEFAULT_WARM_PARTITION_CANDIDATES_HOURS_THRESHOLD: u64 = 24;
    const DEFAULT_COLD_PARTITION_CANDIDATES_HOURS_THRESHOLD: u64 = 24;
    const DEFAULT_MAX_PARALLEL_PARTITIONS: u64 = 20;
    const DEFAULT_MINUTES_WITHOUT_NEW_WRITES: u64 = 8 * 60;
    const DEFAULT_MIN_ROWS_ALLOCATED: u64 = 100;

    const ARBITRARY_LINE_PROTOCOL: &str = r#"
        table,tag1=WA field_int=1000i 8000
        table,tag1=VT field_int=10i 10000
        table,tag1=UT field_int=70i 20000
    "#;

    struct TestSetup {
        compactor: Arc<Compactor>,
        partition: Arc<TestPartition>,
        partition_on_another_shard: Arc<TestPartition>,
        older_partition: Arc<TestPartition>,
    }

    async fn test_setup() -> TestSetup {
        let catalog = TestCatalog::new();
        let ns = catalog.create_namespace_1hr_retention("ns").await;
        let shard = ns.create_shard(1).await;
        let another_shard = ns.create_shard(2).await;
        let table = ns.create_table("table").await;
        table.create_column("field_int", ColumnType::I64).await;
        table.create_column("tag1", ColumnType::Tag).await;
        table.create_column("time", ColumnType::Time).await;

        let partition = table
            .with_shard(&shard)
            .create_partition("2022-07-13")
            .await;

        // Same partition key, but associated with a different shard
        let partition_on_another_shard = table
            .with_shard(&another_shard)
            .create_partition("2022-07-13")
            .await;

        // Same shard, but for an older partition key
        let older_partition = table
            .with_shard(&shard)
            .create_partition("2022-07-12")
            .await;

        let config = make_compactor_config();
        let metrics = Arc::new(metric::Registry::new());
        let compactor = Arc::new(Compactor::new(
            ShardAssignment::All,
            Arc::clone(&catalog.catalog),
            ParquetStorage::new(Arc::clone(&catalog.object_store), StorageId::from("iox")),
            catalog.exec(),
            Arc::new(SystemProvider::new()),
            BackoffConfig::default(),
            config,
            Arc::clone(&metrics),
        ));

        TestSetup {
            compactor,
            partition,
            partition_on_another_shard,
            older_partition,
        }
    }

    fn make_compactor_config() -> CompactorConfig {
        CompactorConfig {
            max_desired_file_size_bytes: 10_000,
            percentage_max_file_size: 30,
            split_percentage: 80,
            max_number_partitions_per_shard: 1,
            min_number_recent_ingested_files_per_partition: 1,
            hot_multiple: 4,
            warm_multiple: 1,
            memory_budget_bytes: 100_000_000,
            min_num_rows_allocated_per_record_batch_to_datafusion_plan: DEFAULT_MIN_ROWS_ALLOCATED,
            max_num_compacting_files: 20,
            max_num_compacting_files_first_in_partition: 40,
            minutes_without_new_writes_to_be_cold: DEFAULT_MINUTES_WITHOUT_NEW_WRITES,
            cold_partition_candidates_hours_threshold:
                DEFAULT_COLD_PARTITION_CANDIDATES_HOURS_THRESHOLD,
            hot_compaction_hours_threshold_1: DEFAULT_HOT_COMPACTION_HOURS_THRESHOLD_1,
            hot_compaction_hours_threshold_2: DEFAULT_HOT_COMPACTION_HOURS_THRESHOLD_2,
            max_parallel_partitions: DEFAULT_MAX_PARALLEL_PARTITIONS,
            warm_partition_candidates_hours_threshold:
                DEFAULT_WARM_PARTITION_CANDIDATES_HOURS_THRESHOLD,
            warm_compaction_small_size_threshold_bytes: 5_000,
            warm_compaction_min_small_file_count: 10,
        }
    }

    #[tokio::test]
    async fn no_relevant_parquet_files_returns_empty() {
        test_helpers::maybe_start_logging();
        let TestSetup {
            compactor,
            partition,
            partition_on_another_shard,
            older_partition,
        } = test_setup().await;

        // Create some files that shouldn't be returned:

        // - parquet file for another shard's partition
        let builder = TestParquetFileBuilder::default().with_line_protocol(ARBITRARY_LINE_PROTOCOL);
        partition_on_another_shard
            .create_parquet_file(builder)
            .await;

        // - parquet file for an older partition
        let builder = TestParquetFileBuilder::default().with_line_protocol(ARBITRARY_LINE_PROTOCOL);
        older_partition.create_parquet_file(builder).await;

        // - parquet file for this partition, level 0, marked to delete
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(ARBITRARY_LINE_PROTOCOL)
            .with_compaction_level(CompactionLevel::Initial)
            .with_to_delete(true);
        partition.create_parquet_file(builder).await;

        // - parquet file for this partition, level 1, marked to delete
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(ARBITRARY_LINE_PROTOCOL)
            .with_compaction_level(CompactionLevel::FileNonOverlapped)
            .with_to_delete(true);
        partition.create_parquet_file(builder).await;
        let partition_with_info =
            Arc::new(PartitionCompactionCandidateWithInfo::from_test_partition(&partition).await);

        let parquet_files_for_compaction = ParquetFilesForCompaction::for_partition(
            compactor,
            partition_with_info,
            CompactionType::Hot,
        )
        .await
        .unwrap();
        assert!(parquet_files_for_compaction.is_none());
    }

    #[tokio::test]
    async fn one_level_0_file_gets_returned() {
        test_helpers::maybe_start_logging();
        let TestSetup {
            compactor,
            partition,
            ..
        } = test_setup().await;

        // Create a level 0 file
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(ARBITRARY_LINE_PROTOCOL)
            .with_compaction_level(CompactionLevel::Initial);
        let parquet_file = partition.create_parquet_file(builder).await;

        let partition_with_info =
            Arc::new(PartitionCompactionCandidateWithInfo::from_test_partition(&partition).await);

        let parquet_files_for_compaction = ParquetFilesForCompaction::for_partition(
            compactor,
            partition_with_info,
            CompactionType::Hot,
        )
        .await
        .unwrap()
        .unwrap();

        let parquet_file_file_size_in_mem = 5 * parquet_file.parquet_file.file_size_bytes as u64;
        assert_eq!(
            parquet_files_for_compaction.level_0,
            vec![CompactorParquetFile::new(
                parquet_file.parquet_file,
                0,
                parquet_file_file_size_in_mem
            )]
        );

        assert!(
            parquet_files_for_compaction.level_1.is_empty(),
            "Expected empty, got: {:#?}",
            parquet_files_for_compaction.level_1
        );
    }

    #[tokio::test]
    async fn one_level_1_file_gets_returned() {
        test_helpers::maybe_start_logging();
        let TestSetup {
            compactor,
            partition,
            ..
        } = test_setup().await;

        // Create a level 1 file
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(ARBITRARY_LINE_PROTOCOL)
            .with_compaction_level(CompactionLevel::FileNonOverlapped);
        let parquet_file = partition.create_parquet_file(builder).await;

        let partition_with_info =
            Arc::new(PartitionCompactionCandidateWithInfo::from_test_partition(&partition).await);

        let parquet_files_for_compaction = ParquetFilesForCompaction::for_partition(
            compactor,
            partition_with_info,
            CompactionType::Cold,
        )
        .await
        .unwrap()
        .unwrap();

        assert!(
            parquet_files_for_compaction.level_0.is_empty(),
            "Expected empty, got: {:#?}",
            parquet_files_for_compaction.level_0
        );

        let parquet_file_file_size_in_mem = 5 * parquet_file.parquet_file.file_size_bytes as u64;
        assert_eq!(
            parquet_files_for_compaction.level_1,
            vec![CompactorParquetFile::new(
                parquet_file.parquet_file,
                0,
                parquet_file_file_size_in_mem
            )]
        );
    }

    #[tokio::test]
    async fn one_level_2_file_gets_returned() {
        test_helpers::maybe_start_logging();
        let TestSetup {
            compactor,
            partition,
            ..
        } = test_setup().await;

        // Create a level 2 file
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(ARBITRARY_LINE_PROTOCOL)
            .with_compaction_level(CompactionLevel::Final);
        let _parquet_file = partition.create_parquet_file(builder).await;

        let partition_with_info =
            Arc::new(PartitionCompactionCandidateWithInfo::from_test_partition(&partition).await);

        let parquet_files_for_compaction = ParquetFilesForCompaction::for_partition(
            Arc::clone(&compactor),
            partition_with_info,
            CompactionType::Hot,
        )
        .await
        .unwrap();
        // Only L2 files, nothing to compact
        assert!(parquet_files_for_compaction.is_none());
    }

    #[tokio::test]
    async fn one_file_of_each_level_gets_returned() {
        test_helpers::maybe_start_logging();
        let TestSetup {
            compactor,
            partition,
            ..
        } = test_setup().await;

        // Create a level 0 file
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(ARBITRARY_LINE_PROTOCOL)
            .with_compaction_level(CompactionLevel::Initial);
        let l0 = partition.create_parquet_file(builder).await;

        // Create a level 1 file
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(ARBITRARY_LINE_PROTOCOL)
            .with_compaction_level(CompactionLevel::FileNonOverlapped);
        let l1 = partition.create_parquet_file(builder).await;

        // Create a level 2 file
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(ARBITRARY_LINE_PROTOCOL)
            .with_compaction_level(CompactionLevel::Final);
        let l2 = partition.create_parquet_file(builder).await;

        let partition_with_info =
            Arc::new(PartitionCompactionCandidateWithInfo::from_test_partition(&partition).await);

        let parquet_files_for_compaction = ParquetFilesForCompaction::for_partition(
            compactor,
            partition_with_info,
            CompactionType::Hot,
        )
        .await
        .unwrap()
        .unwrap();

        let l0_file_size_in_mem = 5 * l0.parquet_file.file_size_bytes as u64;
        assert_eq!(
            parquet_files_for_compaction.level_0,
            vec![CompactorParquetFile::new(
                l0.parquet_file,
                0,
                l0_file_size_in_mem
            )]
        );

        let l1_file_size_in_mem = 5 * l1.parquet_file.file_size_bytes as u64;
        assert_eq!(
            parquet_files_for_compaction.level_1,
            vec![CompactorParquetFile::new(
                l1.parquet_file,
                0,
                l1_file_size_in_mem
            )]
        );

        let l2_file_size_in_mem = 5 * l2.parquet_file.file_size_bytes as u64;
        assert_eq!(
            parquet_files_for_compaction.level_2,
            vec![CompactorParquetFile::new(
                l2.parquet_file,
                0,
                l2_file_size_in_mem
            )]
        );
    }

    #[tokio::test]
    async fn level_0_files_are_sorted_on_created_at() {
        test_helpers::maybe_start_logging();
        let TestSetup {
            compactor,
            partition,
            ..
        } = test_setup().await;

        let time_provider = SystemProvider::new();
        let one_hour_ago = time_provider.hours_ago(1);
        let ten_munites_ago = time_provider.minutes_ago(10);

        // Create a level 0 file 10 minutes ago
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(ARBITRARY_LINE_PROTOCOL)
            .with_compaction_level(CompactionLevel::Initial)
            .with_creation_time(ten_munites_ago);
        let l0_ten_minutes_ago = partition.create_parquet_file(builder).await;

        // Create a level 0 file one hour ago
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(ARBITRARY_LINE_PROTOCOL)
            .with_compaction_level(CompactionLevel::Initial)
            .with_creation_time(one_hour_ago);
        let l0_one_hour_ago = partition.create_parquet_file(builder).await;

        // Create a level 1 file
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(ARBITRARY_LINE_PROTOCOL)
            .with_compaction_level(CompactionLevel::FileNonOverlapped);
        let l1 = partition.create_parquet_file(builder).await;

        let partition_with_info =
            Arc::new(PartitionCompactionCandidateWithInfo::from_test_partition(&partition).await);

        let parquet_files_for_compaction = ParquetFilesForCompaction::for_partition(
            compactor,
            partition_with_info,
            CompactionType::Hot,
        )
        .await
        .unwrap()
        .unwrap();

        let l0_ten_minutes_ago_file_size_in_mem =
            5 * l0_ten_minutes_ago.parquet_file.file_size_bytes as u64;
        let l0_one_hour_ago_file_size_in_mem =
            5 * l0_one_hour_ago.parquet_file.file_size_bytes as u64;
        let l1_file_size_in_mem = 5 * l1.parquet_file.file_size_bytes as u64;

        assert_eq!(
            parquet_files_for_compaction.level_0,
            vec![
                CompactorParquetFile::new(
                    l0_one_hour_ago.parquet_file,
                    0,
                    l0_one_hour_ago_file_size_in_mem
                ),
                CompactorParquetFile::new(
                    l0_ten_minutes_ago.parquet_file,
                    0,
                    l0_ten_minutes_ago_file_size_in_mem
                ),
            ]
        );

        assert_eq!(
            parquet_files_for_compaction.level_1,
            vec![CompactorParquetFile::new(
                l1.parquet_file,
                0,
                l1_file_size_in_mem
            )]
        );
    }

    #[tokio::test]
    async fn level_1_files_are_sorted_on_min_time() {
        test_helpers::maybe_start_logging();
        let TestSetup {
            compactor,
            partition,
            ..
        } = test_setup().await;

        // Create a level 1 file, max seq = 100, min time = 8888
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(ARBITRARY_LINE_PROTOCOL)
            .with_compaction_level(CompactionLevel::FileNonOverlapped)
            .with_max_seq(100)
            .with_min_time(8888);
        let l1_min_time_8888 = partition.create_parquet_file(builder).await;

        // Create a level 1 file, max seq = 50, min time = 7777
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(ARBITRARY_LINE_PROTOCOL)
            .with_compaction_level(CompactionLevel::FileNonOverlapped)
            .with_max_seq(50)
            .with_min_time(7777);
        let l1_min_time_7777 = partition.create_parquet_file(builder).await;

        // Create a level 1 file, max seq = 150, min time = 6666
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(ARBITRARY_LINE_PROTOCOL)
            .with_compaction_level(CompactionLevel::FileNonOverlapped)
            .with_max_seq(150)
            .with_min_time(6666);
        let l1_min_time_6666 = partition.create_parquet_file(builder).await;

        // Create a level 0 file
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(ARBITRARY_LINE_PROTOCOL)
            .with_compaction_level(CompactionLevel::Initial);
        let l0 = partition.create_parquet_file(builder).await;

        // Create a level 2 file
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(ARBITRARY_LINE_PROTOCOL)
            .with_compaction_level(CompactionLevel::Final);
        let l2 = partition.create_parquet_file(builder).await;

        let partition_with_info =
            Arc::new(PartitionCompactionCandidateWithInfo::from_test_partition(&partition).await);

        let parquet_files_for_compaction = ParquetFilesForCompaction::for_partition(
            compactor,
            partition_with_info,
            CompactionType::Hot,
        )
        .await
        .unwrap()
        .unwrap();

        let l0_file_size_in_mem = 5 * l0.parquet_file.file_size_bytes as u64;
        assert_eq!(
            parquet_files_for_compaction.level_0,
            vec![CompactorParquetFile::new(
                l0.parquet_file,
                0,
                l0_file_size_in_mem
            )]
        );

        let l1_min_time_6666_file_size_in_mem =
            5 * l1_min_time_6666.parquet_file.file_size_bytes as u64;
        let l1_min_time_7777_file_size_in_mem =
            5 * l1_min_time_7777.parquet_file.file_size_bytes as u64;
        let l1_min_time_8888_file_size_in_mem =
            5 * l1_min_time_8888.parquet_file.file_size_bytes as u64;
        assert_eq!(
            parquet_files_for_compaction.level_1,
            vec![
                CompactorParquetFile::new(
                    l1_min_time_6666.parquet_file,
                    0,
                    l1_min_time_6666_file_size_in_mem
                ),
                CompactorParquetFile::new(
                    l1_min_time_7777.parquet_file,
                    0,
                    l1_min_time_7777_file_size_in_mem
                ),
                CompactorParquetFile::new(
                    l1_min_time_8888.parquet_file,
                    0,
                    l1_min_time_8888_file_size_in_mem
                ),
            ]
        );

        let l2_file_size_in_mem = 5 * l2.parquet_file.file_size_bytes as u64;
        assert_eq!(
            parquet_files_for_compaction.level_2,
            vec![CompactorParquetFile::new(
                l2.parquet_file,
                0,
                l2_file_size_in_mem
            )]
        );
    }
}
