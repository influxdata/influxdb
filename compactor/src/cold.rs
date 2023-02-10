//! Compact partitions that are cold because they have not gotten writes recently and they're not
//! fully compacted.

use crate::{
    compact::Compactor,
    compact_candidates_with_memory_budget, compact_in_parallel, parquet_file_combining,
    parquet_file_lookup::{self, CompactionType},
    utils::get_candidates_with_retry,
};
use data_types::CompactionLevel;
use metric::Attributes;
use observability_deps::tracing::*;
use snafu::Snafu;
use std::sync::Arc;

/// Cold compaction. Returns the number of compacted partitions.
#[allow(dead_code)]
pub async fn compact(compactor: Arc<Compactor>, do_full_compact: bool) -> usize {
    let compaction_type = CompactionType::Cold;

    // https://github.com/influxdata/influxdb_iox/issues/6518 to remove the use of shard_id and
    // simplify this
    let max_num_partitions =
        compactor.shards.len() * compactor.config.max_number_partitions_per_shard;

    let cold_partition_candidates_hours_threshold =
        compactor.config.cold_partition_candidates_hours_threshold;
    let candidates = get_candidates_with_retry(
        Arc::clone(&compactor),
        compaction_type,
        move |compactor_for_retry| async move {
            compactor_for_retry
                .partitions_to_compact(
                    compaction_type,
                    vec![cold_partition_candidates_hours_threshold],
                    max_num_partitions,
                )
                .await
        },
    )
    .await;

    let n_candidates = candidates.len();
    if n_candidates == 0 {
        debug!(%compaction_type, "no compaction candidates found");
        return 0;
    } else {
        debug!(n_candidates, %compaction_type, "found compaction candidates");
    }

    let start_time = compactor.time_provider.now();

    debug!("Start cold compaction first step (L0+L1 -> L1)");

    // Compact any remaining level 0 files in parallel
    compact_candidates_with_memory_budget(
        Arc::clone(&compactor),
        CompactionType::Cold,
        CompactionLevel::Initial,
        CompactionLevel::FileNonOverlapped,
        compact_in_parallel,
        true, // split
        candidates.clone().into(),
    )
    .await;

    debug!("Finish cold compaction first step");
    debug!("Start cold compaction second step (L1+L2 -> L2)");

    if do_full_compact {
        //Compact level 1 files in parallel ("full compaction")
        compact_candidates_with_memory_budget(
            Arc::clone(&compactor),
            CompactionType::Cold,
            CompactionLevel::FileNonOverlapped,
            CompactionLevel::Final,
            compact_in_parallel,
            true, // split
            candidates.into(),
        )
        .await;
    }
    debug!("Finish cold compaction second step");

    // Done compacting all candidates in the cycle, record its time
    if let Some(delta) = compactor
        .time_provider
        .now()
        .checked_duration_since(start_time)
    {
        let attributes = Attributes::from([("partition_type", compaction_type.to_string().into())]);
        let duration = compactor.compaction_cycle_duration.recorder(attributes);
        duration.record(delta);
    }

    n_candidates
}

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub(crate) enum Error {
    #[snafu(display("{}", source))]
    Lookup {
        source: parquet_file_lookup::PartitionFilesFromPartitionError,
    },

    #[snafu(display("{}", source))]
    Combining {
        source: Box<parquet_file_combining::Error>,
    },

    #[snafu(display("{}", source))]
    Upgrading {
        source: iox_catalog::interface::Error,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        compact::ShardAssignment, compact_one_partition, handler::CompactorConfig,
        parquet_file_filtering, parquet_file_lookup::CompactionType, ParquetFilesForCompaction,
    };
    use ::parquet_file::storage::ParquetStorage;
    use arrow_util::assert_batches_sorted_eq;
    use backoff::BackoffConfig;
    use data_types::{ColumnType, CompactionLevel, ParquetFileId};
    use iox_tests::{TestCatalog, TestParquetFileBuilder, TestTable};
    use iox_time::{SystemProvider, TimeProvider};
    use parquet_file::storage::StorageId;
    use std::collections::HashMap;

    const DEFAULT_HOT_COMPACTION_HOURS_THRESHOLD_1: u64 = 4;
    const DEFAULT_HOT_COMPACTION_HOURS_THRESHOLD_2: u64 = 24;
    const DEFAULT_WARM_PARTITION_CANDIDATES_HOURS_THRESHOLD: u64 = 24;
    const DEFAULT_COLD_PARTITION_CANDIDATES_HOURS_THRESHOLD: u64 = 24;
    const MINUTE_WITHOUT_NEW_WRITE_TO_BE_COLD: u64 = 10;
    const DEFAULT_MAX_PARALLEL_PARTITIONS: u64 = 20;
    const DEFAULT_MAX_NUM_PARTITION_CANDIDATES: usize = 10;

    #[tokio::test]
    async fn test_compact_remaining_level_0_files_many_files() {
        test_helpers::maybe_start_logging();
        let catalog = TestCatalog::new();

        // lp1 does not overlap with any other level 0
        let lp1 = vec![
            "table,tag1=WA field_int=1000i 10",
            "table,tag1=VT field_int=10i 20",
        ]
        .join("\n");

        // lp2 overlaps with lp3
        let lp2 = vec![
            "table,tag1=WA field_int=1000i 8000", // will be eliminated due to duplicate
            "table,tag1=VT field_int=10i 10000",
            "table,tag1=UT field_int=70i 20000",
        ]
        .join("\n");

        // lp3 overlaps with lp2
        let lp3 = vec![
            "table,tag1=WA field_int=1500i 8000", // latest duplicate and kept
            "table,tag1=VT field_int=10i 6000",
            "table,tag1=UT field_int=270i 25000",
        ]
        .join("\n");

        // lp4 does not overlap with any
        let lp4 = vec![
            "table,tag2=WA,tag3=10 field_int=1600i 28000",
            "table,tag2=VT,tag3=20 field_int=20i 26000",
        ]
        .join("\n");

        // lp5 overlaps with lp1
        let lp5 = vec![
            "table,tag2=PA,tag3=15 field_int=1601i 9",
            "table,tag2=OH,tag3=21 field_int=21i 25",
        ]
        .join("\n");

        // lp6 does not overlap with any
        let lp6 = vec![
            "table,tag2=PA,tag3=15 field_int=81601i 90000",
            "table,tag2=OH,tag3=21 field_int=421i 91000",
        ]
        .join("\n");

        let ns = catalog.create_namespace_1hr_retention("ns").await;
        let shard = ns.create_shard(1).await;
        let table = ns.create_table("table").await;
        table.create_column("field_int", ColumnType::I64).await;
        table.create_column("tag1", ColumnType::Tag).await;
        table.create_column("tag2", ColumnType::Tag).await;
        table.create_column("tag3", ColumnType::Tag).await;
        table.create_column("time", ColumnType::Time).await;

        let partition = table.with_shard(&shard).create_partition("part").await;
        let time = Arc::new(SystemProvider::new());
        let time_five_hour_ago = time.hours_ago(5);
        let config = make_compactor_config();
        let metrics = Arc::new(metric::Registry::new());
        let compactor = Arc::new(Compactor::new(
            ShardAssignment::Only(vec![shard.shard.id]),
            Arc::clone(&catalog.catalog),
            ParquetStorage::new(Arc::clone(&catalog.object_store), StorageId::from("iox")),
            catalog.exec(),
            Arc::new(SystemProvider::new()),
            BackoffConfig::default(),
            config,
            Arc::clone(&metrics),
        ));

        // parquet files that are all in the same partition
        let mut size_overrides = HashMap::<ParquetFileId, i64>::default();

        let time_60_minutes_ago = time.minutes_ago(60);
        let time_50_minutes_ago = time.minutes_ago(50);
        let time_40_minutes_ago = time.minutes_ago(40);
        let time_30_minutes_ago = time.minutes_ago(30);
        let time_20_minutes_ago = time.minutes_ago(20);
        let time_11_minutes_ago = time.minutes_ago(11);

        // pf1 does not overlap with any other level 0
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp1)
            .with_creation_time(time_50_minutes_ago)
            .with_min_time(10)
            .with_max_time(20);
        let pf1_no_overlap = partition.create_parquet_file(builder).await;
        size_overrides.insert(
            pf1_no_overlap.parquet_file.id,
            compactor.config.max_desired_file_size_bytes as i64 + 10,
        );

        // pf2 overlaps with pf3
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp2)
            .with_creation_time(time_40_minutes_ago)
            .with_min_time(8_000)
            .with_max_time(20_000);
        let pf2 = partition.create_parquet_file(builder).await;
        size_overrides.insert(pf2.parquet_file.id, 100); // small file

        // pf3 overlaps with pf2
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp3)
            .with_creation_time(time_30_minutes_ago)
            .with_min_time(6_000)
            .with_max_time(25_000);
        let pf3 = partition.create_parquet_file(builder).await;
        size_overrides.insert(pf3.parquet_file.id, 100); // small file

        // pf4 does not overlap with any but is small
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp4)
            .with_creation_time(time_20_minutes_ago)
            .with_min_time(26_000)
            .with_max_time(28_000)
            .with_creation_time(time_five_hour_ago);
        let pf4 = partition.create_parquet_file(builder).await;
        size_overrides.insert(pf4.parquet_file.id, 100); // small file

        // pf5 was created in a previous compaction cycle; overlaps with pf1
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp5)
            .with_creation_time(time_60_minutes_ago)
            .with_min_time(9)
            .with_max_time(25)
            .with_compaction_level(CompactionLevel::FileNonOverlapped);
        let pf5 = partition.create_parquet_file(builder).await;
        size_overrides.insert(pf5.parquet_file.id, 100); // small file

        // pf6 was created in a previous compaction cycle; does not overlap with any
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp6)
            .with_creation_time(time_11_minutes_ago)
            .with_min_time(90000)
            .with_max_time(91000)
            .with_compaction_level(CompactionLevel::FileNonOverlapped);
        let pf6 = partition.create_parquet_file(builder).await;
        size_overrides.insert(pf6.parquet_file.id, 100); // small file

        // should have 4 level-0 files before compacting
        let count = catalog.count_level_0_files(shard.shard.id).await;
        assert_eq!(count, 4);

        // ------------------------------------------------
        // Compact
        let compaction_type = CompactionType::Cold;
        let hour_threshold = compactor.config.cold_partition_candidates_hours_threshold;
        let max_num_partitions = compactor.config.max_number_partitions_per_shard;
        let mut partition_candidates = compactor
            .partitions_to_compact(compaction_type, vec![hour_threshold], max_num_partitions)
            .await
            .unwrap();

        assert_eq!(partition_candidates.len(), 1);
        let partition = partition_candidates.pop().unwrap();

        let parquet_files_for_compaction =
            parquet_file_lookup::ParquetFilesForCompaction::for_partition_with_size_overrides(
                Arc::clone(&compactor),
                Arc::clone(&partition),
                CompactionType::Cold,
                &size_overrides,
            )
            .await
            .unwrap()
            .unwrap();

        let ParquetFilesForCompaction {
            level_0,
            level_1,
            .. // Ignore other levels
        } = parquet_files_for_compaction;

        let to_compact = parquet_file_filtering::filter_parquet_files(
            partition,
            level_0,
            level_1,
            compactor.config.memory_budget_bytes,
            compactor.config.max_num_compacting_files,
            compactor.config.max_num_compacting_files_first_in_partition,
            compactor.config.max_desired_file_size_bytes,
            &compactor.parquet_file_candidate_gauge,
            &compactor.parquet_file_candidate_bytes,
        );

        let to_compact = to_compact.into();

        compact_one_partition(&compactor, to_compact, CompactionType::Cold, true)
            .await
            .unwrap();

        // Should have 3 non-soft-deleted files:
        //
        // - pf6, the level 1 file that didn't overlap with anything
        // - the two newly created after compacting and splitting pf1, pf2, pf3, pf4, pf5
        let mut files = catalog.list_by_table_not_to_delete(table.table.id).await;
        assert_eq!(files.len(), 3);
        let files_and_levels: Vec<_> = files
            .iter()
            .map(|f| (f.id.get(), f.compaction_level))
            .collect();
        assert_eq!(
            files_and_levels,
            vec![
                (
                    pf6.parquet_file.id.get(),
                    CompactionLevel::FileNonOverlapped
                ),
                (7, CompactionLevel::FileNonOverlapped),
                (8, CompactionLevel::FileNonOverlapped),
            ]
        );

        // ------------------------------------------------
        // Verify the parquet file content

        // Data from pf3 and pf4, later times
        let file2 = files.pop().unwrap();
        let batches = table.read_parquet_file(file2).await;
        assert_batches_sorted_eq!(
            &[
                "+-----------+------+------+------+-----------------------------+",
                "| field_int | tag1 | tag2 | tag3 | time                        |",
                "+-----------+------+------+------+-----------------------------+",
                "| 1600      |      | WA   | 10   | 1970-01-01T00:00:00.000028Z |",
                "| 20        |      | VT   | 20   | 1970-01-01T00:00:00.000026Z |",
                "| 270       | UT   |      |      | 1970-01-01T00:00:00.000025Z |",
                "+-----------+------+------+------+-----------------------------+",
            ],
            &batches
        );

        // Data from pf1, pf2, pf3, and pf5, earlier times
        let file1 = files.pop().unwrap();
        let batches = table.read_parquet_file(file1).await;
        assert_batches_sorted_eq!(
            &[
                "+-----------+------+------+------+--------------------------------+",
                "| field_int | tag1 | tag2 | tag3 | time                           |",
                "+-----------+------+------+------+--------------------------------+",
                "| 10        | VT   |      |      | 1970-01-01T00:00:00.000000020Z |",
                "| 10        | VT   |      |      | 1970-01-01T00:00:00.000006Z    |",
                "| 10        | VT   |      |      | 1970-01-01T00:00:00.000010Z    |",
                "| 1000      | WA   |      |      | 1970-01-01T00:00:00.000000010Z |",
                "| 1500      | WA   |      |      | 1970-01-01T00:00:00.000008Z    |",
                "| 1601      |      | PA   | 15   | 1970-01-01T00:00:00.000000009Z |",
                "| 21        |      | OH   | 21   | 1970-01-01T00:00:00.000000025Z |",
                "| 70        | UT   |      |      | 1970-01-01T00:00:00.000020Z    |",
                "+-----------+------+------+------+--------------------------------+",
            ],
            &batches
        );

        // Data from pf6 that didn't overlap with anything, left unchanged
        let file0 = files.pop().unwrap();
        let batches = table.read_parquet_file(file0).await;
        assert_batches_sorted_eq!(
            &[
                "+-----------+------+------+-----------------------------+",
                "| field_int | tag2 | tag3 | time                        |",
                "+-----------+------+------+-----------------------------+",
                "| 421       | OH   | 21   | 1970-01-01T00:00:00.000091Z |",
                "| 81601     | PA   | 15   | 1970-01-01T00:00:00.000090Z |",
                "+-----------+------+------+-----------------------------+",
            ],
            &batches
        );
    }

    #[tokio::test]
    async fn test_compact_remaining_level_0_files_one_level_0_without_overlap() {
        test_helpers::maybe_start_logging();
        let catalog = TestCatalog::new();

        // lp1 does not overlap with any other level 0 or level 1
        let lp1 = vec![
            "table,tag1=WA field_int=1000i 10",
            "table,tag1=VT field_int=10i 20",
        ]
        .join("\n");

        // lp6 does not overlap with any
        let lp6 = vec![
            "table,tag2=PA,tag3=15 field_int=81601i 90000",
            "table,tag2=OH,tag3=21 field_int=421i 91000",
        ]
        .join("\n");

        let ns = catalog.create_namespace_1hr_retention("ns").await;
        let shard = ns.create_shard(1).await;
        let table = ns.create_table("table").await;
        table.create_column("field_int", ColumnType::I64).await;
        table.create_column("tag1", ColumnType::Tag).await;
        table.create_column("tag2", ColumnType::Tag).await;
        table.create_column("tag3", ColumnType::Tag).await;
        table.create_column("time", ColumnType::Time).await;

        let partition = table.with_shard(&shard).create_partition("part").await;
        let time = Arc::new(SystemProvider::new());
        let time_five_hour_ago = time.hours_ago(5);
        let config = make_compactor_config();
        let metrics = Arc::new(metric::Registry::new());
        let compactor = Arc::new(Compactor::new(
            ShardAssignment::Only(vec![shard.shard.id]),
            Arc::clone(&catalog.catalog),
            ParquetStorage::new(Arc::clone(&catalog.object_store), StorageId::from("iox")),
            catalog.exec(),
            Arc::new(SystemProvider::new()),
            BackoffConfig::default(),
            config,
            Arc::clone(&metrics),
        ));

        // parquet files that are all in the same partition
        let mut size_overrides = HashMap::<ParquetFileId, i64>::default();

        // pf1 does not overlap with any other level 0
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp1)
            .with_max_seq(3)
            .with_min_time(10)
            .with_max_time(20)
            .with_creation_time(time_five_hour_ago);
        let pf1 = partition.create_parquet_file(builder).await;
        size_overrides.insert(pf1.parquet_file.id, 100); // small file

        // pf6 was created in a previous compaction cycle; does not overlap with any
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp6)
            .with_max_seq(20)
            .with_min_time(90000)
            .with_max_time(91000)
            .with_creation_time(time_five_hour_ago)
            .with_compaction_level(CompactionLevel::FileNonOverlapped);
        let pf6 = partition.create_parquet_file(builder).await;
        size_overrides.insert(pf6.parquet_file.id, 100); // small file

        // should have 1 level-0 file before compacting
        let count = catalog.count_level_0_files(shard.shard.id).await;
        assert_eq!(count, 1);

        // ------------------------------------------------
        // Compact
        let compaction_type = CompactionType::Cold;
        let hour_threshold = compactor.config.cold_partition_candidates_hours_threshold;
        let max_num_partitions = compactor.config.max_number_partitions_per_shard;
        let mut partition_candidates = compactor
            .partitions_to_compact(compaction_type, vec![hour_threshold], max_num_partitions)
            .await
            .unwrap();

        assert_eq!(partition_candidates.len(), 1);
        let partition = partition_candidates.pop().unwrap();

        let parquet_files_for_compaction =
            parquet_file_lookup::ParquetFilesForCompaction::for_partition_with_size_overrides(
                Arc::clone(&compactor),
                Arc::clone(&partition),
                CompactionType::Cold,
                &size_overrides,
            )
            .await
            .unwrap()
            .unwrap();

        let ParquetFilesForCompaction {
            level_0,
            level_1,
            .. // Ignore other levels
        } = parquet_files_for_compaction;

        let to_compact = parquet_file_filtering::filter_parquet_files(
            Arc::clone(&partition),
            level_0,
            level_1,
            compactor.config.memory_budget_bytes,
            compactor.config.max_num_compacting_files,
            compactor.config.max_num_compacting_files_first_in_partition,
            compactor.config.max_desired_file_size_bytes,
            &compactor.parquet_file_candidate_gauge,
            &compactor.parquet_file_candidate_bytes,
        );

        let to_compact = to_compact.into();

        compact_one_partition(&compactor, to_compact, CompactionType::Cold, true)
            .await
            .unwrap();

        // Should have 2 non-soft-deleted files:
        //
        // - pf1, the newly created level 1 file that was only upgraded from level 0
        // - pf6, the level 1 file that didn't overlap with anything
        let mut files = catalog.list_by_table_not_to_delete(table.table.id).await;
        assert_eq!(files.len(), 2);
        let files_and_levels: Vec<_> = files
            .iter()
            .map(|f| (f.id.get(), f.compaction_level))
            .collect();
        assert_eq!(
            files_and_levels,
            vec![
                (
                    pf1.parquet_file.id.get(),
                    CompactionLevel::FileNonOverlapped
                ),
                (
                    pf6.parquet_file.id.get(),
                    CompactionLevel::FileNonOverlapped
                ),
            ]
        );

        // ------------------------------------------------
        // Verify the parquet file content

        // pf6
        let file1 = files.pop().unwrap();
        let batches = table.read_parquet_file(file1).await;
        assert_batches_sorted_eq!(
            &[
                "+-----------+------+------+-----------------------------+",
                "| field_int | tag2 | tag3 | time                        |",
                "+-----------+------+------+-----------------------------+",
                "| 421       | OH   | 21   | 1970-01-01T00:00:00.000091Z |",
                "| 81601     | PA   | 15   | 1970-01-01T00:00:00.000090Z |",
                "+-----------+------+------+-----------------------------+",
            ],
            &batches
        );

        // pf1
        let file0 = files.pop().unwrap();
        let batches = table.read_parquet_file(file0).await;
        assert_batches_sorted_eq!(
            &[
                "+-----------+------+--------------------------------+",
                "| field_int | tag1 | time                           |",
                "+-----------+------+--------------------------------+",
                "| 10        | VT   | 1970-01-01T00:00:00.000000020Z |",
                "| 1000      | WA   | 1970-01-01T00:00:00.000000010Z |",
                "+-----------+------+--------------------------------+",
            ],
            &batches
        );

        // Full compaction will now combine the two level 1 files into one level 2 file
        let parquet_files_for_compaction =
            parquet_file_lookup::ParquetFilesForCompaction::for_partition_with_size_overrides(
                Arc::clone(&compactor),
                Arc::clone(&partition),
                CompactionType::Cold,
                &size_overrides,
            )
            .await
            .unwrap()
            .unwrap();

        let ParquetFilesForCompaction {
            level_1,
            level_2,
            .. // Ignore other levels
        } = parquet_files_for_compaction;

        let to_compact = parquet_file_filtering::filter_parquet_files(
            Arc::clone(&partition),
            level_1,
            level_2,
            compactor.config.memory_budget_bytes,
            compactor.config.max_num_compacting_files,
            compactor.config.max_num_compacting_files_first_in_partition,
            compactor.config.max_desired_file_size_bytes,
            &compactor.parquet_file_candidate_gauge,
            &compactor.parquet_file_candidate_bytes,
        );

        let to_compact = to_compact.into();

        compact_one_partition(&compactor, to_compact, CompactionType::Cold, false)
            .await
            .unwrap();

        let mut files = catalog.list_by_table_not_to_delete(table.table.id).await;
        assert_eq!(files.len(), 1);
        let file = files.pop().unwrap();
        assert_eq!(file.id.get(), 3);
        assert_eq!(file.compaction_level, CompactionLevel::Final);

        // ------------------------------------------------
        // Verify the parquet file content

        let batches = table.read_parquet_file(file).await;
        assert_batches_sorted_eq!(
            &[
                "+-----------+------+------+------+--------------------------------+",
                "| field_int | tag1 | tag2 | tag3 | time                           |",
                "+-----------+------+------+------+--------------------------------+",
                "| 10        | VT   |      |      | 1970-01-01T00:00:00.000000020Z |",
                "| 1000      | WA   |      |      | 1970-01-01T00:00:00.000000010Z |",
                "| 421       |      | OH   | 21   | 1970-01-01T00:00:00.000091Z    |",
                "| 81601     |      | PA   | 15   | 1970-01-01T00:00:00.000090Z    |",
                "+-----------+------+------+------+--------------------------------+",
            ],
            &batches
        );
    }

    #[tokio::test]
    async fn test_full_cold_compaction_upgrades_one_level_0() {
        test_helpers::maybe_start_logging();
        let catalog = TestCatalog::new();

        // Create one cold level 0 file that will get upgraded to level 1 then upgraded to level 2
        let lp1 = vec![
            "table,tag1=WA field_int=1000i 10",
            "table,tag1=VT field_int=10i 20",
        ]
        .join("\n");

        let ns = catalog.create_namespace_1hr_retention("ns").await;
        let shard = ns.create_shard(1).await;
        let table = ns.create_table("table").await;
        table.create_column("field_int", ColumnType::I64).await;
        table.create_column("tag1", ColumnType::Tag).await;
        table.create_column("time", ColumnType::Time).await;
        let partition = table.with_shard(&shard).create_partition("part").await;
        let time = Arc::new(SystemProvider::new());
        let time_five_hour_ago = time.hours_ago(5);
        let config = make_compactor_config();
        let metrics = Arc::new(metric::Registry::new());
        let compactor = Arc::new(Compactor::new(
            ShardAssignment::Only(vec![shard.shard.id]),
            Arc::clone(&catalog.catalog),
            ParquetStorage::new(Arc::clone(&catalog.object_store), StorageId::from("iox")),
            catalog.exec(),
            Arc::new(SystemProvider::new()),
            BackoffConfig::default(),
            config,
            Arc::clone(&metrics),
        ));

        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp1)
            .with_creation_time(time_five_hour_ago);
        partition.create_parquet_file(builder).await;

        // should have 1 level-0 file before compacting
        let count = catalog.count_level_0_files(shard.shard.id).await;
        assert_eq!(count, 1);

        // ------------------------------------------------
        // Compact
        compact(compactor, true).await;

        // Should have 1 non-soft-deleted files:
        //
        // - the newly created file that was upgraded to level 1 then to level 2
        let mut files = catalog.list_by_table_not_to_delete(table.table.id).await;
        assert_eq!(files.len(), 1);
        let file = files.pop().unwrap();
        assert_eq!(file.id.get(), 1); // ID doesn't change because the file doesn't get rewritten
        assert_eq!(file.compaction_level, CompactionLevel::Final);

        // ------------------------------------------------
        // Verify the parquet file content

        let batches = table.read_parquet_file(file).await;
        assert_batches_sorted_eq!(
            &[
                "+-----------+------+--------------------------------+",
                "| field_int | tag1 | time                           |",
                "+-----------+------+--------------------------------+",
                "| 10        | VT   | 1970-01-01T00:00:00.000000020Z |",
                "| 1000      | WA   | 1970-01-01T00:00:00.000000010Z |",
                "+-----------+------+--------------------------------+",
            ],
            &batches
        );
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
            min_num_rows_allocated_per_record_batch_to_datafusion_plan: 1,
            max_num_compacting_files: 20,
            max_num_compacting_files_first_in_partition: 40,
            minutes_without_new_writes_to_be_cold: MINUTE_WITHOUT_NEW_WRITE_TO_BE_COLD,
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
    async fn cold_compaction_first_step_no_split() {
        test_helpers::maybe_start_logging();
        let TestDb {
            catalog,
            compactor,
            table,
        } = make_db_with_4_l0s_2_l1s().await;

        // Let do cold compaction first step
        //
        // Select partition candidates. Must be one becasue all 6 files belong to the same partition
        let compaction_type = CompactionType::Cold;
        let hour_threshold = compactor.config.cold_partition_candidates_hours_threshold;
        let candidates = compactor
            .partitions_to_compact(
                compaction_type,
                vec![hour_threshold],
                DEFAULT_MAX_NUM_PARTITION_CANDIDATES,
            )
            .await
            .unwrap();
        assert_eq!(candidates.len(), 1);
        //
        // Cold compaction first step
        compact_candidates_with_memory_budget(
            Arc::clone(&compactor),
            CompactionType::Cold,
            CompactionLevel::Initial,
            CompactionLevel::FileNonOverlapped,
            compact_in_parallel,
            false, // no split
            candidates.clone().into(),
        )
        .await;

        // Should have 2 non-soft-deleted L-1 files:
        //   . level-1 pf6 with is not compacted because it does not overlap with any L0s
        //   . new compacted level-1 file as a result of compacting 4 L0s (pf1, pf2, pf3, pf4) and one L1 (pf5)
        let files = catalog.list_by_table_not_to_delete(table.table.id).await;
        assert_eq!(files.len(), 2, "{files:?}");
        let files_and_levels: Vec<_> = files
            .iter()
            .map(|f| (f.id.get(), f.compaction_level))
            .collect();

        // The initial files and their IDs are: L0 1-4, L1 5-6. The first step of cold compaction took files 1-5
        // and compacted them into level-1 with next ID 7
        assert_eq!(
            files_and_levels,
            vec![
                (6, CompactionLevel::FileNonOverlapped),
                (7, CompactionLevel::FileNonOverlapped)
            ]
        );
    }

    #[tokio::test]
    async fn cold_compaction_first_step_split() {
        test_helpers::maybe_start_logging();

        let TestDb {
            catalog,
            compactor,
            table,
        } = make_db_with_4_l0s_2_l1s().await;

        // Let do cold compaction first step
        //
        // Select partition candidates. Must be one becasue all 6 files belong to the same partition
        let compaction_type = CompactionType::Cold;
        let hour_threshold = compactor.config.cold_partition_candidates_hours_threshold;
        let candidates = compactor
            .partitions_to_compact(
                compaction_type,
                vec![hour_threshold],
                DEFAULT_MAX_NUM_PARTITION_CANDIDATES,
            )
            .await
            .unwrap();
        assert_eq!(candidates.len(), 1);
        //
        // Cold compaction first step
        compact_candidates_with_memory_budget(
            Arc::clone(&compactor),
            CompactionType::Cold,
            CompactionLevel::Initial,
            CompactionLevel::FileNonOverlapped,
            compact_in_parallel,
            true, // split
            candidates.clone().into(),
        )
        .await;

        // Should have 3 non-soft-deleted L-1 files:
        //   . level-1 pf6 is not compacted because it does not overlap with any L0s
        //   . Two new compacted level-1 files as a result of compacting 4 L0s (pf1, pf2, pf3, pf4) and one L1 (pf5)
        //     and split the output into 2 files
        let files = catalog.list_by_table_not_to_delete(table.table.id).await;
        assert_eq!(files.len(), 3, "{files:?}");
        let files_and_levels: Vec<_> = files
            .iter()
            .map(|f| (f.id.get(), f.compaction_level))
            .collect();

        // The initial files and their IDs are: L0 1-4, L1 5-6. The first step of cold compaction took files 1-5
        // and compacted them into 2 level-1 with IDs 7 and 8
        assert_eq!(
            files_and_levels,
            vec![
                (6, CompactionLevel::FileNonOverlapped),
                (7, CompactionLevel::FileNonOverlapped),
                (8, CompactionLevel::FileNonOverlapped)
            ]
        );
    }

    #[tokio::test]
    async fn full_cold_compaction_many_files() {
        test_helpers::maybe_start_logging();

        let TestDb {
            catalog,
            compactor,
            table,
        } = make_db_with_4_l0s_2_l1s().await;

        // ------------------------------------------------
        // Compact

        compact(compactor, true).await;

        // Should have 2 non-soft-deleted file:
        //
        // - the 2 level-2 files created after combining all 3 level 1 files created by the first step
        //   of compaction to compact remaining level 0 files
        let mut files = catalog.list_by_table_not_to_delete(table.table.id).await;
        assert_eq!(files.len(), 2, "{files:?}");
        let files_and_levels: Vec<_> = files
            .iter()
            .map(|f| (f.id.get(), f.compaction_level))
            .collect();

        // The initial files are: L0 1-4, L1 5-6. The first step of cold compaction took files 1-5
        // and compacted them into two l-1 files 7, 8. The second step of cold compaction
        // took 6, 7, and 8 and combined them all into two files 9 and 10.
        assert_eq!(
            files_and_levels,
            vec![(9, CompactionLevel::Final), (10, CompactionLevel::Final)]
        );

        // ------------------------------------------------
        // Verify the parquet file content
        // first file:
        let file = files.pop().unwrap();
        let batches = table.read_parquet_file(file).await;
        assert_batches_sorted_eq!(
            &[
                "+-----------+------+------+------+-----------------------------+",
                "| field_int | tag1 | tag2 | tag3 | time                        |",
                "+-----------+------+------+------+-----------------------------+",
                "| 421       |      | OH   | 21   | 1970-01-01T00:00:00.000091Z |",
                "| 81601     |      | PA   | 15   | 1970-01-01T00:00:00.000090Z |",
                "+-----------+------+------+------+-----------------------------+",
            ],
            &batches
        );
        // second file
        let file = files.pop().unwrap();
        let batches = table.read_parquet_file(file).await;
        assert_batches_sorted_eq!(
            &[
                "+-----------+------+------+------+--------------------------------+",
                "| field_int | tag1 | tag2 | tag3 | time                           |",
                "+-----------+------+------+------+--------------------------------+",
                "| 10        | VT   |      |      | 1970-01-01T00:00:00.000000020Z |",
                "| 10        | VT   |      |      | 1970-01-01T00:00:00.000006Z    |",
                "| 10        | VT   |      |      | 1970-01-01T00:00:00.000010Z    |",
                "| 1000      | WA   |      |      | 1970-01-01T00:00:00.000000010Z |",
                "| 1500      | WA   |      |      | 1970-01-01T00:00:00.000008Z    |",
                "| 1600      |      | WA   | 10   | 1970-01-01T00:00:00.000028Z    |",
                "| 1601      |      | PA   | 15   | 1970-01-01T00:00:00.000000009Z |",
                "| 20        |      | VT   | 20   | 1970-01-01T00:00:00.000026Z    |",
                "| 21        |      | OH   | 21   | 1970-01-01T00:00:00.000000025Z |",
                "| 270       | UT   |      |      | 1970-01-01T00:00:00.000025Z    |",
                "| 70        | UT   |      |      | 1970-01-01T00:00:00.000020Z    |",
                "+-----------+------+------+------+--------------------------------+",
            ],
            &batches
        );
    }

    #[tokio::test]
    async fn one_level_1_file_takes_up_memory_budget_gets_upgraded() {
        test_helpers::maybe_start_logging();

        let catalog = TestCatalog::new();

        // lp1 will be level 1 with min time 26000, no overlaps
        let lp1 = vec![
            "table,tag2=WA,tag3=10 field_int=1600i 28000",
            "table,tag2=VT,tag3=20 field_int=20i 26000",
        ]
        .join("\n");

        // lp2 will be level 1 with min time 90000, no overlaps
        let lp2 = vec![
            "table,tag2=PA,tag3=15 field_int=81601i 90000",
            "table,tag2=OH,tag3=21 field_int=421i 91000",
        ]
        .join("\n");

        let ns = catalog.create_namespace_1hr_retention("ns").await;
        let shard = ns.create_shard(1).await;
        let table = ns.create_table("table").await;
        table.create_column("field_int", ColumnType::I64).await;
        table.create_column("tag1", ColumnType::Tag).await;
        table.create_column("tag2", ColumnType::Tag).await;
        table.create_column("tag3", ColumnType::Tag).await;
        table.create_column("time", ColumnType::Time).await;

        let partition = table.with_shard(&shard).create_partition("part").await;
        let time = Arc::new(SystemProvider::new());
        let time_five_hour_ago = time.hours_ago(5);
        let mut config = make_compactor_config();

        // Set the memory budget such that only one of the files will be compacted in a group
        config.memory_budget_bytes = 20_000;

        let metrics = Arc::new(metric::Registry::new());
        let compactor = Arc::new(Compactor::new(
            ShardAssignment::Only(vec![shard.shard.id]),
            Arc::clone(&catalog.catalog),
            ParquetStorage::new(Arc::clone(&catalog.object_store), StorageId::from("iox")),
            catalog.exec(),
            Arc::new(SystemProvider::new()),
            BackoffConfig::default(),
            config,
            Arc::clone(&metrics),
        ));

        // pf1, L1
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp1)
            .with_max_seq(1)
            .with_min_time(26_000)
            .with_max_time(28_000)
            .with_creation_time(time_five_hour_ago)
            .with_compaction_level(CompactionLevel::FileNonOverlapped);
        // pf1 file size: 2183, estimated file bytes: 4590
        let pf1 = partition.create_parquet_file(builder).await;
        println!("=== pf1 file size: {:#?}", pf1.parquet_file.file_size_bytes);

        // pf2, L1
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp2)
            .with_max_seq(20)
            .with_min_time(90000)
            .with_max_time(91000)
            .with_creation_time(time_five_hour_ago)
            .with_compaction_level(CompactionLevel::FileNonOverlapped);
        // pf2 file size: 2183
        let pf2 = partition.create_parquet_file(builder).await;
        println!("=== pf2 file size: {:#?}", pf2.parquet_file.file_size_bytes);

        // ------------------------------------------------
        // Compact

        // The first time through, the first file will get upgraded by itself because only the
        // one file will fit in the memory budget.
        compact(Arc::clone(&compactor), true).await;
        // Then when the partition is selected for compaction again, the second file will get
        // upgraded by itself.
        compact(compactor, true).await;

        let files = catalog.list_by_table_not_to_delete(table.table.id).await;
        assert_eq!(files.len(), 2, "{files:?}");
        let files_and_levels: Vec<_> = files
            .iter()
            .map(|f| (f.id.get(), f.compaction_level))
            .collect();

        // Both files get upgraded from level 1 to level 2 without actually doing compaction,
        // so their IDs stay the same.
        assert_eq!(
            files_and_levels,
            vec![(1, CompactionLevel::Final), (2, CompactionLevel::Final),]
        );
    }

    #[tokio::test]
    async fn level_1_files_limited_by_estimated_size() {
        test_helpers::maybe_start_logging();

        let catalog = TestCatalog::new();

        // lp1 will be level 1 with min time 26000, no overlaps
        let lp1 = vec![
            "table,tag2=WA,tag3=10 field_int=1600i 28000",
            "table,tag2=VT,tag3=20 field_int=20i 26000",
        ]
        .join("\n");

        // lp2 will be level 1 with min time 90000, no overlaps
        let lp2 = vec![
            "table,tag2=PA,tag3=15 field_int=81601i 90000",
            "table,tag2=OH,tag3=21 field_int=421i 91000",
        ]
        .join("\n");

        // lp3 will be level 1 with min time 350000, no overlaps
        let lp3 = vec![
            "table,tag2=ND,tag3=8 field_int=333i 350000",
            "table,tag2=SD,tag3=207 field_int=999i 360000",
        ]
        .join("\n");

        let ns = catalog.create_namespace_1hr_retention("ns").await;
        let shard = ns.create_shard(1).await;
        let table = ns.create_table("table").await;
        table.create_column("field_int", ColumnType::I64).await;
        table.create_column("tag1", ColumnType::Tag).await;
        table.create_column("tag2", ColumnType::Tag).await;
        table.create_column("tag3", ColumnType::Tag).await;
        table.create_column("time", ColumnType::Time).await;

        let partition = table.with_shard(&shard).create_partition("part").await;
        let time = Arc::new(SystemProvider::new());
        let time_five_hour_ago = time.hours_ago(5);
        let mut config = make_compactor_config();

        // Set the memory budget such that two of the files will be compacted in a group
        config.memory_budget_bytes = 30_000;

        let metrics = Arc::new(metric::Registry::new());
        let compactor = Arc::new(Compactor::new(
            ShardAssignment::Only(vec![shard.shard.id]),
            Arc::clone(&catalog.catalog),
            ParquetStorage::new(Arc::clone(&catalog.object_store), StorageId::from("iox")),
            catalog.exec(),
            Arc::new(SystemProvider::new()),
            BackoffConfig::default(),
            config,
            Arc::clone(&metrics),
        ));

        // pf1, L1
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp1)
            .with_max_seq(1)
            .with_min_time(26_000)
            .with_max_time(28_000)
            .with_creation_time(time_five_hour_ago)
            .with_compaction_level(CompactionLevel::FileNonOverlapped);
        // pf1 file size: 2183, estimated file bytes: 4590
        let pf1 = partition.create_parquet_file(builder).await;
        println!("=== pf1 file size: {:#?}", pf1.parquet_file.file_size_bytes);

        // pf2, L1
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp2)
            .with_max_seq(20)
            .with_min_time(90_000)
            .with_max_time(91_000)
            .with_creation_time(time_five_hour_ago)
            .with_compaction_level(CompactionLevel::FileNonOverlapped);
        // pf2 file size: 2183, estimated file bytes: 4590
        let pf2 = partition.create_parquet_file(builder).await;
        println!("=== pf2 file size: {:#?}", pf2.parquet_file.file_size_bytes);

        // pf3, L1
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp3)
            .with_max_seq(25)
            .with_min_time(350_000)
            .with_max_time(360_000)
            .with_creation_time(time_five_hour_ago)
            .with_compaction_level(CompactionLevel::FileNonOverlapped);
        // pf2 file size: 2183, estimated file bytes: 4590
        let pf3 = partition.create_parquet_file(builder).await;
        println!("=== pf3 file size: {:#?}", pf3.parquet_file.file_size_bytes);

        // ------------------------------------------------
        // Compact

        compact(compactor, true).await;

        let files = catalog.list_by_table_not_to_delete(table.table.id).await;
        assert_eq!(files.len(), 3, "{files:?}");
        let files_and_levels: Vec<_> = files
            .iter()
            .map(|f| (f.id.get(), f.compaction_level))
            .collect();

        // Compaction will select files 1 and 2 because adding file 3 would go over the memory
        // budget. File 3 remains untouched. Files 1 and 2 are compacted and then split into files
        // 4 and 5 at level 2.
        assert_eq!(
            files_and_levels,
            vec![
                (3, CompactionLevel::FileNonOverlapped),
                (4, CompactionLevel::Final),
                (5, CompactionLevel::Final),
            ]
        );
    }

    #[tokio::test]
    async fn full_cold_compaction_new_level_1_overlapping_with_level_2() {
        test_helpers::maybe_start_logging();

        let catalog = TestCatalog::new();

        // lp1 will be level 1 with min time 10, overlaps with lp5 (L2)
        let lp1 = vec![
            "table,tag1=WA field_int=1000i 10",
            "table,tag1=VT field_int=10i 20",
        ]
        .join("\n");

        // lp2 will be level 2 with min time 8000, overlaps with lp3 (L1)
        let lp2 = vec![
            "table,tag1=WA field_int=1000i 8000", // will be eliminated due to duplicate with l1
            "table,tag1=VT field_int=10i 10000",
            "table,tag1=UT field_int=70i 20000",
        ]
        .join("\n");

        // lp3 will be level 1 with min time 6000, overlaps with lp2 (L2)
        let lp3 = vec![
            "table,tag1=WA field_int=1500i 8000", // latest duplicate and kept
            "table,tag1=VT field_int=10i 6000",
            "table,tag1=UT field_int=270i 25000",
        ]
        .join("\n");

        // lp4 will be level 1 with min time 26000, no overlaps
        let lp4 = vec![
            "table,tag2=WA,tag3=10 field_int=1600i 28000",
            "table,tag2=VT,tag3=20 field_int=20i 26000",
        ]
        .join("\n");

        // lp5 will be level 2 with min time 21, overlaps with lp1 (L1)
        let lp5 = vec![
            "table,tag2=PA,tag3=15 field_int=1601i 9",
            "table,tag2=OH,tag3=21 field_int=21i 25",
        ]
        .join("\n");

        // lp6 will be level 2, no overlaps
        let lp6 = vec![
            "table,tag2=PA,tag3=15 field_int=81601i 90000",
            "table,tag2=OH,tag3=21 field_int=421i 91000",
        ]
        .join("\n");

        let ns = catalog.create_namespace_1hr_retention("ns").await;
        let shard = ns.create_shard(1).await;
        let table = ns.create_table("table").await;
        table.create_column("field_int", ColumnType::I64).await;
        table.create_column("tag1", ColumnType::Tag).await;
        table.create_column("tag2", ColumnType::Tag).await;
        table.create_column("tag3", ColumnType::Tag).await;
        table.create_column("time", ColumnType::Time).await;

        let partition = table.with_shard(&shard).create_partition("part").await;
        let time = Arc::new(SystemProvider::new());
        let time_five_hour_ago = time.hours_ago(5);
        let mut config = make_compactor_config();

        // Set the memory budget such that only some of the files will be compacted in a group
        config.memory_budget_bytes = 40000;

        let metrics = Arc::new(metric::Registry::new());
        let compactor = Arc::new(Compactor::new(
            ShardAssignment::Only(vec![shard.shard.id]),
            Arc::clone(&catalog.catalog),
            ParquetStorage::new(Arc::clone(&catalog.object_store), StorageId::from("iox")),
            catalog.exec(),
            Arc::new(SystemProvider::new()),
            BackoffConfig::default(),
            config,
            Arc::clone(&metrics),
        ));

        // pf1, L1, overlaps with lp5 (L2)
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp1)
            .with_max_seq(2)
            .with_min_time(10)
            .with_max_time(20)
            .with_creation_time(time_five_hour_ago)
            .with_compaction_level(CompactionLevel::FileNonOverlapped);
        // p1 file size: 1757
        let pf1 = partition.create_parquet_file(builder).await;
        println!("=== p1 file size: {:#?}", pf1.parquet_file.file_size_bytes);

        // pf2, L2, overlaps with lp3 (L1)
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp2)
            .with_max_seq(5)
            .with_min_time(8_000)
            .with_max_time(20_000)
            .with_creation_time(time_five_hour_ago)
            .with_compaction_level(CompactionLevel::Final);
        // p2 file size: 1777
        let pf2 = partition.create_parquet_file(builder).await;
        println!("=== p2 file size: {:#?}", pf2.parquet_file.file_size_bytes);

        // pf3, L1, overlaps with lp2 (L2)
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp3)
            .with_max_seq(3)
            .with_min_time(6_000)
            .with_max_time(25_000)
            .with_creation_time(time_five_hour_ago)
            .with_compaction_level(CompactionLevel::FileNonOverlapped);
        // p3 file size: 1777
        let pf3 = partition.create_parquet_file(builder).await;
        println!("=== p3 file size: {:#?}", pf3.parquet_file.file_size_bytes);

        // pf4, L1, does not overlap with any, won't fit in budget with 1, 2, 3, 5
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp4)
            .with_max_seq(1)
            .with_min_time(26_000)
            .with_max_time(28_000)
            .with_creation_time(time_five_hour_ago)
            .with_compaction_level(CompactionLevel::FileNonOverlapped);
        // p4 file size: 2183
        let pf4 = partition.create_parquet_file(builder).await;
        println!("=== p4 file size: {:#?}", pf4.parquet_file.file_size_bytes);

        // pf5, L2, overlaps with lp1 (L1)
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp5)
            .with_max_seq(1)
            .with_min_time(9)
            .with_max_time(25)
            .with_creation_time(time_five_hour_ago)
            .with_compaction_level(CompactionLevel::Final);
        // p5 file size: 2183
        let pf5 = partition.create_parquet_file(builder).await;
        println!("=== p5 file size: {:#?}", pf5.parquet_file.file_size_bytes);

        // pf6, L2, does not overlap with any
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp6)
            .with_max_seq(20)
            .with_min_time(90000)
            .with_max_time(91000)
            .with_creation_time(time_five_hour_ago)
            .with_compaction_level(CompactionLevel::Final);
        // p6 file size: 2183
        let pf6 = partition.create_parquet_file(builder).await;
        println!("=== p6 file size: {:#?}", pf6.parquet_file.file_size_bytes);

        // ------------------------------------------------
        // Compact

        compact(compactor, true).await;

        // Should have 4 non-soft-deleted files:
        //
        // - pf4, the level 1 file untouched because it didn't fit in the memory budget
        // - pf6, the level 2 file untouched because it doesn't overlap anything
        // - two level-2 files created after combining all 3 level 1 files created by the first step
        //   of compaction to compact remaining level 0 files
        let mut files = catalog.list_by_table_not_to_delete(table.table.id).await;
        assert_eq!(files.len(), 4, "{files:?}");
        let files_and_levels: Vec<_> = files
            .iter()
            .map(|f| (f.id.get(), f.compaction_level))
            .collect();

        // File 4 was L1 but didn't fit in the memory budget, so was untouched.
        // File 6 was already L2 and did not overlap with anything, so was untouched.
        // Cold compaction took files 1, 2, 3, 5 and compacted them into 2 files 7 and 8.
        assert_eq!(
            files_and_levels,
            vec![
                (4, CompactionLevel::FileNonOverlapped),
                (6, CompactionLevel::Final),
                (7, CompactionLevel::Final),
                (8, CompactionLevel::Final),
            ]
        );

        // ------------------------------------------------
        // Verify the parquet file content
        // newly created L-2 with largest timestamp
        let file = files.pop().unwrap();
        let batches = table.read_parquet_file(file).await;
        assert_batches_sorted_eq!(
            &[
                "+-----------+------+------+------+-----------------------------+",
                "| field_int | tag1 | tag2 | tag3 | time                        |",
                "+-----------+------+------+------+-----------------------------+",
                "| 270       | UT   |      |      | 1970-01-01T00:00:00.000025Z |",
                "+-----------+------+------+------+-----------------------------+",
            ],
            &batches
        );
        // newly created L-2 with smallest timestamp
        let file = files.pop().unwrap();
        let batches = table.read_parquet_file(file).await;
        assert_batches_sorted_eq!(
            &[
                "+-----------+------+------+------+--------------------------------+",
                "| field_int | tag1 | tag2 | tag3 | time                           |",
                "+-----------+------+------+------+--------------------------------+",
                "| 10        | VT   |      |      | 1970-01-01T00:00:00.000000020Z |",
                "| 10        | VT   |      |      | 1970-01-01T00:00:00.000006Z    |",
                "| 10        | VT   |      |      | 1970-01-01T00:00:00.000010Z    |",
                "| 1000      | WA   |      |      | 1970-01-01T00:00:00.000000010Z |",
                "| 1500      | WA   |      |      | 1970-01-01T00:00:00.000008Z    |",
                "| 1601      |      | PA   | 15   | 1970-01-01T00:00:00.000000009Z |",
                "| 21        |      | OH   | 21   | 1970-01-01T00:00:00.000000025Z |",
                "| 70        | UT   |      |      | 1970-01-01T00:00:00.000020Z    |",
                "+-----------+------+------+------+--------------------------------+",
            ],
            &batches
        );
        // available L2 that does not overlap
        let file = files.pop().unwrap();
        let batches = table.read_parquet_file(file).await;
        assert_batches_sorted_eq!(
            &[
                "+-----------+------+------+-----------------------------+",
                "| field_int | tag2 | tag3 | time                        |",
                "+-----------+------+------+-----------------------------+",
                "| 421       | OH   | 21   | 1970-01-01T00:00:00.000091Z |",
                "| 81601     | PA   | 15   | 1970-01-01T00:00:00.000090Z |",
                "+-----------+------+------+-----------------------------+",
            ],
            &batches
        );
        // available L1 that did not fit in the memory budget
        let file = files.pop().unwrap();
        let batches = table.read_parquet_file(file).await;
        assert_batches_sorted_eq!(
            &[
                "+-----------+------+------+-----------------------------+",
                "| field_int | tag2 | tag3 | time                        |",
                "+-----------+------+------+-----------------------------+",
                "| 1600      | WA   | 10   | 1970-01-01T00:00:00.000028Z |",
                "| 20        | VT   | 20   | 1970-01-01T00:00:00.000026Z |",
                "+-----------+------+------+-----------------------------+",
            ],
            &batches
        );
    }

    struct TestDb {
        catalog: Arc<TestCatalog>,
        compactor: Arc<Compactor>,
        table: Arc<TestTable>,
    }

    async fn make_db_with_4_l0s_2_l1s() -> TestDb {
        let catalog = TestCatalog::new();

        // lp1 does not overlap with any other level 0
        let lp1 = vec![
            "table,tag1=WA field_int=1000i 10",
            "table,tag1=VT field_int=10i 20",
        ]
        .join("\n");

        // lp2 overlaps with lp3
        let lp2 = vec![
            "table,tag1=WA field_int=1000i 8000", // will be eliminated due to duplicate
            "table,tag1=VT field_int=10i 10000",
            "table,tag1=UT field_int=70i 20000",
        ]
        .join("\n");

        // lp3 overlaps with lp2
        let lp3 = vec![
            "table,tag1=WA field_int=1500i 8000", // latest duplicate and kept
            "table,tag1=VT field_int=10i 6000",
            "table,tag1=UT field_int=270i 25000",
        ]
        .join("\n");

        // lp4 does not overlap with any
        let lp4 = vec![
            "table,tag2=WA,tag3=10 field_int=1600i 28000",
            "table,tag2=VT,tag3=20 field_int=20i 26000",
        ]
        .join("\n");

        // lp5 overlaps with lp1
        let lp5 = vec![
            "table,tag2=PA,tag3=15 field_int=1601i 9",
            "table,tag2=OH,tag3=21 field_int=21i 25",
        ]
        .join("\n");

        // lp6 does not overlap with any
        let lp6 = vec![
            "table,tag2=PA,tag3=15 field_int=81601i 90000",
            "table,tag2=OH,tag3=21 field_int=421i 91000",
        ]
        .join("\n");

        let ns = catalog.create_namespace_1hr_retention("ns").await;
        let shard = ns.create_shard(1).await;
        let table = ns.create_table("table").await;
        table.create_column("field_int", ColumnType::I64).await;
        table.create_column("tag1", ColumnType::Tag).await;
        table.create_column("tag2", ColumnType::Tag).await;
        table.create_column("tag3", ColumnType::Tag).await;
        table.create_column("time", ColumnType::Time).await;

        let partition = table.with_shard(&shard).create_partition("part").await;
        let time = Arc::new(SystemProvider::new());
        // let time_five_hour_ago = time.hours_ago(5);
        let config = make_compactor_config();
        let metrics = Arc::new(metric::Registry::new());
        let compactor = Arc::new(Compactor::new(
            ShardAssignment::Only(vec![shard.shard.id]),
            Arc::clone(&catalog.catalog),
            ParquetStorage::new(Arc::clone(&catalog.object_store), StorageId::from("iox")),
            catalog.exec(),
            Arc::new(SystemProvider::new()),
            BackoffConfig::default(),
            config,
            Arc::clone(&metrics),
        ));

        // parquet files that are all in the same partition
        let mut size_overrides = HashMap::<ParquetFileId, i64>::default();

        let time_60_minutes_ago = time.minutes_ago(60);
        let time_50_minutes_ago = time.minutes_ago(50);
        let time_40_minutes_ago = time.minutes_ago(40);
        let time_30_minutes_ago = time.minutes_ago(30);
        let time_20_minutes_ago = time.minutes_ago(20);
        let time_11_minutes_ago = time.minutes_ago(11);

        // pf1 does not overlap with any other level 0
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp1)
            .with_creation_time(time_50_minutes_ago)
            .with_min_time(10)
            .with_max_time(20);
        let pf1 = partition.create_parquet_file(builder).await;
        size_overrides.insert(
            pf1.parquet_file.id,
            compactor.config.max_desired_file_size_bytes as i64 + 10,
        );

        // pf2 overlaps with pf3
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp2)
            .with_creation_time(time_40_minutes_ago)
            .with_min_time(8_000)
            .with_max_time(20_000);
        let pf2 = partition.create_parquet_file(builder).await;
        size_overrides.insert(
            pf2.parquet_file.id,
            100, // small file
        );

        // pf3 overlaps with pf2
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp3)
            .with_creation_time(time_30_minutes_ago)
            .with_min_time(6_000)
            .with_max_time(25_000);
        let pf3 = partition.create_parquet_file(builder).await;
        size_overrides.insert(
            pf3.parquet_file.id,
            100, // small file
        );

        // pf4 does not overlap with any but is small
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp4)
            .with_creation_time(time_20_minutes_ago)
            .with_min_time(26_000)
            .with_max_time(28_000);
        let pf4 = partition.create_parquet_file(builder).await;
        size_overrides.insert(
            pf4.parquet_file.id,
            100, // small file
        );

        // pf5 was created in a previous compaction cycle; overlaps with pf1
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp5)
            .with_creation_time(time_60_minutes_ago)
            .with_min_time(9)
            .with_max_time(25)
            .with_compaction_level(CompactionLevel::FileNonOverlapped);
        let pf5 = partition.create_parquet_file(builder).await;
        size_overrides.insert(
            pf5.parquet_file.id,
            100, // small file
        );

        // pf6 was created in a previous compaction cycle; does not overlap with any
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp6)
            .with_creation_time(time_11_minutes_ago)
            .with_min_time(90000)
            .with_max_time(91000)
            .with_compaction_level(CompactionLevel::FileNonOverlapped);
        let pf6 = partition.create_parquet_file(builder).await;
        size_overrides.insert(
            pf6.parquet_file.id,
            100, // small file
        );

        // should have 4 level-0 files before compacting
        let count = catalog.count_level_0_files(shard.shard.id).await;
        assert_eq!(count, 4);

        // should have two level-1 file. Thus total L-0 and l-1 will be six before compacting
        let all_files = catalog.list_by_table_not_to_delete(table.table.id).await;
        assert_eq!(all_files.len(), 6);

        TestDb {
            catalog: Arc::clone(&catalog),
            compactor: Arc::clone(&compactor),
            table: Arc::clone(&table),
        }
    }
}
