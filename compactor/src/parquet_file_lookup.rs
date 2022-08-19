//! Logic for finding relevant Parquet files in the catalog to be considered during a compaction
//! operation.

use data_types::{CompactionLevel, ParquetFile, PartitionId};
use iox_catalog::interface::Catalog;
use observability_deps::tracing::*;
use snafu::{ResultExt, Snafu};
use std::sync::Arc;

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

/// Collection of Parquet files relevant to compacting a partition. Separated by compaction level.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ParquetFilesForCompaction {
    /// Parquet files for a partition with `CompactionLevel::Initial`. Ordered by ascending max
    /// sequence number.
    pub(crate) level_0: Vec<ParquetFile>,

    /// Parquet files for a partition with `CompactionLevel::FileNonOverlapped`. Arbitrary order.
    pub(crate) level_1: Vec<ParquetFile>,
}

impl ParquetFilesForCompaction {
    /// Given a catalog and a partition ID, find the Parquet files in the catalog relevant to a
    /// compaction operation.
    pub(crate) async fn for_partition(
        catalog: Arc<dyn Catalog>,
        partition_id: PartitionId,
    ) -> Result<Self, PartitionFilesFromPartitionError> {
        info!(
            partition_id = partition_id.get(),
            "finding parquet files for compaction"
        );

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

        for parquet_file in parquet_files {
            match parquet_file.compaction_level {
                CompactionLevel::Initial => level_0.push(parquet_file),
                CompactionLevel::FileNonOverlapped => level_1.push(parquet_file),
            }
        }

        level_0.sort_by_key(|pf| pf.max_sequence_number);

        Ok(Self { level_0, level_1 })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use data_types::ColumnType;
    use iox_tests::util::{TestCatalog, TestParquetFileBuilder, TestPartition};

    const ARBITRARY_LINE_PROTOCOL: &str = r#"
        table,tag1=WA field_int=1000i 8000
        table,tag1=VT field_int=10i 10000
        table,tag1=UT field_int=70i 20000
    "#;

    struct TestSetup {
        catalog: Arc<TestCatalog>,
        partition: Arc<TestPartition>,
        partition_on_another_shard: Arc<TestPartition>,
        older_partition: Arc<TestPartition>,
    }

    async fn test_setup() -> TestSetup {
        let catalog = TestCatalog::new();
        let ns = catalog.create_namespace("ns").await;
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

        TestSetup {
            catalog,
            partition,
            partition_on_another_shard,
            older_partition,
        }
    }

    #[tokio::test]
    async fn no_relevant_parquet_files_returns_empty() {
        test_helpers::maybe_start_logging();
        let TestSetup {
            catalog,
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

        let parquet_files_for_compaction = ParquetFilesForCompaction::for_partition(
            Arc::clone(&catalog.catalog),
            partition.partition.id,
        )
        .await
        .unwrap();
        assert!(
            parquet_files_for_compaction.level_0.is_empty(),
            "Expected empty, got: {:#?}",
            parquet_files_for_compaction.level_0
        );
        assert!(
            parquet_files_for_compaction.level_1.is_empty(),
            "Expected empty, got: {:#?}",
            parquet_files_for_compaction.level_1
        );
    }

    #[tokio::test]
    async fn one_level_0_file_gets_returned() {
        test_helpers::maybe_start_logging();
        let TestSetup {
            catalog, partition, ..
        } = test_setup().await;

        // Create a level 0 file
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(ARBITRARY_LINE_PROTOCOL)
            .with_compaction_level(CompactionLevel::Initial);
        let parquet_file = partition.create_parquet_file(builder).await;

        let parquet_files_for_compaction = ParquetFilesForCompaction::for_partition(
            Arc::clone(&catalog.catalog),
            partition.partition.id,
        )
        .await
        .unwrap();

        assert_eq!(
            parquet_files_for_compaction.level_0,
            vec![parquet_file.parquet_file]
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
            catalog, partition, ..
        } = test_setup().await;

        // Create a level 1 file
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(ARBITRARY_LINE_PROTOCOL)
            .with_compaction_level(CompactionLevel::FileNonOverlapped);
        let parquet_file = partition.create_parquet_file(builder).await;

        let parquet_files_for_compaction = ParquetFilesForCompaction::for_partition(
            Arc::clone(&catalog.catalog),
            partition.partition.id,
        )
        .await
        .unwrap();

        assert!(
            parquet_files_for_compaction.level_0.is_empty(),
            "Expected empty, got: {:#?}",
            parquet_files_for_compaction.level_0
        );

        assert_eq!(
            parquet_files_for_compaction.level_1,
            vec![parquet_file.parquet_file]
        );
    }

    #[tokio::test]
    async fn one_level_0_file_one_level_1_file_gets_returned() {
        test_helpers::maybe_start_logging();
        let TestSetup {
            catalog, partition, ..
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

        let parquet_files_for_compaction = ParquetFilesForCompaction::for_partition(
            Arc::clone(&catalog.catalog),
            partition.partition.id,
        )
        .await
        .unwrap();

        assert_eq!(parquet_files_for_compaction.level_0, vec![l0.parquet_file]);

        assert_eq!(parquet_files_for_compaction.level_1, vec![l1.parquet_file]);
    }

    #[tokio::test]
    async fn level_0_files_are_sorted_on_max_seq_num() {
        test_helpers::maybe_start_logging();
        let TestSetup {
            catalog, partition, ..
        } = test_setup().await;

        // Create a level 0 file, max seq = 100
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(ARBITRARY_LINE_PROTOCOL)
            .with_compaction_level(CompactionLevel::Initial)
            .with_max_seq(100);
        let l0_max_seq_100 = partition.create_parquet_file(builder).await;

        // Create a level 0 file, max seq = 50
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(ARBITRARY_LINE_PROTOCOL)
            .with_compaction_level(CompactionLevel::Initial)
            .with_max_seq(50);
        let l0_max_seq_50 = partition.create_parquet_file(builder).await;

        // Create a level 1 file
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(ARBITRARY_LINE_PROTOCOL)
            .with_compaction_level(CompactionLevel::FileNonOverlapped);
        let l1 = partition.create_parquet_file(builder).await;

        let parquet_files_for_compaction = ParquetFilesForCompaction::for_partition(
            Arc::clone(&catalog.catalog),
            partition.partition.id,
        )
        .await
        .unwrap();

        assert_eq!(
            parquet_files_for_compaction.level_0,
            vec![l0_max_seq_50.parquet_file, l0_max_seq_100.parquet_file]
        );

        assert_eq!(parquet_files_for_compaction.level_1, vec![l1.parquet_file]);
    }
}
