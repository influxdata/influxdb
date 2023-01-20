//! Actual compaction routine.
use std::sync::Arc;

use data_types::{CompactionLevel, ParquetFile, ParquetFileParams};
use snafu::{ResultExt, Snafu};

use crate::config::Config;

use super::{
    compact_builder::CompactPlanBuilder, compact_executor::CompactExecutor,
    partition::PartitionInfo,
};

/// Compaction errors.
#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub enum Error {
    #[snafu(display("Not implemented"))]
    NotImplemented,

    #[snafu(display("Error building compact plan: {}", source))]
    BuildCompactPlan {
        source: super::compact_builder::Error,
    },

    #[snafu(display("Error building compact plan: {}", source))]
    ExecuteCompactPlan {
        source: super::compact_executor::Error,
    },
}

/// Perform compaction on given files including catalog transaction.
///
/// This MUST use all files. No further filtering is performed here.
/// The caller MUST ensure that the conpaction_level of the files are either compaction_level or compaction_level - 1
pub async fn compact_files(
    files: Arc<Vec<ParquetFile>>,
    partition_info: Arc<PartitionInfo>,
    config: Arc<Config>,
    compaction_level: CompactionLevel,
) -> Result<Vec<ParquetFileParams>, Error> {
    if files.is_empty() {
        return Ok(vec![]);
    }

    // build compact plan
    let builder = CompactPlanBuilder::new(
        files,
        Arc::clone(&partition_info),
        Arc::clone(&config),
        compaction_level,
    );
    let plan = builder
        .build_compact_plan()
        .await
        .context(BuildCompactPlanSnafu)?;

    // execute the plan
    let executor = CompactExecutor::new(plan, partition_info, config, compaction_level);
    let compacted_files = executor.execute().await.context(ExecuteCompactPlanSnafu)?;

    Ok(compacted_files)
}

#[cfg(test)]
mod tests {
    use data_types::CompactionLevel;
    use std::sync::Arc;

    use crate::{components::compact::compact_files::compact_files, test_util::TestSetup};

    #[tokio::test]
    async fn test_compact_no_file() {
        test_helpers::maybe_start_logging();

        // no files
        let setup = TestSetup::new(false).await;
        let TestSetup {
            files,
            partition_info,
            config,
            ..
        } = setup;

        let compacted_files = compact_files(
            Arc::clone(&files),
            Arc::clone(&partition_info),
            Arc::clone(&config),
            CompactionLevel::FileNonOverlapped,
        )
        .await
        .unwrap();

        assert!(compacted_files.is_empty());
    }

    #[tokio::test]
    async fn test_compact() {
        test_helpers::maybe_start_logging();

        // Create a test setup with 6 files
        let setup = TestSetup::new(true).await;
        let TestSetup {
            files,
            partition_info,
            config,
            ..
        } = setup;

        // By default, the config value is small, so the output file will be split
        let compacted_files = compact_files(
            Arc::clone(&files),
            Arc::clone(&partition_info),
            Arc::clone(&config),
            CompactionLevel::FileNonOverlapped,
        )
        .await
        .unwrap();
        assert_eq!(compacted_files.len(), 2);

        let mut config = (*config).clone();

        // let not split the output file by setting the config to a large value
        config.max_desired_file_size_bytes = 100 * 1024 * 1024;
        config.percentage_max_file_size = 100;
        config.split_percentage = 100;

        let compacted_files = compact_files(
            Arc::clone(&files),
            Arc::clone(&partition_info),
            Arc::new(config),
            CompactionLevel::FileNonOverlapped,
        )
        .await
        .unwrap();
        assert_eq!(compacted_files.len(), 1);
    }
}
