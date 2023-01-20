//! Actual compaction routine.
use std::sync::Arc;

use data_types::{CompactionLevel, ParquetFile, ParquetFileParams};
use snafu::Snafu;

use crate::config::Config;

use super::{compact_builder::CompactPlanBuilder, partition::PartitionInfo};

/// Compaction errors.
#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub enum Error {
    #[snafu(display("Not implemented"))]
    NotImplemented,
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
    let builder = CompactPlanBuilder::new(files, partition_info, config, compaction_level);

    let _logical_plan = builder.build_logical_compact_plan();

    // TODO: build and run physical plans

    // TODO: create parquet files for output plans

    Ok(vec![])
}
