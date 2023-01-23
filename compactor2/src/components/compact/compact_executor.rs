use std::{future, sync::Arc};

use data_types::{CompactionLevel, ParquetFileParams};
use datafusion::{error::DataFusionError, physical_plan::SendableRecordBatchStream};
use futures::{stream::FuturesOrdered, StreamExt, TryStreamExt};
use observability_deps::tracing::debug;
use snafu::{ResultExt, Snafu};

use crate::{components::parquet_file_sink::ParquetFileSink, partition_info::PartitionInfo};

/// Compaction errors.
#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub enum Error {
    #[snafu(display("Error executing compact plan  {}", source))]
    ExecuteCompactPlan { source: DataFusionError },

    #[snafu(display("Could not serialize and persist record batches {}", source))]
    Persist {
        source: parquet_file::storage::UploadError,
    },
}

/// Executor of a plan
pub(crate) struct CompactExecutor {
    partition: Arc<PartitionInfo>,
    streams: Vec<SendableRecordBatchStream>,
    sink: Arc<dyn ParquetFileSink>,
    target_level: CompactionLevel,
}

impl CompactExecutor {
    /// Create a new executor
    pub fn new(
        streams: Vec<SendableRecordBatchStream>,
        partition: Arc<PartitionInfo>,
        sink: Arc<dyn ParquetFileSink>,
        target_level: CompactionLevel,
    ) -> Self {
        Self {
            partition,
            streams,
            sink,
            target_level,
        }
    }

    pub async fn execute(self) -> Result<Vec<ParquetFileParams>, Error> {
        let Self {
            partition,
            streams,
            sink,
            target_level,
        } = self;

        // Run to collect each stream of the plan
        debug!(stream_count = streams.len(), "running plan with streams");

        // These streams *must* to run in parallel otherwise a deadlock
        // can occur. Since there is a merge in the plan, in order to make
        // progress on one stream there must be (potential space) on the
        // other streams.
        //
        // https://github.com/influxdata/influxdb_iox/issues/4306
        // https://github.com/influxdata/influxdb_iox/issues/4324
        let compacted_parquet_files: Vec<ParquetFileParams> = streams
            .into_iter()
            .map(|stream| sink.store(stream, Arc::clone(&partition), target_level))
            // NB: FuturesOrdered allows the futures to run in parallel
            .collect::<FuturesOrdered<_>>()
            // Check for errors in the task
            .map(|t| t.context(ExecuteCompactPlanSnafu))
            // Discard the streams that resulted in empty output / no file uploaded
            // to the object store.
            .try_filter_map(|v| future::ready(Ok(v)))
            // Collect all the persisted parquet files together.
            .try_collect::<Vec<_>>()
            .await?;

        Ok(compacted_parquet_files)
    }
}
