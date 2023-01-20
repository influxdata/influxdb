use std::{future, sync::Arc};

use data_types::{CompactionLevel, ParquetFileParams, SequenceNumber, ShardId};
use datafusion::{error::DataFusionError, physical_plan::ExecutionPlan};
use futures::{stream::FuturesOrdered, StreamExt, TryStreamExt};
use iox_query::exec::{Executor, ExecutorType};
use iox_time::TimeProvider;
use observability_deps::tracing::{debug, info, trace, warn};
use parquet_file::{
    metadata::IoxMetadata,
    serialize::CodecError,
    storage::{ParquetStorage, UploadError},
};
use snafu::{ResultExt, Snafu};
use uuid::Uuid;

use crate::config::Config;

use super::partition::PartitionInfo;

// fields no longer used but still exists in the catalog
const SHARD_ID: ShardId = ShardId::new(0);
const MAX_SEQUENCE_NUMBER: i64 = 0;

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

    #[snafu(display("Error executing parquet write task  {}", source))]
    ExecuteParquetTask { source: tokio::task::JoinError },
}

/// Executor of a plan
pub(crate) struct CompactExecutor {
    // Partition of the plan to compact
    partition: Arc<PartitionInfo>,
    plan: Arc<dyn ExecutionPlan>,
    store: ParquetStorage,
    exec: Arc<Executor>,
    time_provider: Arc<dyn TimeProvider>,
    target_level: CompactionLevel,
}

impl CompactExecutor {
    /// Create a new executor
    pub fn new(
        plan: Arc<dyn ExecutionPlan>,
        partition: Arc<PartitionInfo>,
        config: Arc<Config>,
        target_level: CompactionLevel,
    ) -> Self {
        Self {
            partition,
            plan,
            store: config.parquet_store.clone(),
            exec: Arc::clone(&config.exec),
            time_provider: Arc::clone(&config.time_provider),

            target_level,
        }
    }

    pub async fn execute(self) -> Result<Vec<ParquetFileParams>, Error> {
        let Self {
            partition,
            plan,
            store,
            exec,
            time_provider,
            target_level,
        } = self;

        let partition_id = partition.partition_id;

        // Run to collect each stream of the plan
        let stream_count = plan.output_partitioning().partition_count();
        debug!(stream_count, "running plan with streams");

        // These streams *must* to run in parallel otherwise a deadlock
        // can occur. Since there is a merge in the plan, in order to make
        // progress on one stream there must be (potential space) on the
        // other streams.
        //
        // https://github.com/influxdata/influxdb_iox/issues/4306
        // https://github.com/influxdata/influxdb_iox/issues/4324
        let compacted_parquet_files: Vec<ParquetFileParams> = (0..stream_count)
            .map(|i| {
                // Prepare variables to pass to the closure
                let ctx = exec.new_context(ExecutorType::Reorg);
                let physical_plan = Arc::clone(&plan);
                let store = store.clone();
                let time_provider = Arc::clone(&time_provider);
                let partition = Arc::clone(&partition);
                let sort_key = partition.sort_key.clone();
                // run as a separate tokio task so files can be written
                // concurrently.
                tokio::task::spawn(async move {
                    trace!(partition = i, "executing datafusion partition");
                    let data = ctx
                        .execute_stream_partitioned(physical_plan, i)
                        .await
                        .context(ExecuteCompactPlanSnafu)?;
                    trace!(partition = i, "built result stream for partition");

                    let meta = IoxMetadata {
                        object_store_id: Uuid::new_v4(),
                        creation_timestamp: time_provider.now(),
                        shard_id: SHARD_ID,
                        namespace_id: partition.namespace_id,
                        namespace_name: partition.namespace_name.clone().into(),
                        table_id: partition.table.id,
                        table_name: partition.table.name.clone().into(),
                        partition_id,
                        partition_key: partition.partition_key.clone(),
                        max_sequence_number: SequenceNumber::new(MAX_SEQUENCE_NUMBER),
                        compaction_level: target_level,
                        sort_key: sort_key.clone(),
                    };

                    debug!(
                        partition_id = partition_id.get(),
                        "executing and uploading compaction StreamSplitExec"
                    );

                    let object_store_id = meta.object_store_id;
                    info!(
                        partition_id = partition_id.get(),
                        object_store_id = object_store_id.to_string(),
                        "streaming exec to object store"
                    );

                    // Stream the record batches from the compaction exec, serialize
                    // them, and directly upload the resulting Parquet files to
                    // object storage.
                    let (parquet_meta, file_size) = match store.upload(data, &meta).await {
                        Ok(v) => v,
                        Err(UploadError::Serialise(CodecError::NoRows)) => {
                            // This MAY be a bug.
                            //
                            // This also may happen legitimately, though very, very
                            // rarely. See test_empty_parquet_file_panic for an
                            // explanation.
                            warn!(
                                partition_id = partition_id.get(),
                                object_store_id = object_store_id.to_string(),
                                "SplitExec produced an empty result stream"
                            );
                            return Ok(None);
                        }
                        Err(e) => return Err(Error::Persist { source: e }),
                    };

                    debug!(
                        partition_id = partition_id.get(),
                        object_store_id = object_store_id.to_string(),
                        "file uploaded to object store"
                    );

                    let parquet_file =
                        meta.to_parquet_file(partition_id, file_size, &parquet_meta, |name| {
                            partition
                                .table_schema
                                .columns
                                .get(name)
                                .expect("unknown column")
                                .id
                        });

                    Ok(Some(parquet_file))
                })
            })
            // NB: FuturesOrdered allows the futures to run in parallel
            .collect::<FuturesOrdered<_>>()
            // Check for errors in the task
            .map(|t| t.context(ExecuteParquetTaskSnafu)?)
            // Discard the streams that resulted in empty output / no file uploaded
            // to the object store.
            .try_filter_map(|v| future::ready(Ok(v)))
            // Collect all the persisted parquet files together.
            .try_collect::<Vec<_>>()
            .await?;

        Ok(compacted_parquet_files)
    }
}
