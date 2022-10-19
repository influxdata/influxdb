use crate::{
    compact::PartitionCompactionCandidateWithInfo, parquet_file::CompactorParquetFile,
    query::QueryableParquetChunk,
};
use data_types::{
    CompactionLevel, ParquetFile, ParquetFileId, ParquetFileParams, PartitionId, SequenceNumber,
    TableSchema, TimestampMinMax,
};
use datafusion::{error::DataFusionError, logical_expr::LogicalPlan};
use futures::{stream::FuturesOrdered, StreamExt, TryStreamExt};
use iox_catalog::interface::Catalog;
use iox_query::{
    exec::{Executor, ExecutorType},
    frontend::reorg::ReorgPlanner,
    QueryChunk,
};
use iox_time::TimeProvider;
use metric::{Attributes, Metric, U64Histogram};
use observability_deps::tracing::*;
use parquet_file::{
    chunk::ParquetChunk,
    metadata::IoxMetadata,
    serialize::CodecError,
    storage::{ParquetStorage, UploadError},
};
use schema::{sort::SortKey, Schema};
use snafu::{ensure, ResultExt, Snafu};
use std::{
    cmp::{max, min},
    collections::BTreeMap,
    future,
    sync::Arc,
};
use uuid::Uuid;

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub(crate) enum Error {
    #[snafu(display(
        "Must specify at least 2 files to compact for {}, got {num_files}", partition_id.get()
    ))]
    NotEnoughParquetFiles {
        num_files: usize,
        partition_id: PartitionId,
    },

    #[snafu(display("Error building compact logical plan  {}", source))]
    CompactLogicalPlan {
        source: iox_query::frontend::reorg::Error,
    },

    #[snafu(display("Error building compact physical plan  {}", source))]
    CompactPhysicalPlan { source: DataFusionError },

    #[snafu(display("Error executing compact plan  {}", source))]
    ExecuteCompactPlan { source: DataFusionError },

    #[snafu(display("Error executing parquet write task  {}", source))]
    ExecuteParquetTask { source: tokio::task::JoinError },

    #[snafu(display("Could not serialize and persist record batches {}", source))]
    Persist {
        source: parquet_file::storage::UploadError,
    },

    #[snafu(display("Could not update catalog for partition {}: {source}", partition_id.get()))]
    Catalog {
        partition_id: PartitionId,
        source: CatalogUpdateError,
    },
}

// Compact the given parquet files received from `filter_parquet_files` into one stream
#[allow(clippy::too_many_arguments)]
pub(crate) async fn compact_parquet_files(
    files: Vec<CompactorParquetFile>,
    partition: Arc<PartitionCompactionCandidateWithInfo>,
    // The global catalog for schema, parquet files and tombstones
    catalog: Arc<dyn Catalog>,
    // Object store for reading input parquet files and writing compacted parquet files
    store: ParquetStorage,
    // Executor for running queries, compacting, and persisting
    exec: Arc<Executor>,
    time_provider: Arc<dyn TimeProvider>,
    // Histogram for the sizes of the files compacted
    compaction_input_file_bytes: &Metric<U64Histogram>,
    // Desired max size of compacted parquet files.
    // It is a target desired value, rather than a guarantee.
    max_desired_file_size_bytes: u64,
    // Percentage of desired max file size. This percentage of `max_desired_file_size_bytes` is
    // considered "small" and will not be split. 100 + this percentage of
    // `max_desired_file_size_bytes` is considered "large" and will be split into files roughly of
    // `max_desired_file_size_bytes`. For amounts of data between "small" and "large", the data
    // will be split into 2 parts with roughly `split_percentage` in the earlier compacted file and
    // 1 - `split_percentage` in the later compacted file.
    percentage_max_file_size: u16,
    // When data is between a "small" and "large" amount, split the compacted files at roughly this
    // percentage in the earlier compacted file, and the remainder in the later compacted file.
    split_percentage: u16,
    // Compaction level the newly created file will have.
    target_level: CompactionLevel,
) -> Result<(), Error> {
    let partition_id = partition.id();

    let num_files = files.len();
    ensure!(
        num_files > 0,
        NotEnoughParquetFilesSnafu {
            num_files,
            partition_id
        }
    );

    // Save all file sizes for recording metrics if this compaction succeeds.
    let file_sizes: Vec<_> = files.iter().map(|f| f.file_size_bytes()).collect();
    // Find the total size of all files, to be used to determine if the result should be one file
    // or if the result should be split into multiple files.
    let total_size: i64 = file_sizes.iter().sum();
    let total_size = total_size as u64;

    // Compute the number of files per compaction level for logging
    let mut num_files_by_level = BTreeMap::new();
    for compaction_level in files.iter().map(|f| f.compaction_level()) {
        *num_files_by_level.entry(compaction_level).or_default() += 1;
    }
    let num_level_0 = num_files_by_level
        .get(&CompactionLevel::Initial)
        .unwrap_or(&0);
    let num_level_1 = num_files_by_level
        .get(&CompactionLevel::FileNonOverlapped)
        .unwrap_or(&0);
    let num_level_2 = num_files_by_level
        .get(&CompactionLevel::Final)
        .unwrap_or(&0);
    debug!(
        ?partition_id,
        num_files, num_level_0, num_level_1, num_level_2, "compact files to stream"
    );

    // Collect all the parquet file IDs, to be able to set their catalog records to be
    // deleted. These should already be unique, no need to dedupe.
    let original_parquet_file_ids: Vec<_> = files.iter().map(|f| f.id()).collect();

    // Convert the input files into QueryableParquetChunk for making query plan
    let query_chunks: Vec<_> = files
        .into_iter()
        .map(|file| {
            to_queryable_parquet_chunk(
                file,
                store.clone(),
                partition.table.name.clone(),
                &partition.table_schema,
                partition.sort_key.clone(),
                target_level,
            )
        })
        .collect();

    trace!(
        n_query_chunks = query_chunks.len(),
        "gathered parquet data to compact"
    );

    // Compute max sequence numbers and min/max time
    // unwrap here will work because the len of the query_chunks already >= 1
    let (head, tail) = query_chunks.split_first().unwrap();
    let mut max_sequence_number = head.max_sequence_number();
    let mut min_time = head.min_time();
    let mut max_time = head.max_time();
    for c in tail {
        max_sequence_number = max(max_sequence_number, c.max_sequence_number());
        min_time = min(min_time, c.min_time());
        max_time = max(max_time, c.max_time());
    }

    // extract the min & max chunk times for filtering potential split times.
    let chunk_times: Vec<_> = query_chunks
        .iter()
        .map(|c| TimestampMinMax::new(c.min_time(), c.max_time()))
        .collect();

    // Merge schema of the compacting chunks
    let query_chunks: Vec<_> = query_chunks
        .into_iter()
        .map(|c| Arc::new(c) as Arc<dyn QueryChunk>)
        .collect();
    let merged_schema = QueryableParquetChunk::merge_schemas(&query_chunks);
    debug!(
        num_cols = merged_schema.as_arrow().fields().len(),
        "Number of columns in the merged schema to build query plan"
    );

    // All partitions in the catalog MUST contain a sort key.
    let sort_key = partition
        .sort_key
        .as_ref()
        .expect("no partition sort key in catalog")
        .filter_to(&merged_schema.primary_key(), partition_id.get());

    let (small_cutoff_bytes, large_cutoff_bytes) =
        cutoff_bytes(max_desired_file_size_bytes, percentage_max_file_size);

    let ctx = exec.new_context(ExecutorType::Reorg);
    let plan = if total_size <= small_cutoff_bytes {
        // Compact everything into one file
        ReorgPlanner::new(ctx.child_ctx("ReorgPlanner"))
            .compact_plan(Arc::clone(&merged_schema), query_chunks, sort_key.clone())
            .context(CompactLogicalPlanSnafu)?
    } else {
        let split_times = if small_cutoff_bytes < total_size && total_size <= large_cutoff_bytes {
            // Split compaction into two files, the earlier of split_percentage amount of
            // max_desired_file_size_bytes, the later of the rest
            vec![min_time + ((max_time - min_time) * split_percentage as i64) / 100]
        } else {
            // Split compaction into multiple files
            crate::utils::compute_split_time(
                chunk_times,
                min_time,
                max_time,
                total_size,
                max_desired_file_size_bytes,
            )
        };

        if split_times.is_empty() || (split_times.len() == 1 && split_times[0] == max_time) {
            // The split times might not have actually split anything, so in this case, compact
            // everything into one file
            ReorgPlanner::new(ctx.child_ctx("ReorgPlanner"))
                .compact_plan(Arc::clone(&merged_schema), query_chunks, sort_key.clone())
                .context(CompactLogicalPlanSnafu)?
        } else {
            // split compact query plan
            ReorgPlanner::new(ctx.child_ctx("ReorgPlanner"))
                .split_plan(
                    Arc::clone(&merged_schema),
                    query_chunks,
                    sort_key.clone(),
                    split_times,
                )
                .context(CompactLogicalPlanSnafu)?
        }
    };

    let compacted_parquet_files = compact_with_plan(
        store,
        exec,
        time_provider,
        plan,
        sort_key,
        Arc::clone(&partition),
        partition_id,
        max_sequence_number,
        target_level,
    )
    .await?;

    update_catalog(
        catalog,
        partition_id,
        compacted_parquet_files,
        &original_parquet_file_ids,
    )
    .await
    .context(CatalogSnafu { partition_id })?;

    info!(?partition_id, "compaction complete");

    let attributes = Attributes::from([("shard_id", format!("{}", partition.shard_id()).into())]);
    let compaction_input_file_bytes = compaction_input_file_bytes.recorder(attributes);
    for size in file_sizes {
        compaction_input_file_bytes.record(size as u64);
    }

    Ok(())
}

/// Compact all files given, no matter their size, into one file.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn compact_final_no_splits(
    files: Vec<CompactorParquetFile>,
    partition: Arc<PartitionCompactionCandidateWithInfo>,
    // The global catalog for schema, parquet files and tombstones
    catalog: Arc<dyn Catalog>,
    // Object store for reading input parquet files and writing compacted parquet files
    store: ParquetStorage,
    // Executor for running queries, compacting, and persisting
    exec: Arc<Executor>,
    time_provider: Arc<dyn TimeProvider>,
    // Histogram for the sizes of the files compacted
    compaction_input_file_bytes: &Metric<U64Histogram>,
    // Compaction level the newly created file will have.
    target_level: CompactionLevel,
) -> Result<(), Error> {
    let partition_id = partition.id();

    let num_files = files.len();
    ensure!(
        num_files > 0,
        NotEnoughParquetFilesSnafu {
            num_files,
            partition_id
        }
    );

    // Save all file sizes for recording metrics if this compaction succeeds.
    let file_sizes: Vec<_> = files.iter().map(|f| f.file_size_bytes()).collect();

    debug!(?partition_id, num_files, "compact_final_no_splits");

    // Collect all the parquet file IDs, to be able to set their catalog records to be
    // deleted. These should already be unique, no need to dedupe.
    let original_parquet_file_ids: Vec<_> = files.iter().map(|f| f.id()).collect();

    // Convert the input files into QueryableParquetChunk for making query plan
    let query_chunks: Vec<_> = files
        .into_iter()
        .map(|file| {
            to_queryable_parquet_chunk(
                file,
                store.clone(),
                partition.table.name.clone(),
                &partition.table_schema,
                partition.sort_key.clone(),
                target_level,
            )
        })
        .collect();

    trace!(
        n_query_chunks = query_chunks.len(),
        "gathered parquet data to compact"
    );

    // Compute max sequence numbers and min/max time
    // unwrap here will work because the len of the query_chunks already >= 1
    let (head, tail) = query_chunks.split_first().unwrap();
    let mut max_sequence_number = head.max_sequence_number();
    let mut min_time = head.min_time();
    let mut max_time = head.max_time();
    for c in tail {
        max_sequence_number = max(max_sequence_number, c.max_sequence_number());
        min_time = min(min_time, c.min_time());
        max_time = max(max_time, c.max_time());
    }

    // Merge schema of the compacting chunks
    let query_chunks: Vec<_> = query_chunks
        .into_iter()
        .map(|c| Arc::new(c) as Arc<dyn QueryChunk>)
        .collect();
    let merged_schema = QueryableParquetChunk::merge_schemas(&query_chunks);
    debug!(
        num_cols = merged_schema.as_arrow().fields().len(),
        "Number of columns in the merged schema to build query plan"
    );

    // All partitions in the catalog MUST contain a sort key.
    let sort_key = partition
        .sort_key
        .as_ref()
        .expect("no partition sort key in catalog")
        .filter_to(&merged_schema.primary_key(), partition_id.get());

    let ctx = exec.new_context(ExecutorType::Reorg);
    // Compact everything into one file
    let plan = ReorgPlanner::new(ctx.child_ctx("ReorgPlanner"))
        .compact_plan(Arc::clone(&merged_schema), query_chunks, sort_key.clone())
        .context(CompactLogicalPlanSnafu)?;

    let compacted_parquet_files = compact_with_plan(
        store,
        exec,
        time_provider,
        plan,
        sort_key,
        Arc::clone(&partition),
        partition_id,
        max_sequence_number,
        target_level,
    )
    .await?;

    update_catalog(
        catalog,
        partition_id,
        compacted_parquet_files,
        &original_parquet_file_ids,
    )
    .await
    .context(CatalogSnafu { partition_id })?;

    info!(?partition_id, "compaction complete");

    let attributes = Attributes::from([("shard_id", format!("{}", partition.shard_id()).into())]);
    let compaction_input_file_bytes = compaction_input_file_bytes.recorder(attributes);
    for size in file_sizes {
        compaction_input_file_bytes.record(size as u64);
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn compact_with_plan(
    store: ParquetStorage,
    exec: Arc<Executor>,
    time_provider: Arc<dyn TimeProvider>,
    plan: LogicalPlan,
    sort_key: SortKey,
    partition: Arc<PartitionCompactionCandidateWithInfo>,
    partition_id: PartitionId,
    max_sequence_number: SequenceNumber,
    target_level: CompactionLevel,
) -> Result<Vec<ParquetFileParams>, Error> {
    let ctx = exec.new_context(ExecutorType::Reorg);
    let physical_plan = ctx
        .create_physical_plan(&plan)
        .await
        .context(CompactPhysicalPlanSnafu)?;

    // Run to collect each stream of the plan
    let stream_count = physical_plan.output_partitioning().partition_count();

    debug!("running plan with {} streams", stream_count);

    // These streams *must* to run in parallel otherwise a deadlock
    // can occur. Since there is a merge in the plan, in order to make
    // progress on one stream there must be (potential space) on the
    // other streams.
    //
    // https://github.com/influxdata/influxdb_iox/issues/4306
    // https://github.com/influxdata/influxdb_iox/issues/4324
    (0..stream_count)
        .map(|i| {
            // Prepare variables to pass to the closure
            let ctx = exec.new_context(ExecutorType::Reorg);
            let physical_plan = Arc::clone(&physical_plan);
            let store = store.clone();
            let time_provider = Arc::clone(&time_provider);
            let sort_key = sort_key.clone();
            let partition = Arc::clone(&partition);
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
                    shard_id: partition.shard_id(),
                    namespace_id: partition.namespace_id(),
                    namespace_name: partition.namespace.name.clone().into(),
                    table_id: partition.table.id,
                    table_name: partition.table.name.clone().into(),
                    partition_id,
                    partition_key: partition.partition_key.clone(),
                    max_sequence_number,
                    compaction_level: target_level,
                    sort_key: Some(sort_key.clone()),
                };

                debug!(
                    ?partition_id,
                    "executing and uploading compaction StreamSplitExec"
                );

                let object_store_id = meta.object_store_id;
                info!(?partition_id, %object_store_id, "streaming exec to object store");

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
                            ?partition_id,
                            %object_store_id,
                            "SplitExec produced an empty result stream"
                        );
                        return Ok(None);
                    }
                    Err(e) => return Err(Error::Persist { source: e }),
                };

                debug!(?partition_id, %object_store_id, "file uploaded to object store");

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
        .await
}

/// Convert ParquetFile to a QueryableParquetChunk
fn to_queryable_parquet_chunk(
    file: CompactorParquetFile,
    store: ParquetStorage,
    table_name: String,
    table_schema: &TableSchema,
    partition_sort_key: Option<SortKey>,
    target_level: CompactionLevel,
) -> QueryableParquetChunk {
    let column_id_lookup = table_schema.column_id_map();
    let selection: Vec<_> = file
        .column_set()
        .iter()
        .flat_map(|id| column_id_lookup.get(id).copied())
        .collect();
    let table_schema: Schema = table_schema
        .clone()
        .try_into()
        .expect("table schema is broken");
    let schema = table_schema
        .select_by_names(&selection)
        .expect("schema in-sync");
    let pk = schema.primary_key();
    let sort_key = partition_sort_key
        .as_ref()
        .map(|sk| sk.filter_to(&pk, file.partition_id().get()));
    let file = Arc::new(ParquetFile::from(file));

    let parquet_chunk = ParquetChunk::new(Arc::clone(&file), Arc::new(schema), store);

    trace!(
        parquet_file_id=?file.id,
        parquet_file_shard_id=?file.shard_id,
        parquet_file_namespace_id=?file.namespace_id,
        parquet_file_table_id=?file.table_id,
        parquet_file_partition_id=?file.partition_id,
        parquet_file_object_store_id=?file.object_store_id,
        "built parquet chunk from metadata"
    );

    // If there is no sort key on this parquet chunk, the query
    // engine will end up resorting it, requiring substantial
    // memory. Thus warn if this has happened as it signals a bug in
    // the code somewhere.
    if sort_key.is_none() {
        warn!(parquet_file_id=?file.id,
              parquet_file_namespace_id=?file.namespace_id,
              parquet_file_object_store_id=?file.object_store_id,
              "Parquet file is not sorted."
        );
    }

    QueryableParquetChunk::new(
        table_name,
        file.partition_id,
        Arc::new(parquet_chunk),
        &[],
        file.max_sequence_number,
        file.min_time,
        file.max_time,
        sort_key,
        partition_sort_key,
        file.compaction_level,
        target_level,
    )
}

fn cutoff_bytes(max_desired_file_size_bytes: u64, percentage_max_file_size: u16) -> (u64, u64) {
    (
        (max_desired_file_size_bytes * percentage_max_file_size as u64) / 100,
        (max_desired_file_size_bytes * (100 + percentage_max_file_size as u64)) / 100,
    )
}

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub(crate) enum CatalogUpdateError {
    #[snafu(display("Error while starting catalog transaction {}", source))]
    Transaction {
        source: iox_catalog::interface::Error,
    },

    #[snafu(display("Error while committing catalog transaction {}", source))]
    TransactionCommit {
        source: iox_catalog::interface::Error,
    },

    #[snafu(display("Error updating catalog {}", source))]
    Update {
        source: iox_catalog::interface::Error,
    },

    #[snafu(display("Error while flagging a parquet file for deletion {}", source))]
    FlagForDelete {
        source: iox_catalog::interface::Error,
    },
}

async fn update_catalog(
    catalog: Arc<dyn Catalog>,
    partition_id: PartitionId,
    compacted_parquet_files: Vec<ParquetFileParams>,
    original_parquet_file_ids: &[ParquetFileId],
) -> Result<(), CatalogUpdateError> {
    let mut txn = catalog
        .start_transaction()
        .await
        .context(TransactionSnafu)?;

    // Create the new parquet file in the catalog first
    for parquet_file in compacted_parquet_files {
        debug!(
            ?partition_id,
            %parquet_file.object_store_id,
            "updating catalog"
        );

        txn.parquet_files()
            .create(parquet_file)
            .await
            .context(UpdateSnafu)?;
    }

    // Mark input files for deletion
    for &original_parquet_file_id in original_parquet_file_ids {
        txn.parquet_files()
            .flag_for_delete(original_parquet_file_id)
            .await
            .context(FlagForDeleteSnafu)?;
    }

    txn.commit().await.context(TransactionCommitSnafu)
}

#[cfg(test)]
mod tests {
    use crate::parquet_file::CompactorParquetFile;

    use super::*;
    use arrow_util::assert_batches_sorted_eq;
    use data_types::{ColumnType, PartitionParam, ShardId};
    use iox_tests::util::{TestCatalog, TestParquetFileBuilder, TestTable};
    use metric::U64HistogramOptions;
    use parquet_file::storage::StorageId;
    use test_helpers::assert_error;

    #[test]
    fn test_cutoff_bytes() {
        let (small, large) = cutoff_bytes(100, 30);
        assert_eq!(small, 30);
        assert_eq!(large, 130);

        let (small, large) = cutoff_bytes(100 * 1024 * 1024, 30);
        assert_eq!(small, 30 * 1024 * 1024);
        assert_eq!(large, 130 * 1024 * 1024);

        let (small, large) = cutoff_bytes(100, 60);
        assert_eq!(small, 60);
        assert_eq!(large, 160);
    }

    const DEFAULT_MAX_DESIRED_FILE_SIZE_BYTES: u64 = 100 * 1024 * 1024;
    const DEFAULT_PERCENTAGE_MAX_FILE_SIZE: u16 = 30;
    const DEFAULT_SPLIT_PERCENTAGE: u16 = 80;
    const BUCKET_500_KB: u64 = 500 * 1024;

    struct TestSetup {
        catalog: Arc<TestCatalog>,
        table: Arc<TestTable>,
        candidate_partition: Arc<PartitionCompactionCandidateWithInfo>,
        parquet_files: Vec<CompactorParquetFile>,
    }

    async fn test_setup() -> TestSetup {
        let catalog = TestCatalog::new();
        let ns = catalog.create_namespace("ns").await;
        let shard = ns.create_shard(1).await;
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

        let candidate_partition = Arc::new(PartitionCompactionCandidateWithInfo {
            table: Arc::new(table.table.clone()),
            table_schema: Arc::new(table_schema),
            column_type_counts: Vec::new(), // not relevant
            namespace: Arc::new(ns.namespace.clone()),
            candidate: PartitionParam {
                partition_id: partition.partition.id,
                shard_id: partition.partition.shard_id,
                namespace_id: ns.namespace.id,
                table_id: partition.partition.table_id,
            },
            sort_key: partition.partition.sort_key(),
            partition_key: partition.partition.partition_key.clone(),
        });

        let lp = vec![
            "table,tag2=PA,tag3=15 field_int=1601i 30000",
            "table,tag2=OH,tag3=21 field_int=21i 36000",
        ]
        .join("\n");
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp)
            .with_max_seq(20) // This should be irrelevant because this is a level 1 file
            .with_compaction_level(CompactionLevel::FileNonOverlapped); // Prev compaction
        let level_1_file = partition.create_parquet_file(builder).await.into();

        let lp = vec![
            "table,tag1=WA field_int=1000i 8000", // will be eliminated due to duplicate
            "table,tag1=VT field_int=10i 10000",
            "table,tag1=UT field_int=70i 20000",
        ]
        .join("\n");
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp)
            .with_max_seq(1);
        let level_0_max_seq_1 = partition.create_parquet_file(builder).await.into();

        let lp = vec![
            "table,tag1=WA field_int=1500i 8000", // latest duplicate and kept
            "table,tag1=VT field_int=10i 6000",
            "table,tag1=UT field_int=270i 25000",
        ]
        .join("\n");
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp)
            .with_max_seq(2);
        let level_0_max_seq_2 = partition.create_parquet_file(builder).await.into();

        let lp = vec![
            "table,tag1=VT field_int=88i 10000", // will be deduplicated with level_0_max_seq_1
            "table,tag1=OR field_int=99i 12000",
        ]
        .join("\n");
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp)
            .with_max_seq(5) // This should be irrelevant because this is a level 1 file
            .with_compaction_level(CompactionLevel::FileNonOverlapped); // Prev compaction
        let level_1_with_duplicates = partition.create_parquet_file(builder).await.into();

        let lp = vec!["table,tag2=OH,tag3=21 field_int=21i 36000"].join("\n");
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp)
            .with_min_time(0)
            .with_max_time(36000)
            // Will put the group size between "small" and "large"
            .with_size_override(50 * 1024 * 1024);
        let medium_file = partition.create_parquet_file(builder).await.into();

        let lp = vec![
            "table,tag1=VT field_int=10i 68000",
            "table,tag2=OH,tag3=21 field_int=210i 136000",
        ]
        .join("\n");
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp)
            .with_min_time(36001)
            .with_max_time(136000)
            // Will put the group size two multiples over "large"
            .with_size_override(180 * 1024 * 1024);
        let large_file = partition.create_parquet_file(builder).await.into();

        // Order here isn't relevant; the chunk order should ensure the level 1 files are ordered
        // first, then the other files by max seq num.
        let parquet_files = vec![
            level_0_max_seq_2,
            level_1_with_duplicates,
            level_0_max_seq_1,
            level_1_file,
            medium_file,
            large_file,
        ];

        TestSetup {
            catalog,
            table,
            candidate_partition,
            parquet_files,
        }
    }

    fn metrics() -> Metric<U64Histogram> {
        let registry = Arc::new(metric::Registry::new());
        registry.register_metric_with_options(
            "compaction_input_file_bytes",
            "Number of bytes of Parquet files used as inputs to a successful compaction \
             operation",
            || {
                U64HistogramOptions::new([
                    BUCKET_500_KB,    // 500 KB
                    1024 * 1024,      // 1 MB
                    3 * 1024 * 1024,  // 3 MB
                    10 * 1024 * 1024, // 10 MB
                    30 * 1024 * 1024, // 30 MB
                    u64::MAX,         // Inf
                ])
            },
        )
    }

    #[tokio::test]
    async fn no_input_files_is_an_error() {
        test_helpers::maybe_start_logging();

        let TestSetup {
            catalog,
            candidate_partition,
            ..
        } = test_setup().await;
        let compaction_input_file_bytes = metrics();
        let shard_id = candidate_partition.shard_id();

        let files = vec![];
        let result = compact_parquet_files(
            files,
            candidate_partition,
            Arc::clone(&catalog.catalog),
            ParquetStorage::new(Arc::clone(&catalog.object_store), StorageId::from("iox")),
            Arc::clone(&catalog.exec),
            Arc::clone(&catalog.time_provider) as Arc<dyn TimeProvider>,
            &compaction_input_file_bytes,
            DEFAULT_MAX_DESIRED_FILE_SIZE_BYTES,
            DEFAULT_PERCENTAGE_MAX_FILE_SIZE,
            DEFAULT_SPLIT_PERCENTAGE,
            CompactionLevel::FileNonOverlapped,
        )
        .await;
        assert_error!(result, Error::NotEnoughParquetFiles { num_files: 0, .. });

        // No metrics recorded because the compaction didn't succeed
        assert_eq!(
            extract_byte_metrics(&compaction_input_file_bytes, shard_id),
            ExtractedByteMetrics {
                sample_count: 0,
                buckets_with_counts: vec![],
            }
        );
    }

    #[tokio::test]
    async fn one_input_file_gets_compacted() {
        test_helpers::maybe_start_logging();

        let TestSetup {
            catalog,
            candidate_partition,
            mut parquet_files,
            ..
        } = test_setup().await;
        let table_id = candidate_partition.table_id();
        let compaction_input_file_bytes = metrics();
        let shard_id = candidate_partition.shard_id();

        let parquet_file = parquet_files.remove(0);
        compact_parquet_files(
            vec![parquet_file],
            candidate_partition,
            Arc::clone(&catalog.catalog),
            ParquetStorage::new(Arc::clone(&catalog.object_store), StorageId::from("iox")),
            Arc::clone(&catalog.exec),
            Arc::clone(&catalog.time_provider) as Arc<dyn TimeProvider>,
            &compaction_input_file_bytes,
            DEFAULT_MAX_DESIRED_FILE_SIZE_BYTES,
            DEFAULT_PERCENTAGE_MAX_FILE_SIZE,
            DEFAULT_SPLIT_PERCENTAGE,
            CompactionLevel::FileNonOverlapped,
        )
        .await
        .unwrap();

        // Should have 6 non-soft-deleted files:
        //
        // - 3 initial level 0 files not compacted
        // - 2 initial level 1 files not compacted
        // - the 1 initial level 0 file that was "compacted" into 1 level 1 file
        let files = catalog.list_by_table_not_to_delete(table_id).await;
        assert_eq!(files.len(), 6);
        let files_and_levels: Vec<_> = files
            .iter()
            .map(|f| (f.id.get(), f.compaction_level))
            .collect();
        assert_eq!(
            files_and_levels,
            vec![
                (1, CompactionLevel::FileNonOverlapped),
                (2, CompactionLevel::Initial),
                (4, CompactionLevel::FileNonOverlapped),
                (5, CompactionLevel::Initial),
                (6, CompactionLevel::Initial),
                (7, CompactionLevel::FileNonOverlapped),
            ]
        );

        // Verify the metrics
        assert_eq!(
            extract_byte_metrics(&compaction_input_file_bytes, shard_id),
            ExtractedByteMetrics {
                sample_count: 1,
                buckets_with_counts: vec![(BUCKET_500_KB, 1)],
            }
        );
    }

    #[tokio::test]
    async fn small_files_get_compacted_into_one() {
        test_helpers::maybe_start_logging();

        let TestSetup {
            catalog,
            table,
            candidate_partition,
            parquet_files,
        } = test_setup().await;
        let compaction_input_file_bytes = metrics();
        let shard_id = candidate_partition.shard_id();

        compact_parquet_files(
            parquet_files.into_iter().take(4).collect(),
            candidate_partition,
            Arc::clone(&catalog.catalog),
            ParquetStorage::new(Arc::clone(&catalog.object_store), StorageId::from("iox")),
            Arc::clone(&catalog.exec),
            Arc::clone(&catalog.time_provider) as Arc<dyn TimeProvider>,
            &compaction_input_file_bytes,
            DEFAULT_MAX_DESIRED_FILE_SIZE_BYTES,
            DEFAULT_PERCENTAGE_MAX_FILE_SIZE,
            DEFAULT_SPLIT_PERCENTAGE,
            CompactionLevel::FileNonOverlapped,
        )
        .await
        .unwrap();

        // Should have 3 non-soft-deleted files:
        //
        // - the one newly created after compacting
        // - the 2 large ones not included in this compaction operation
        let mut files = catalog.list_by_table_not_to_delete(table.table.id).await;
        assert_eq!(files.len(), 3);
        let files_and_levels: Vec<_> = files
            .iter()
            .map(|f| (f.id.get(), f.compaction_level))
            .collect();
        // 2 large files not included in compaction,
        // 1 newly created CompactionLevel::FileNonOverlapped file as the result of
        // compaction
        assert_eq!(
            files_and_levels,
            vec![
                (5, CompactionLevel::Initial),
                (6, CompactionLevel::Initial),
                (7, CompactionLevel::FileNonOverlapped),
            ]
        );

        // Verify the metrics
        assert_eq!(
            extract_byte_metrics(&compaction_input_file_bytes, shard_id),
            ExtractedByteMetrics {
                sample_count: 4,
                buckets_with_counts: vec![(BUCKET_500_KB, 4)],
            }
        );

        // ------------------------------------------------
        // Verify the parquet file content

        // Compacted file
        let file1 = files.pop().unwrap();
        let batches = table.read_parquet_file(file1).await;
        assert_batches_sorted_eq!(
            &[
                "+-----------+------+------+------+-----------------------------+",
                "| field_int | tag1 | tag2 | tag3 | time                        |",
                "+-----------+------+------+------+-----------------------------+",
                "| 10        | VT   |      |      | 1970-01-01T00:00:00.000006Z |",
                "| 10        | VT   |      |      | 1970-01-01T00:00:00.000010Z |",
                "| 1500      | WA   |      |      | 1970-01-01T00:00:00.000008Z |",
                "| 1601      |      | PA   | 15   | 1970-01-01T00:00:00.000030Z |",
                "| 21        |      | OH   | 21   | 1970-01-01T00:00:00.000036Z |",
                "| 270       | UT   |      |      | 1970-01-01T00:00:00.000025Z |",
                "| 70        | UT   |      |      | 1970-01-01T00:00:00.000020Z |",
                "| 99        | OR   |      |      | 1970-01-01T00:00:00.000012Z |",
                "+-----------+------+------+------+-----------------------------+",
            ],
            &batches
        );
    }

    #[tokio::test]
    async fn medium_input_files_get_split_into_two() {
        test_helpers::maybe_start_logging();

        let TestSetup {
            catalog,
            table,
            candidate_partition,
            parquet_files,
        } = test_setup().await;
        let compaction_input_file_bytes = metrics();
        let shard_id = candidate_partition.shard_id();

        compact_parquet_files(
            parquet_files.into_iter().take(5).collect(),
            candidate_partition,
            Arc::clone(&catalog.catalog),
            ParquetStorage::new(Arc::clone(&catalog.object_store), StorageId::from("iox")),
            Arc::clone(&catalog.exec),
            Arc::clone(&catalog.time_provider) as Arc<dyn TimeProvider>,
            &compaction_input_file_bytes,
            DEFAULT_MAX_DESIRED_FILE_SIZE_BYTES,
            DEFAULT_PERCENTAGE_MAX_FILE_SIZE,
            DEFAULT_SPLIT_PERCENTAGE,
            CompactionLevel::FileNonOverlapped,
        )
        .await
        .unwrap();

        // Should have 3 non-soft-deleted files:
        // - 1 large file not included in compaction
        // - 2 files from compacting everything together then splitting.
        let mut files = catalog.list_by_table_not_to_delete(table.table.id).await;
        assert_eq!(files.len(), 3);
        let files_and_levels: Vec<_> = files
            .iter()
            .map(|f| (f.id.get(), f.compaction_level))
            .collect();
        // 1 large files not included in compaction,
        // 2 newly created CompactionLevel::FileNonOverlapped file as the result of
        // compaction and splitting
        assert_eq!(
            files_and_levels,
            vec![
                (6, CompactionLevel::Initial),
                (7, CompactionLevel::FileNonOverlapped),
                (8, CompactionLevel::FileNonOverlapped),
            ]
        );

        // Verify the metrics
        assert_eq!(
            extract_byte_metrics(&compaction_input_file_bytes, shard_id),
            ExtractedByteMetrics {
                sample_count: 5,
                buckets_with_counts: vec![(BUCKET_500_KB, 4), (u64::MAX, 1)],
            }
        );

        // ------------------------------------------------
        // Verify the parquet file content

        // Compacted file with the later data
        let file1 = files.pop().unwrap();
        let batches = table.read_parquet_file(file1).await;
        assert_batches_sorted_eq!(
            &[
                "+-----------+------+------+------+-----------------------------+",
                "| field_int | tag1 | tag2 | tag3 | time                        |",
                "+-----------+------+------+------+-----------------------------+",
                "| 1601      |      | PA   | 15   | 1970-01-01T00:00:00.000030Z |",
                "| 21        |      | OH   | 21   | 1970-01-01T00:00:00.000036Z |",
                "+-----------+------+------+------+-----------------------------+",
            ],
            &batches
        );

        // Compacted file with the earlier data
        let file0 = files.pop().unwrap();
        let batches = table.read_parquet_file(file0).await;
        assert_batches_sorted_eq!(
            &[
                "+-----------+------+------+------+-----------------------------+",
                "| field_int | tag1 | tag2 | tag3 | time                        |",
                "+-----------+------+------+------+-----------------------------+",
                "| 10        | VT   |      |      | 1970-01-01T00:00:00.000006Z |",
                "| 10        | VT   |      |      | 1970-01-01T00:00:00.000010Z |",
                "| 1500      | WA   |      |      | 1970-01-01T00:00:00.000008Z |",
                "| 270       | UT   |      |      | 1970-01-01T00:00:00.000025Z |",
                "| 70        | UT   |      |      | 1970-01-01T00:00:00.000020Z |",
                "| 99        | OR   |      |      | 1970-01-01T00:00:00.000012Z |",
                "+-----------+------+------+------+-----------------------------+",
            ],
            &batches
        );
    }

    #[tokio::test]
    async fn medium_input_files_cant_split_dont_make_empty_file() {
        test_helpers::maybe_start_logging();

        let TestSetup {
            catalog,
            table,
            candidate_partition,
            parquet_files,
        } = test_setup().await;
        let compaction_input_file_bytes = metrics();
        let shard_id = candidate_partition.shard_id();

        let files_to_compact: Vec<_> = parquet_files.into_iter().take(5).collect();

        // If the split percentage is set to 100%, we'd create an empty parquet file, so this
        // needs to be special cased.
        let split_percentage = 100;

        compact_parquet_files(
            files_to_compact,
            candidate_partition,
            Arc::clone(&catalog.catalog),
            ParquetStorage::new(Arc::clone(&catalog.object_store), StorageId::from("iox")),
            Arc::clone(&catalog.exec),
            Arc::clone(&catalog.time_provider) as Arc<dyn TimeProvider>,
            &compaction_input_file_bytes,
            DEFAULT_MAX_DESIRED_FILE_SIZE_BYTES,
            DEFAULT_PERCENTAGE_MAX_FILE_SIZE,
            split_percentage,
            CompactionLevel::FileNonOverlapped,
        )
        .await
        .unwrap();

        // Should have 2 non-soft-deleted files:
        // - 1 large file not included in compaction
        // - 1 file from compacting everything-- even though this is the medium-sized case, the
        //   split percentage would make an empty file if we split, so don't do that.
        let mut files = catalog.list_by_table_not_to_delete(table.table.id).await;
        assert_eq!(files.len(), 2);
        let files_and_levels: Vec<_> = files
            .iter()
            .map(|f| (f.id.get(), f.compaction_level))
            .collect();
        // 1 large file not included in compaction,
        // 1 newly created CompactionLevel::FileNonOverlapped file as the result of
        // compaction and NOT splitting
        assert_eq!(
            files_and_levels,
            vec![
                (6, CompactionLevel::Initial),
                (7, CompactionLevel::FileNonOverlapped),
            ]
        );

        // Verify the metrics
        assert_eq!(
            extract_byte_metrics(&compaction_input_file_bytes, shard_id),
            ExtractedByteMetrics {
                sample_count: 5,
                buckets_with_counts: vec![(BUCKET_500_KB, 4), (u64::MAX, 1)],
            }
        );

        // ------------------------------------------------
        // Verify the parquet file content

        // Compacted file with all the data
        let file1 = files.pop().unwrap();
        let batches = table.read_parquet_file(file1).await;
        assert_batches_sorted_eq!(
            &[
                "+-----------+------+------+------+-----------------------------+",
                "| field_int | tag1 | tag2 | tag3 | time                        |",
                "+-----------+------+------+------+-----------------------------+",
                "| 10        | VT   |      |      | 1970-01-01T00:00:00.000006Z |",
                "| 10        | VT   |      |      | 1970-01-01T00:00:00.000010Z |",
                "| 1500      | WA   |      |      | 1970-01-01T00:00:00.000008Z |",
                "| 1601      |      | PA   | 15   | 1970-01-01T00:00:00.000030Z |",
                "| 21        |      | OH   | 21   | 1970-01-01T00:00:00.000036Z |",
                "| 270       | UT   |      |      | 1970-01-01T00:00:00.000025Z |",
                "| 70        | UT   |      |      | 1970-01-01T00:00:00.000020Z |",
                "| 99        | OR   |      |      | 1970-01-01T00:00:00.000012Z |",
                "+-----------+------+------+------+-----------------------------+",
            ],
            &batches
        );
    }

    #[tokio::test]
    async fn large_input_files_get_split_multiple_times() {
        test_helpers::maybe_start_logging();

        let TestSetup {
            catalog,
            table,
            candidate_partition,
            parquet_files,
        } = test_setup().await;
        let compaction_input_file_bytes = metrics();
        let shard_id = candidate_partition.shard_id();

        compact_parquet_files(
            parquet_files,
            candidate_partition,
            Arc::clone(&catalog.catalog),
            ParquetStorage::new(Arc::clone(&catalog.object_store), StorageId::from("iox")),
            Arc::clone(&catalog.exec),
            Arc::clone(&catalog.time_provider) as Arc<dyn TimeProvider>,
            &compaction_input_file_bytes,
            DEFAULT_MAX_DESIRED_FILE_SIZE_BYTES,
            DEFAULT_PERCENTAGE_MAX_FILE_SIZE,
            DEFAULT_SPLIT_PERCENTAGE,
            CompactionLevel::FileNonOverlapped,
        )
        .await
        .unwrap();

        // Should have 3 non-soft-deleted files:
        // - 3 files from compacting everything together then splitting.
        let mut files = catalog.list_by_table_not_to_delete(table.table.id).await;
        assert_eq!(files.len(), 3);
        let files_and_levels: Vec<_> = files
            .iter()
            .map(|f| (f.id.get(), f.compaction_level))
            .collect();
        // 3 newly created CompactionLevel::FileNonOverlapped file as the result of
        // compaction and splitting
        assert_eq!(
            files_and_levels,
            vec![
                (7, CompactionLevel::FileNonOverlapped),
                (8, CompactionLevel::FileNonOverlapped),
                (9, CompactionLevel::FileNonOverlapped),
            ]
        );

        // Verify the metrics
        assert_eq!(
            extract_byte_metrics(&compaction_input_file_bytes, shard_id),
            ExtractedByteMetrics {
                sample_count: 6,
                buckets_with_counts: vec![(BUCKET_500_KB, 4), (u64::MAX, 2)],
            }
        );

        // ------------------------------------------------
        // Verify the parquet file content

        // Compacted file with the latest data
        let file2 = files.pop().unwrap();
        let batches = table.read_parquet_file(file2).await;
        assert_batches_sorted_eq!(
            &[
                "+-----------+------+------+------+-----------------------------+",
                "| field_int | tag1 | tag2 | tag3 | time                        |",
                "+-----------+------+------+------+-----------------------------+",
                "| 210       |      | OH   | 21   | 1970-01-01T00:00:00.000136Z |",
                "+-----------+------+------+------+-----------------------------+",
            ],
            &batches
        );

        // Compacted file with the later data
        let file1 = files.pop().unwrap();
        let batches = table.read_parquet_file(file1).await;
        assert_batches_sorted_eq!(
            &[
                "+-----------+------+------+------+-----------------------------+",
                "| field_int | tag1 | tag2 | tag3 | time                        |",
                "+-----------+------+------+------+-----------------------------+",
                "| 10        | VT   |      |      | 1970-01-01T00:00:00.000068Z |",
                "+-----------+------+------+------+-----------------------------+",
            ],
            &batches
        );

        // Compacted file with the earlier data
        let file0 = files.pop().unwrap();
        let batches = table.read_parquet_file(file0).await;
        assert_batches_sorted_eq!(
            &[
                "+-----------+------+------+------+-----------------------------+",
                "| field_int | tag1 | tag2 | tag3 | time                        |",
                "+-----------+------+------+------+-----------------------------+",
                "| 10        | VT   |      |      | 1970-01-01T00:00:00.000006Z |",
                "| 10        | VT   |      |      | 1970-01-01T00:00:00.000010Z |",
                "| 1500      | WA   |      |      | 1970-01-01T00:00:00.000008Z |",
                "| 1601      |      | PA   | 15   | 1970-01-01T00:00:00.000030Z |",
                "| 21        |      | OH   | 21   | 1970-01-01T00:00:00.000036Z |",
                "| 270       | UT   |      |      | 1970-01-01T00:00:00.000025Z |",
                "| 70        | UT   |      |      | 1970-01-01T00:00:00.000020Z |",
                "| 99        | OR   |      |      | 1970-01-01T00:00:00.000012Z |",
                "+-----------+------+------+------+-----------------------------+",
            ],
            &batches
        );
    }

    #[tokio::test]
    async fn compact_final_no_splits_creates_one_level_2_file() {
        test_helpers::maybe_start_logging();

        let TestSetup {
            catalog,
            table,
            candidate_partition,
            parquet_files,
        } = test_setup().await;
        let compaction_input_file_bytes = metrics();
        let shard_id = candidate_partition.shard_id();

        let level_1_files = parquet_files
            .into_iter()
            .filter(|f| f.compaction_level() == CompactionLevel::FileNonOverlapped)
            .collect();

        // Compact all files given together into one level 2 file.
        compact_final_no_splits(
            level_1_files,
            candidate_partition,
            Arc::clone(&catalog.catalog),
            ParquetStorage::new(Arc::clone(&catalog.object_store), StorageId::from("iox")),
            Arc::clone(&catalog.exec),
            Arc::clone(&catalog.time_provider) as Arc<dyn TimeProvider>,
            &compaction_input_file_bytes,
            CompactionLevel::Final,
        )
        .await
        .unwrap();

        // Should have 1 level 2 file, not split at all, and 4 level 0 files.
        let mut files = catalog.list_by_table_not_to_delete(table.table.id).await;
        assert_eq!(files.len(), 5);
        let files_and_levels: Vec<_> = files
            .iter()
            .map(|f| (f.id.get(), f.compaction_level))
            .collect();
        assert_eq!(
            files_and_levels,
            vec![
                (2, CompactionLevel::Initial),
                (3, CompactionLevel::Initial),
                (5, CompactionLevel::Initial),
                (6, CompactionLevel::Initial),
                (7, CompactionLevel::Final),
            ]
        );

        let file = files.pop().unwrap();

        // Verify the metrics
        assert_eq!(
            extract_byte_metrics(&compaction_input_file_bytes, shard_id),
            ExtractedByteMetrics {
                sample_count: 2,
                buckets_with_counts: vec![(BUCKET_500_KB, 2)],
            }
        );

        // ------------------------------------------------
        // Verify the parquet file content

        let batches = table.read_parquet_file(file).await;
        assert_batches_sorted_eq!(
            &[
                "+-----------+------+------+------+-----------------------------+",
                "| field_int | tag1 | tag2 | tag3 | time                        |",
                "+-----------+------+------+------+-----------------------------+",
                "| 1601      |      | PA   | 15   | 1970-01-01T00:00:00.000030Z |",
                "| 21        |      | OH   | 21   | 1970-01-01T00:00:00.000036Z |",
                "| 88        | VT   |      |      | 1970-01-01T00:00:00.000010Z |",
                "| 99        | OR   |      |      | 1970-01-01T00:00:00.000012Z |",
                "+-----------+------+------+------+-----------------------------+",
            ],
            &batches
        );
    }

    #[derive(Debug, PartialEq)]
    struct ExtractedByteMetrics {
        sample_count: u64,
        buckets_with_counts: Vec<(u64, u64)>,
    }

    fn extract_byte_metrics(
        metric: &Metric<U64Histogram>,
        shard_id: ShardId,
    ) -> ExtractedByteMetrics {
        let attributes = Attributes::from([("shard_id", format!("{}", shard_id).into())]);

        let (sample_count, buckets_with_counts) =
            if let Some(observer) = metric.get_observer(&attributes) {
                let observer = observer.fetch();
                let mut buckets_with_counts: Vec<_> = observer
                    .buckets
                    .iter()
                    .filter_map(|o| {
                        if o.count == 0 {
                            None
                        } else {
                            Some((o.le, o.count))
                        }
                    })
                    .collect();
                buckets_with_counts.sort();
                (observer.sample_count(), buckets_with_counts)
            } else {
                (0, vec![])
            };

        ExtractedByteMetrics {
            sample_count,
            buckets_with_counts,
        }
    }
}
