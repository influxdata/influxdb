use crate::{compact::PartitionCompactionCandidateWithInfo, query::QueryableParquetChunk};
use data_types::{CompactionLevel, ParquetFile, PartitionId, TableSchema};
use datafusion::error::DataFusionError;
use iox_catalog::interface::Catalog;
use iox_query::{
    exec::{Executor, ExecutorType},
    frontend::reorg::ReorgPlanner,
    QueryChunk,
};
use iox_time::TimeProvider;
use observability_deps::tracing::*;
use parquet_file::{chunk::ParquetChunk, metadata::IoxMetadata, storage::ParquetStorage};
use schema::{sort::SortKey, Schema};
use snafu::{ensure, ResultExt, Snafu};
use std::{
    cmp::{max, min},
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

    #[snafu(display(
        "Total size of selected files for {} is {total_size}, compacting that much is \
         not yet implemented.", partition_id.get()
    ))]
    TooMuchData {
        total_size: i64,
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

    #[snafu(display("Could not serialize and persist record batches {}", source))]
    Persist {
        source: parquet_file::storage::UploadError,
    },

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

/// 99.99% of current partitions are under this size. Temporarily only compact this amount of data
/// into one file, as that is simpler than splitting into multiple files.
const TEMPORARY_COMPACTION_MAX_BYTES_LIMIT: i64 = 30 * 1024 * 1024;

// Compact the given parquet files received from `filter_parquet_files` into one stream
pub(crate) async fn compact_parquet_files(
    files: Vec<ParquetFile>,
    partition: &PartitionCompactionCandidateWithInfo,
    // The global catalog for schema, parquet files and tombstones
    catalog: Arc<dyn Catalog>,
    // Object store for reading input parquet files and writing compacted parquet files
    store: ParquetStorage,
    // Executor for running queries, compacting, and persisting
    exec: Arc<Executor>,
    time_provider: Arc<dyn TimeProvider>,
) -> Result<(), Error> {
    let partition_id = partition.id();

    let num_files = files.len();
    ensure!(
        num_files >= 2,
        NotEnoughParquetFilesSnafu {
            num_files,
            partition_id
        }
    );

    // Find the total size of all files. For now, only compact if the total size is under 30MB.
    let total_size: i64 = files.iter().map(|f| f.file_size_bytes).sum();
    ensure!(
        total_size < TEMPORARY_COMPACTION_MAX_BYTES_LIMIT,
        TooMuchDataSnafu {
            total_size,
            partition_id
        }
    );

    let num_level_1 = files
        .iter()
        .filter(|f| f.compaction_level == CompactionLevel::FileNonOverlapped)
        .count();
    let num_level_0 = num_files - num_level_1;
    debug!(
        ?partition_id,
        num_files, num_level_1, num_level_0, "compact files to stream"
    );

    // Collect all the parquet file IDs, to be able to set their catalog records to be
    // deleted. These should already be unique, no need to dedupe.
    let original_parquet_file_ids: Vec<_> = files.iter().map(|f| f.id).collect();

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
        .filter_to(&merged_schema.primary_key());

    // Build compact logical plan, compacting everything into one file
    let ctx = exec.new_context(ExecutorType::Reorg);
    let plan = ReorgPlanner::new(ctx.child_ctx("ReorgPlanner"))
        .compact_plan(Arc::clone(&merged_schema), query_chunks, sort_key.clone())
        .context(CompactLogicalPlanSnafu)?;

    let ctx = exec.new_context(ExecutorType::Reorg);
    let physical_plan = ctx
        .create_physical_plan(&plan)
        .await
        .context(CompactPhysicalPlanSnafu)?;

    let data = ctx
        .execute_stream(Arc::clone(&physical_plan))
        .await
        .context(ExecuteCompactPlanSnafu)?;

    let meta = IoxMetadata {
        object_store_id: Uuid::new_v4(),
        creation_timestamp: time_provider.now(),
        sequencer_id: partition.sequencer_id(),
        namespace_id: partition.namespace_id(),
        namespace_name: partition.namespace.name.clone().into(),
        table_id: partition.table.id,
        table_name: partition.table.name.clone().into(),
        partition_id,
        partition_key: partition.partition_key.clone(),
        max_sequence_number,
        compaction_level: CompactionLevel::FileNonOverlapped,
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
    let (parquet_meta, file_size) = store.upload(data, &meta).await.context(PersistSnafu)?;

    debug!(?partition_id, %object_store_id, "file uploaded to object store");

    let parquet_file = meta.to_parquet_file(partition_id, file_size, &parquet_meta, |name| {
        partition
            .table_schema
            .columns
            .get(name)
            .expect("unknown column")
            .id
    });

    let mut txn = catalog
        .start_transaction()
        .await
        .context(TransactionSnafu)?;

    debug!(
        ?partition_id,
        %object_store_id,
        "updating catalog"
    );

    // Create the new parquet file in the catalog first
    txn.parquet_files()
        .create(parquet_file)
        .await
        .context(UpdateSnafu)?;

    // Mark input files for deletion
    for original_parquet_file_id in original_parquet_file_ids {
        txn.parquet_files()
            .flag_for_delete(original_parquet_file_id)
            .await
            .context(FlagForDeleteSnafu)?;
    }

    txn.commit().await.context(TransactionCommitSnafu)?;

    info!(?partition_id, %object_store_id, "compaction complete");

    Ok(())
}

/// Convert ParquetFile to a QueryableParquetChunk
fn to_queryable_parquet_chunk(
    file: ParquetFile,
    store: ParquetStorage,
    table_name: String,
    table_schema: &TableSchema,
    partition_sort_key: Option<SortKey>,
) -> QueryableParquetChunk {
    let column_id_lookup = table_schema.column_id_map();
    let selection: Vec<_> = file
        .column_set
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
    let sort_key = partition_sort_key.as_ref().map(|sk| sk.filter_to(&pk));
    let file = Arc::new(file);

    let parquet_chunk = ParquetChunk::new(Arc::clone(&file), Arc::new(schema), store);

    trace!(
        parquet_file_id=?file.id,
        parquet_file_sequencer_id=?file.sequencer_id,
        parquet_file_namespace_id=?file.namespace_id,
        parquet_file_table_id=?file.table_id,
        parquet_file_partition_id=?file.partition_id,
        parquet_file_object_store_id=?file.object_store_id,
        "built parquet chunk from metadata"
    );

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
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::record_batch::RecordBatch;
    use arrow_util::assert_batches_sorted_eq;
    use data_types::{ColumnType, PartitionParam};
    use iox_tests::util::{TestCatalog, TestParquetFileBuilder, TestTable};
    use parquet_file::ParquetFilePath;
    use test_helpers::assert_error;

    struct TestSetup {
        catalog: Arc<TestCatalog>,
        table: Arc<TestTable>,
        candidate_partition: PartitionCompactionCandidateWithInfo,
        parquet_files: Vec<ParquetFile>,
    }

    async fn test_setup() -> TestSetup {
        let catalog = TestCatalog::new();
        let ns = catalog.create_namespace("ns").await;
        let sequencer = ns.create_sequencer(1).await;
        let table = ns.create_table("table").await;
        table.create_column("field_int", ColumnType::I64).await;
        table.create_column("tag1", ColumnType::Tag).await;
        table.create_column("tag2", ColumnType::Tag).await;
        table.create_column("tag3", ColumnType::Tag).await;
        table.create_column("time", ColumnType::Time).await;
        let table_schema = table.catalog_schema().await;

        let partition = table
            .with_sequencer(&sequencer)
            .create_partition("2022-07-13")
            .await;

        // The sort key comes from the catalog and should be the union of all tags the
        // ingester has seen
        let sort_key = SortKey::from_columns(["tag1", "tag2", "tag3", "time"]);
        let partition = partition.update_sort_key(sort_key.clone()).await;

        let candidate_partition = PartitionCompactionCandidateWithInfo {
            table: Arc::new(table.table.clone()),
            table_schema: Arc::new(table_schema),
            namespace: Arc::new(ns.namespace.clone()),
            candidate: PartitionParam {
                partition_id: partition.partition.id,
                sequencer_id: partition.partition.sequencer_id,
                namespace_id: ns.namespace.id,
                table_id: partition.partition.table_id,
            },
            sort_key: partition.partition.sort_key(),
            partition_key: partition.partition.partition_key.clone(),
        };

        let lp = vec![
            "table,tag2=PA,tag3=15 field_int=1601i 30000",
            "table,tag2=OH,tag3=21 field_int=21i 36000",
        ]
        .join("\n");
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp)
            .with_compaction_level(CompactionLevel::FileNonOverlapped); // Prev compaction
        let level_1_file = partition.create_parquet_file(builder).await;

        let lp = vec![
            "table,tag1=WA field_int=1000i 8000", // will be eliminated due to duplicate
            "table,tag1=VT field_int=10i 10000",
            "table,tag1=UT field_int=70i 20000",
        ]
        .join("\n");
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp)
            .with_max_seq(1);
        let level_0_max_seq_1 = partition.create_parquet_file(builder).await;

        let lp = vec![
            "table,tag1=WA field_int=1500i 8000", // latest duplicate and kept
            "table,tag1=VT field_int=10i 6000",
            "table,tag1=UT field_int=270i 25000",
        ]
        .join("\n");
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp)
            .with_max_seq(2);
        let level_0_max_seq_2 = partition.create_parquet_file(builder).await;

        let lp = vec!["table,tag2=OH,tag3=21 field_int=21i 36000"].join("\n");
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp)
            .with_file_size_bytes(40 * 1024 * 1024); // Really large file
        let large_file = partition.create_parquet_file(builder).await;

        // Order here is important! The Level 1 files are first, then Level 0 files in max seq num
        // ascending order, as filter_parquet_files would create them
        let parquet_files = vec![
            level_1_file.parquet_file,
            level_0_max_seq_1.parquet_file,
            level_0_max_seq_2.parquet_file,
            large_file.parquet_file,
        ];

        TestSetup {
            catalog,
            table,
            candidate_partition,
            parquet_files,
        }
    }

    #[tokio::test]
    async fn no_input_files_is_an_error() {
        test_helpers::maybe_start_logging();

        let TestSetup {
            catalog,
            candidate_partition,
            ..
        } = test_setup().await;

        let files = vec![];
        let result = compact_parquet_files(
            files,
            &candidate_partition,
            Arc::clone(&catalog.catalog),
            ParquetStorage::new(Arc::clone(&catalog.object_store)),
            Arc::clone(&catalog.exec),
            Arc::clone(&catalog.time_provider) as Arc<dyn TimeProvider>,
        )
        .await;
        assert_error!(result, Error::NotEnoughParquetFiles { num_files: 0, .. });
    }

    #[tokio::test]
    async fn one_input_file_is_an_error() {
        test_helpers::maybe_start_logging();

        let TestSetup {
            catalog,
            candidate_partition,
            mut parquet_files,
            ..
        } = test_setup().await;

        let result = compact_parquet_files(
            vec![parquet_files.pop().unwrap()],
            &candidate_partition,
            Arc::clone(&catalog.catalog),
            ParquetStorage::new(Arc::clone(&catalog.object_store)),
            Arc::clone(&catalog.exec),
            Arc::clone(&catalog.time_provider) as Arc<dyn TimeProvider>,
        )
        .await;
        assert_error!(result, Error::NotEnoughParquetFiles { num_files: 1, .. });
    }

    #[tokio::test]
    async fn multiple_files_get_compacted() {
        test_helpers::maybe_start_logging();

        let TestSetup {
            catalog,
            table,
            candidate_partition,
            parquet_files,
        } = test_setup().await;

        compact_parquet_files(
            parquet_files.into_iter().take(3).collect(),
            &candidate_partition,
            Arc::clone(&catalog.catalog),
            ParquetStorage::new(Arc::clone(&catalog.object_store)),
            Arc::clone(&catalog.exec),
            Arc::clone(&catalog.time_provider) as Arc<dyn TimeProvider>,
        )
        .await
        .unwrap();

        // Should have 2 non-soft-deleted files:
        //
        // - the one newly created after compacting
        // - the large one not included in this compaction operation
        let mut files = catalog
            .list_by_table_not_to_delete(candidate_partition.table.id)
            .await;
        assert_eq!(files.len(), 2);
        // 1 large file not included in compaction
        assert_eq!(
            (files[0].id.get(), files[0].compaction_level),
            (4, CompactionLevel::Initial)
        );
        // 1 newly created CompactionLevel::FileNonOverlapped file as the result of
        // compaction
        assert_eq!(
            (files[1].id.get(), files[1].compaction_level),
            (5, CompactionLevel::FileNonOverlapped)
        );

        // ------------------------------------------------
        // Verify the parquet file content

        // Compacted file
        let file1 = files.pop().unwrap();
        let batches = read_parquet_file(&table, file1).await;
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
                "+-----------+------+------+------+-----------------------------+",
            ],
            &batches
        );
    }

    #[tokio::test]
    async fn too_large_input_files_is_an_error() {
        test_helpers::maybe_start_logging();

        let TestSetup {
            catalog,
            candidate_partition,
            parquet_files,
            ..
        } = test_setup().await;

        let result = compact_parquet_files(
            parquet_files,
            &candidate_partition,
            Arc::clone(&catalog.catalog),
            ParquetStorage::new(Arc::clone(&catalog.object_store)),
            Arc::clone(&catalog.exec),
            Arc::clone(&catalog.time_provider) as Arc<dyn TimeProvider>,
        )
        .await;
        assert_error!(result, Error::TooMuchData { .. });
    }

    async fn read_parquet_file(table: &Arc<TestTable>, file: ParquetFile) -> Vec<RecordBatch> {
        let storage = ParquetStorage::new(table.catalog.object_store());

        // get schema
        let table_catalog_schema = table.catalog_schema().await;
        let column_id_lookup = table_catalog_schema.column_id_map();
        let table_schema = table.schema().await;
        let selection: Vec<_> = file
            .column_set
            .iter()
            .map(|id| *column_id_lookup.get(id).unwrap())
            .collect();
        let schema = table_schema.select_by_names(&selection).unwrap();

        let path: ParquetFilePath = (&file).into();
        let rx = storage.read_all(schema.as_arrow(), &path).unwrap();
        datafusion::physical_plan::common::collect(rx)
            .await
            .unwrap()
    }
}
