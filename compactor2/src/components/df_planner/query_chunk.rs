//! QueryableParquetChunk for building query plan
use std::{any::Any, sync::Arc};

use data_types::{
    ChunkId, ChunkOrder, CompactionLevel, DeletePredicate, ParquetFile, PartitionId, TableSchema,
    TableSummary, Timestamp, Tombstone,
};
use datafusion::error::DataFusionError;
use iox_query::{
    exec::{stringset::StringSet, IOxSessionContext},
    util::create_basic_summary,
    QueryChunk, QueryChunkData, QueryChunkMeta,
};
use observability_deps::tracing::debug;
use parquet_file::{chunk::ParquetChunk, storage::ParquetStorage};
use predicate::{delete_predicate::tombstones_to_delete_predicates, Predicate};
use schema::{merge::SchemaMerger, sort::SortKey, Projection, Schema};
use uuid::Uuid;

/// QueryableParquetChunk that implements QueryChunk and QueryMetaChunk for building query plan
#[derive(Debug, Clone)]
pub struct QueryableParquetChunk {
    // Data of the parquet file
    data: Arc<ParquetChunk>,
    // Converted from tombstones.
    // We do not yet support delete but we need this to work with the straight QueryChunkMeta
    delete_predicates: Vec<Arc<DeletePredicate>>,
    partition_id: PartitionId,
    min_time: Timestamp,
    max_time: Timestamp,
    sort_key: Option<SortKey>,
    partition_sort_key: Option<SortKey>,
    compaction_level: CompactionLevel,
    target_level: CompactionLevel,
    summary: Arc<TableSummary>,
}

impl QueryableParquetChunk {
    /// Initialize a QueryableParquetChunk
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        partition_id: PartitionId,
        data: Arc<ParquetChunk>,
        deletes: &[Tombstone],
        min_time: Timestamp,
        max_time: Timestamp,
        sort_key: Option<SortKey>,
        partition_sort_key: Option<SortKey>,
        compaction_level: CompactionLevel,
        target_level: CompactionLevel,
    ) -> Self {
        let delete_predicates = tombstones_to_delete_predicates(deletes);
        let summary = Arc::new(create_basic_summary(
            data.rows() as u64,
            data.schema(),
            data.timestamp_min_max(),
        ));
        Self {
            data,
            delete_predicates,
            partition_id,
            min_time,
            max_time,
            sort_key,
            partition_sort_key,
            compaction_level,
            target_level,
            summary,
        }
    }

    /// Merge schema of the given chunks
    pub fn merge_schemas(chunks: &[Arc<dyn QueryChunk>]) -> Schema {
        let mut merger = SchemaMerger::new();
        for chunk in chunks {
            merger = merger.merge(chunk.schema()).expect("schemas compatible");
        }
        merger.build()
    }

    /// Return min time
    pub fn min_time(&self) -> i64 {
        self.min_time.get()
    }

    /// Return max time
    pub fn max_time(&self) -> i64 {
        self.max_time.get()
    }

    /// Return the parquet file's object store id
    pub fn object_store_id(&self) -> Uuid {
        self.data.object_store_id()
    }

    /// Return the creation time of the file
    pub fn created_at(&self) -> Timestamp {
        self.data.parquet_file().created_at
    }
}

impl QueryChunkMeta for QueryableParquetChunk {
    fn summary(&self) -> Arc<TableSummary> {
        Arc::clone(&self.summary)
    }

    fn schema(&self) -> &Schema {
        self.data.schema()
    }

    fn partition_sort_key(&self) -> Option<&SortKey> {
        self.partition_sort_key.as_ref()
    }

    fn partition_id(&self) -> PartitionId {
        self.partition_id
    }

    fn sort_key(&self) -> Option<&SortKey> {
        self.sort_key.as_ref()
    }

    fn delete_predicates(&self) -> &[Arc<DeletePredicate>] {
        self.delete_predicates.as_ref()
    }
}

impl QueryChunk for QueryableParquetChunk {
    // This function is needed to distinguish the ParquetChunks further if they happen to have the
    // same creation order.
    // Ref: chunks.sort_unstable_by_key(|c| (c.order(), c.id())); in provider.rs
    fn id(&self) -> ChunkId {
        // When we need the order to split overlapped chunks, the ChunkOrder is already different.
        // ChunkId is used as tiebreaker does not matter much, so use the object store id
        self.object_store_id().into()
    }

    /// Returns true if the chunk may contain a duplicate "primary key" within itself
    fn may_contain_pk_duplicates(&self) -> bool {
        // Data of a parquet file has no duplicates
        false
    }

    /// Returns a set of Strings with column names from the specified
    /// table that have at least one row that matches `predicate`, if
    /// the predicate can be evaluated entirely on the metadata of
    /// this Chunk. Returns `None` otherwise
    fn column_names(
        &self,
        _ctx: IOxSessionContext,
        _predicate: &Predicate,
        _columns: Projection<'_>,
    ) -> Result<Option<StringSet>, DataFusionError> {
        Ok(None)
    }

    /// Return a set of Strings containing the distinct values in the
    /// specified columns. If the predicate can be evaluated entirely
    /// on the metadata of this Chunk. Returns `None` otherwise
    ///
    /// The requested columns must all have String type.
    fn column_values(
        &self,
        _ctx: IOxSessionContext,
        _column_name: &str,
        _predicate: &Predicate,
    ) -> Result<Option<StringSet>, DataFusionError> {
        Ok(None)
    }

    fn data(&self) -> QueryChunkData {
        QueryChunkData::Parquet(self.data.parquet_exec_input())
    }

    /// Returns chunk type
    fn chunk_type(&self) -> &str {
        "QueryableParquetChunk"
    }

    // Order of the chunk so they can be deduplicated correctly
    fn order(&self) -> ChunkOrder {
        // TODO: If we chnage this design specified in driver.rs's compact functions, we will need to refine this
        // Currently, we only compact files of level_n with level_n+1 and produce level_n+1 files,
        // and with the strictly design that:
        //    . Level-0 files can overlap with any files.
        //    . Level-N files (N > 0) cannot overlap with any files in the same level.
        //    . For Level-0 files, we always pick the smaller `created_at` files to compact (with
        //      each other and overlapped L1 files) first.
        //    . Level-N+1 files are results of compacting Level-N and/or Level-N+1 files, their `created_at`
        //      can be after the `created_at` of other Level-N files but they may include data loaded before
        //      the other Level-N files. Hence we should never use `created_at` of Level-N+1 files to order
        //      them with Level-N files.
        //    . We can only compact different sets of files of the same partition concurrently into the same target_level.
        // We can use the following rules to set order of the chunk of its (compaction_level, target_level) as follows:
        //    . compaction_level < target_level : the order is `created_at`
        //    . compaction_level == target_level : order is 0 to make sure it is in the front of the ordered list.
        //      This means that the chunk of `compaction_level == target_level` will be in arbitrary order and will be
        //      fine as long as they are in front of the chunks of `compaction_level < target_level`

        match (self.compaction_level, self.target_level) {
            (CompactionLevel::Initial, CompactionLevel::FileNonOverlapped)
            | (CompactionLevel::FileNonOverlapped, CompactionLevel::Final) => {
                ChunkOrder::new(self.created_at().get())
            }
            (CompactionLevel::FileNonOverlapped, CompactionLevel::FileNonOverlapped)
            | (CompactionLevel::Final, CompactionLevel::Final) => ChunkOrder::new(0),
            _ => {
                panic!(
                    "Invalid compaction level combination: ({:?}, {:?})",
                    self.compaction_level, self.target_level
                );
            }
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Convert to a QueryableParquetChunk
pub fn to_queryable_parquet_chunk(
    file: ParquetFile,
    store: ParquetStorage,
    table_schema: &TableSchema,
    partition_sort_key: Option<SortKey>,
    target_level: CompactionLevel,
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
    let sort_key = partition_sort_key
        .as_ref()
        .map(|sk| sk.filter_to(&pk, file.partition_id.get()));

    let partition_id = file.partition_id;
    let min_time = file.min_time;
    let max_time = file.max_time;
    let compaction_level = file.compaction_level;

    // Make it debug for it to show up in prod's initial setup
    let uuid = file.object_store_id;
    debug!(
        parquet_file_id = file.id.get(),
        parquet_file_namespace_id = file.namespace_id.get(),
        parquet_file_table_id = file.table_id.get(),
        parquet_file_partition_id = file.partition_id.get(),
        parquet_file_object_store_id = uuid.to_string().as_str(),
        "built parquet chunk from metadata"
    );

    let parquet_chunk = ParquetChunk::new(Arc::new(file), schema, store);
    QueryableParquetChunk::new(
        partition_id,
        Arc::new(parquet_chunk),
        &[],
        min_time,
        max_time,
        sort_key,
        partition_sort_key,
        compaction_level,
        target_level,
    )
}
