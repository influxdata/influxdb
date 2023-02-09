//! QueryableParquetChunk for building query plan
use std::{any::Any, sync::Arc};

use data_types::{ChunkId, ChunkOrder, DeletePredicate, PartitionId, TableSummary, Tombstone};
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

use crate::{partition_info::PartitionInfo, plan_ir::FileIR};

/// QueryableParquetChunk that implements QueryChunk and QueryMetaChunk for building query plan
#[derive(Debug, Clone)]
pub struct QueryableParquetChunk {
    // Data of the parquet file
    data: Arc<ParquetChunk>,
    // Converted from tombstones.
    // We do not yet support delete but we need this to work with the straight QueryChunkMeta
    delete_predicates: Vec<Arc<DeletePredicate>>,
    partition_id: PartitionId,
    sort_key: Option<SortKey>,
    partition_sort_key: Option<SortKey>,
    order: ChunkOrder,
    summary: Arc<TableSummary>,
}

impl QueryableParquetChunk {
    /// Initialize a QueryableParquetChunk
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        partition_id: PartitionId,
        data: Arc<ParquetChunk>,
        deletes: &[Tombstone],
        sort_key: Option<SortKey>,
        partition_sort_key: Option<SortKey>,
        order: ChunkOrder,
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
            sort_key,
            partition_sort_key,
            order,
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

    /// Return the parquet file's object store id
    pub fn object_store_id(&self) -> Uuid {
        self.data.object_store_id()
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
        self.order
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub fn to_query_chunks(
    files: &[FileIR],
    partition_info: &PartitionInfo,
    store: ParquetStorage,
) -> Vec<Arc<dyn QueryChunk>> {
    files
        .iter()
        .map(|file| {
            Arc::new(to_queryable_parquet_chunk(
                file,
                partition_info,
                store.clone(),
            )) as _
        })
        .collect()
}

/// Convert to a QueryableParquetChunk
fn to_queryable_parquet_chunk(
    file: &FileIR,
    partition_info: &PartitionInfo,
    store: ParquetStorage,
) -> QueryableParquetChunk {
    let column_id_lookup = partition_info.table_schema.column_id_map();
    let selection: Vec<_> = file
        .file
        .column_set
        .iter()
        .flat_map(|id| column_id_lookup.get(id).copied())
        .collect();
    let table_schema: Schema = partition_info
        .table_schema
        .as_ref()
        .clone()
        .try_into()
        .expect("table schema is broken");
    let schema = table_schema
        .select_by_names(&selection)
        .expect("schema in-sync");
    let pk = schema.primary_key();
    let sort_key = partition_info
        .sort_key
        .as_ref()
        .map(|sk| sk.filter_to(&pk, partition_info.partition_id.get()));

    let partition_id = partition_info.partition_id;

    // Make it debug for it to show up in prod's initial setup
    let uuid = file.file.object_store_id;
    debug!(
        parquet_file_id = file.file.id.get(),
        parquet_file_namespace_id = file.file.namespace_id.get(),
        parquet_file_table_id = file.file.table_id.get(),
        parquet_file_partition_id = file.file.partition_id.get(),
        parquet_file_object_store_id = uuid.to_string().as_str(),
        "built parquet chunk from metadata"
    );

    let parquet_chunk = ParquetChunk::new(Arc::new(file.file.clone()), schema, store);
    QueryableParquetChunk::new(
        partition_id,
        Arc::new(parquet_chunk),
        &[],
        sort_key,
        partition_info.sort_key.clone(),
        file.order,
    )
}
