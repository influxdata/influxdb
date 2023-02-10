//! Queryable Compactor Data

use data_types::{
    ChunkId, ChunkOrder, CompactionLevel, DeletePredicate, PartitionId, SequenceNumber,
    TableSummary, Timestamp, Tombstone,
};
use datafusion::error::DataFusionError;
use iox_query::{
    exec::{stringset::StringSet, IOxSessionContext},
    util::create_basic_summary,
    QueryChunk, QueryChunkData, QueryChunkMeta,
};
use parquet_file::chunk::ParquetChunk;
use predicate::{delete_predicate::tombstones_to_delete_predicates, Predicate};
use schema::{merge::SchemaMerger, sort::SortKey, Projection, Schema};
use std::{any::Any, sync::Arc};
use uuid::Uuid;

/// QueryableParquetChunk that implements QueryChunk and QueryMetaChunk for building query plan
#[derive(Debug, Clone)]
pub struct QueryableParquetChunk {
    data: Arc<ParquetChunk>,                      // data of the parquet file
    delete_predicates: Vec<Arc<DeletePredicate>>, // converted from tombstones
    partition_id: PartitionId,
    max_sequence_number: SequenceNumber,
    min_time: Timestamp,
    max_time: Timestamp,
    sort_key: Option<SortKey>,
    partition_sort_key: Option<SortKey>,
    compaction_level: CompactionLevel,
    /// The compaction level that this operation will be when finished. Chunks from files that have
    /// the same level as this should get chunk order 0 so that files at a lower compaction level
    /// (and thus created later) should have priority in deduplication.
    ///
    /// That is:
    ///
    /// * When compacting L0 + L1, the target level is L1. L0 files should have priority, so all L1
    ///   files should have chunk order 0 to be sorted first.
    /// * When compacting L1 + L2, the target level is L2. L1 files should have priority, so all L2
    ///   files should have chunk order 0 to be sorted first.
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
        max_sequence_number: SequenceNumber,
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
            max_sequence_number,
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

    /// Return max sequence number
    pub fn max_sequence_number(&self) -> SequenceNumber {
        self.max_sequence_number
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
    // Note: The order of this QueryableParquetChunk is the parquet file's min_sequence_number which
    // will be the same for parquet files of splitted compacted data.
    //
    // This function returns the parquet file's min_time which will be always different for the
    // parquet files of same order/min_sequence_number and is good to order the parquet file
    fn id(&self) -> ChunkId {
        // When we need the order to split overlapped chunks, the ChunkOrder is already different.
        // ChunkId is used as tiebreaker does not matter much, so use the object store id
        self.object_store_id().into()
    }

    /// Returns true if the chunk may contain a duplicate "primary
    /// key" within itself
    fn may_contain_pk_duplicates(&self) -> bool {
        // data within this parquet chunk was deduplicated
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
        use CompactionLevel::*;
        match (self.target_level, self.compaction_level) {
            // Files of the same level as what they're being compacting into were created earlier,
            // so they should be sorted first so that files created later that haven't yet been
            // compacted to this level will have priority when resolving duplicates.
            (FileNonOverlapped, FileNonOverlapped) => ChunkOrder::new(0),
            (Final, Final) => ChunkOrder::new(0),

            // Files that haven't yet been compacted to the target level were created later and
            // should be sorted based on their max sequence number.
            (FileNonOverlapped, Initial) => ChunkOrder::new(self.created_at().get()),
            (Final, FileNonOverlapped) => ChunkOrder::new(self.created_at().get()),

            // These combinations of target compaction level and file compaction level are
            // invalid in this context given the current compaction algorithm.
            (Initial, _) => panic!("Can't compact into CompactionLevel::Initial"),
            (FileNonOverlapped, Final) => panic!(
                "Can't compact CompactionLevel::Final into CompactionLevel::FileNonOverlapped"
            ),
            (Final, Initial) => {
                panic!("Can't compact CompactionLevel::Initial into CompactionLevel::Final")
            }
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use data_types::ColumnType;
    use iox_tests::{TestCatalog, TestParquetFileBuilder};
    use iox_time::{SystemProvider, TimeProvider};
    use parquet_file::storage::{ParquetStorage, StorageId};

    async fn test_setup(
        compaction_level: CompactionLevel,
        target_level: CompactionLevel,
        created_at: iox_time::Time,
    ) -> QueryableParquetChunk {
        let catalog = TestCatalog::new();
        let ns = catalog.create_namespace_1hr_retention("ns").await;
        let shard = ns.create_shard(1).await;
        let table = ns.create_table("table").await;
        table.create_column("field_int", ColumnType::I64).await;
        table.create_column("tag1", ColumnType::Tag).await;
        table.create_column("time", ColumnType::Time).await;

        let partition = table
            .with_shard(&shard)
            .create_partition("2022-07-13")
            .await;

        let lp = vec!["table,tag1=WA field_int=1000i 8000"].join("\n");
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp)
            .with_compaction_level(compaction_level)
            .with_creation_time(created_at);
        let file = partition.create_parquet_file(builder).await;
        let parquet_file = Arc::new(file.parquet_file);

        let parquet_chunk = Arc::new(ParquetChunk::new(
            Arc::clone(&parquet_file),
            table.schema().await,
            ParquetStorage::new(Arc::clone(&catalog.object_store), StorageId::from("iox")),
        ));

        QueryableParquetChunk::new(
            partition.partition.id,
            parquet_chunk,
            &[],
            parquet_file.max_sequence_number,
            parquet_file.min_time,
            parquet_file.max_time,
            None,
            None,
            parquet_file.compaction_level,
            target_level,
        )
    }

    #[tokio::test]
    async fn chunk_order_is_created_at_when_compaction_level_0_and_target_level_1() {
        let tp = SystemProvider::new();
        let time = tp.hours_ago(1);
        let chunk = test_setup(
            CompactionLevel::Initial,
            CompactionLevel::FileNonOverlapped,
            time,
        )
        .await;

        assert_eq!(chunk.order(), ChunkOrder::new(time.timestamp_nanos()));
    }

    #[tokio::test]
    async fn chunk_order_is_0_when_compaction_level_1_and_target_level_1() {
        let tp = SystemProvider::new();
        let time = tp.hours_ago(1);
        let chunk = test_setup(
            CompactionLevel::FileNonOverlapped,
            CompactionLevel::FileNonOverlapped,
            time,
        )
        .await;

        assert_eq!(chunk.order(), ChunkOrder::new(0));
    }

    #[tokio::test]
    async fn chunk_order_is_created_at_when_compaction_level_1_and_target_level_2() {
        let tp = SystemProvider::new();
        let time = tp.hours_ago(1);
        let chunk = test_setup(
            CompactionLevel::FileNonOverlapped,
            CompactionLevel::Final,
            time,
        )
        .await;

        assert_eq!(chunk.order(), ChunkOrder::new(time.timestamp_nanos()));
    }

    #[tokio::test]
    async fn chunk_order_is_0_when_compaction_level_2_and_target_level_2() {
        let tp = SystemProvider::new();
        let time = tp.hours_ago(1);
        let chunk = test_setup(CompactionLevel::Final, CompactionLevel::Final, time).await;

        assert_eq!(chunk.order(), ChunkOrder::new(0));
    }
}
