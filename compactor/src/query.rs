//! Queryable Compactor Data

use std::sync::Arc;

use data_types2::{
    tombstones_to_delete_predicates, ChunkAddr, ChunkId, ChunkOrder, DeletePredicate,
    SequenceNumber, TableSummary, Tombstone,
};
use datafusion::physical_plan::SendableRecordBatchStream;
use observability_deps::tracing::trace;
use parquet_file::{chunk::ParquetChunk, metadata::IoxMetadata};
use predicate::{Predicate, PredicateMatch};
use query::{
    exec::{stringset::StringSet, IOxSessionContext},
    QueryChunk, QueryChunkError, QueryChunkMeta,
};
use schema::{merge::SchemaMerger, selection::Selection, sort::SortKey, Schema};
use snafu::{ResultExt, Snafu};

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub enum Error {
    #[snafu(display("Failed to read parquet: {}", source))]
    ReadParquet { source: parquet_file::chunk::Error },

    #[snafu(display(
        "Error reading IOx Metadata from Parquet IoxParquetMetadata: {}",
        source
    ))]
    ReadParquetMeta {
        source: parquet_file::metadata::Error,
    },
}

/// A specialized `Error` for Compactor's query errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// QueryableParquetChunk that implements QueryChunk and QueryMetaChunk for building query plan
#[derive(Debug, Clone)]
pub struct QueryableParquetChunk {
    data: Arc<ParquetChunk>,                      // data of the parquet file
    iox_metadata: Arc<IoxMetadata>,               // metadata of the parquet file
    delete_predicates: Vec<Arc<DeletePredicate>>, // converted from tombstones
    table_name: String,                           // needed to build query plan
}

impl QueryableParquetChunk {
    /// Initialize a QueryableParquetChunk
    pub fn new(
        table_name: impl Into<String>,
        data: Arc<ParquetChunk>,
        iox_metadata: Arc<IoxMetadata>,
        deletes: &[Tombstone],
    ) -> Self {
        let delete_predicates = tombstones_to_delete_predicates(deletes);
        Self {
            data,
            iox_metadata,
            delete_predicates,
            table_name: table_name.into(),
        }
    }

    /// Merge schema of the given chunks
    pub fn merge_schemas(chunks: &[Arc<dyn QueryChunk>]) -> Arc<Schema> {
        let mut merger = SchemaMerger::new();
        for chunk in chunks {
            merger = merger.merge(&chunk.schema()).expect("schemas compatible");
        }
        Arc::new(merger.build())
    }

    /// Return min sequence number
    pub fn min_sequence_number(&self) -> SequenceNumber {
        self.iox_metadata.min_sequence_number
    }

    /// Return max sequence number
    pub fn max_sequence_number(&self) -> SequenceNumber {
        self.iox_metadata.max_sequence_number
    }

    /// Return min time
    pub fn min_time(&self) -> i64 {
        self.iox_metadata.time_of_first_write.timestamp_nanos()
    }

    /// Return max time
    pub fn max_time(&self) -> i64 {
        self.iox_metadata.time_of_last_write.timestamp_nanos()
    }
}

impl QueryChunkMeta for QueryableParquetChunk {
    fn summary(&self) -> Option<&TableSummary> {
        None
    }

    fn schema(&self) -> Arc<Schema> {
        self.data.schema()
    }

    fn sort_key(&self) -> Option<&SortKey> {
        None // TODO: return the sortkey when it is available in the parquet file #3968
    }

    fn delete_predicates(&self) -> &[Arc<DeletePredicate>] {
        self.delete_predicates.as_ref()
    }
}

impl QueryChunk for QueryableParquetChunk {
    // Todo: This function should not be used in this NG chunk context
    // For now, since we also use scan for both OG and NG, the chunk id
    // is used as second key in build_deduplicate_plan_for_overlapped_chunks
    // to sort the chunk to deduplicate them correctly.
    // Since we make the first key, order, always different, it is fine
    // to have the second key the sames and always 0
    fn id(&self) -> ChunkId {
        // always return id 0 for debugging mode and with reason above
        ChunkId::new_test(0)
    }

    // This function should not be used in this context
    fn addr(&self) -> ChunkAddr {
        unimplemented!()
    }

    /// Returns the name of the table stored in this chunk
    fn table_name(&self) -> &str {
        &self.table_name
    }

    /// Returns true if the chunk may contain a duplicate "primary
    /// key" within itself
    fn may_contain_pk_duplicates(&self) -> bool {
        // data within this parquet chunk was deduplicated
        false
    }

    /// Returns the result of applying the `predicate` to the chunk
    /// using an efficient, but inexact method, based on metadata.
    ///
    /// NOTE: This method is suitable for calling during planning, and
    /// may return PredicateMatch::Unknown for certain types of
    /// predicates.
    fn apply_predicate_to_metadata(
        &self,
        _predicate: &Predicate,
    ) -> Result<PredicateMatch, QueryChunkError> {
        Ok(PredicateMatch::Unknown)
    }

    /// Returns a set of Strings with column names from the specified
    /// table that have at least one row that matches `predicate`, if
    /// the predicate can be evaluated entirely on the metadata of
    /// this Chunk. Returns `None` otherwise
    fn column_names(
        &self,
        _ctx: IOxSessionContext,
        _predicate: &Predicate,
        _columns: Selection<'_>,
    ) -> Result<Option<StringSet>, QueryChunkError> {
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
    ) -> Result<Option<StringSet>, QueryChunkError> {
        Ok(None)
    }

    /// Provides access to raw `QueryChunk` data as an
    /// asynchronous stream of `RecordBatch`es filtered by a *required*
    /// predicate. Note that not all chunks can evaluate all types of
    /// predicates and this function will return an error
    /// if requested to evaluate with a predicate that is not supported
    ///
    /// This is the analog of the `TableProvider` in DataFusion
    ///
    /// The reason we can't simply use the `TableProvider` trait
    /// directly is that the data for a particular Table lives in
    /// several chunks within a partition, so there needs to be an
    /// implementation of `TableProvider` that stitches together the
    /// streams from several different `QueryChunk`s.
    fn read_filter(
        &self,
        mut ctx: IOxSessionContext,
        predicate: &Predicate,
        selection: Selection<'_>,
    ) -> Result<SendableRecordBatchStream, QueryChunkError> {
        ctx.set_metadata("storage", "compactor");
        ctx.set_metadata("projection", format!("{}", selection));
        trace!(?selection, "selection");

        self.data
            .read_filter(predicate, selection)
            .context(ReadParquetSnafu)
            .map_err(|e| Box::new(e) as _)
    }

    /// Returns chunk type
    fn chunk_type(&self) -> &str {
        "QueryableParquetChunk"
    }

    // Order of the chunk so they can be deduplicate correctly
    fn order(&self) -> ChunkOrder {
        let seq_num = self.iox_metadata.min_sequence_number.get();
        let seq_num = u32::try_from(seq_num)
            .expect("Sequence number should have been converted to chunk order successfully");
        ChunkOrder::new(seq_num)
            .expect("Sequence number should have been converted to chunk order successfully")
    }
}
