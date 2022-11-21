//! An adaptor over a set of [`RecordBatch`] allowing them to be used as an IOx
//! [`QueryChunk`].

use std::{any::Any, sync::Arc};

use arrow::record_batch::RecordBatch;
use arrow_util::util::ensure_schema;
use data_types::{ChunkId, ChunkOrder, DeletePredicate, PartitionId, TableSummary};
use datafusion::error::DataFusionError;
use iox_query::{
    exec::{stringset::StringSet, IOxSessionContext},
    util::{compute_timenanosecond_min_max, create_basic_summary},
    QueryChunk, QueryChunkData, QueryChunkMeta,
};
use once_cell::sync::OnceCell;
use predicate::Predicate;
use schema::{merge::merge_record_batch_schemas, sort::SortKey, Projection, Schema};
use snafu::Snafu;

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub enum Error {
    #[snafu(display("Internal error concatenating record batches {}", source))]
    Schema { source: schema::Error },

    #[snafu(display("Internal error concatenating record batches {}", source))]
    ConcatBatches { source: arrow::error::ArrowError },

    #[snafu(display("Internal error filtering columns from a record batch {}", source))]
    FilterColumns { source: crate::data::Error },

    #[snafu(display("Internal error filtering record batch: {}", source))]
    FilterBatch { source: arrow::error::ArrowError },
}

/// A specialized `Error` for Ingester's Query errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A queryable wrapper over a set of ordered [`RecordBatch`] snapshot from a
/// single [`PartitionData`].
///
/// It is an invariant that a [`QueryAdaptor`] MUST always contain at least one
/// row. This frees the caller of having to reason about empty [`QueryAdaptor`]
/// instances yielding empty [`RecordBatch`].
///
/// [`PartitionData`]: crate::buffer_tree::partition::PartitionData
#[derive(Debug, PartialEq, Clone)]
pub(crate) struct QueryAdaptor {
    /// The snapshot data from a partition.
    ///
    /// This MUST be non-pub / closed for modification / immutable to support
    /// interning the merged schema in [`Self::schema()`].
    data: Vec<Arc<RecordBatch>>,

    /// The catalog ID of the partition the this data is part of.
    partition_id: PartitionId,

    /// Chunk ID.
    id: ChunkId,

    /// An interned schema for all [`RecordBatch`] in data.
    schema: OnceCell<Arc<Schema>>,

    /// An interned table summary.
    summary: OnceCell<Arc<TableSummary>>,
}

impl QueryAdaptor {
    /// Construct a [`QueryAdaptor`].
    ///
    /// # Panics
    ///
    /// This constructor panics if `data` contains no [`RecordBatch`], or all
    /// [`RecordBatch`] are empty.
    pub(crate) fn new(partition_id: PartitionId, data: Vec<Arc<RecordBatch>>) -> Self {
        // There must always be at least one record batch and one row.
        //
        // This upholds an invariant that simplifies dealing with empty
        // partitions - if there is a QueryAdaptor, it contains data.
        assert!(data.iter().map(|b| b.num_rows()).sum::<usize>() > 0);

        Self {
            data,
            partition_id,
            // To return a value for debugging and make it consistent with ChunkId created in Compactor,
            // use Uuid for this. Draw this UUID during chunk generation so that it is stable during the whole query process.
            id: ChunkId::new(),
            schema: OnceCell::default(),
            summary: OnceCell::default(),
        }
    }

    pub(crate) fn project_selection(&self, selection: Projection<'_>) -> Vec<RecordBatch> {
        // Project the column selection across all RecordBatch
        self.data
            .iter()
            .map(|data| {
                let batch = data.as_ref();
                let schema = batch.schema();

                // Apply selection to in-memory batch
                match selection {
                    Projection::All => batch.clone(),
                    Projection::Some(columns) => {
                        let projection = columns
                            .iter()
                            .flat_map(|&column_name| {
                                // ignore non-existing columns
                                schema.index_of(column_name).ok()
                            })
                            .collect::<Vec<_>>();
                        batch.project(&projection).expect("bug in projection")
                    }
                }
            })
            .collect()
    }

    /// Returns the [`RecordBatch`] instances in this [`QueryAdaptor`].
    pub(crate) fn record_batches(&self) -> &[Arc<RecordBatch>] {
        self.data.as_ref()
    }

    /// Returns the partition ID from which the data this [`QueryAdaptor`] was
    /// sourced from.
    pub(crate) fn partition_id(&self) -> PartitionId {
        self.partition_id
    }
}

impl QueryChunkMeta for QueryAdaptor {
    fn summary(&self) -> Arc<TableSummary> {
        Arc::clone(self.summary.get_or_init(|| {
            let ts_min_max = compute_timenanosecond_min_max(self.data.iter().map(|b| b.as_ref()))
                .expect("Should have time range");

            Arc::new(create_basic_summary(
                self.data.iter().map(|b| b.num_rows()).sum::<usize>() as u64,
                &self.schema(),
                ts_min_max,
            ))
        }))
    }

    fn schema(&self) -> Arc<Schema> {
        Arc::clone(
            self.schema
                .get_or_init(|| merge_record_batch_schemas(&self.data)),
        )
    }

    fn partition_sort_key(&self) -> Option<&SortKey> {
        None // Ingester data has not persisted yet and should not be attached to any partition
    }

    fn partition_id(&self) -> PartitionId {
        self.partition_id
    }

    fn sort_key(&self) -> Option<&SortKey> {
        None // Ingester data is not sorted
    }

    fn delete_predicates(&self) -> &[Arc<DeletePredicate>] {
        &[]
    }
}

impl QueryChunk for QueryAdaptor {
    fn id(&self) -> ChunkId {
        self.id
    }

    /// Returns true if the chunk may contain a duplicate "primary key" within
    /// itself
    fn may_contain_pk_duplicates(&self) -> bool {
        // always true because the rows across record batches have not been
        // de-duplicated.
        true
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
        let schema = self.schema().as_arrow();

        QueryChunkData::RecordBatches(
            self.data
                .iter()
                .map(|b| ensure_schema(&schema, b).expect("schema handling broken"))
                .collect(),
        )
    }

    /// Returns chunk type
    fn chunk_type(&self) -> &str {
        "QueryAdaptor"
    }

    fn order(&self) -> ChunkOrder {
        unimplemented!()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
