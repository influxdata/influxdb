//! An adaptor over a set of [`RecordBatch`] allowing them to be used as an IOx
//! [`QueryChunk`].

use std::{any::Any, sync::Arc};

use arrow::record_batch::RecordBatch;
use arrow_util::util::ensure_schema;
use data_types::{ChunkId, ChunkOrder, TimestampMinMax, TransitionPartitionId};
use datafusion::physical_plan::Statistics;
use iox_query::{
    util::{compute_timenanosecond_min_max, create_basic_summary},
    QueryChunk, QueryChunkData,
};
use once_cell::sync::OnceCell;
use schema::{merge::merge_record_batch_schemas, sort::SortKey, Schema};

/// A queryable wrapper over a set of ordered [`RecordBatch`] snapshot from a
/// single [`PartitionData`].
///
/// It is an invariant that a [`QueryAdaptor`] MUST always contain at least one
/// row. This frees the caller of having to reason about empty [`QueryAdaptor`]
/// instances yielding empty [`RecordBatch`].
///
/// [`PartitionData`]: crate::buffer_tree::partition::PartitionData
#[derive(Debug, PartialEq, Clone)]
pub struct QueryAdaptor {
    /// The snapshot data from a partition.
    ///
    /// This MUST be non-pub(crate) / closed for modification / immutable to support
    /// interning the merged schema in [`Self::schema()`].
    data: Vec<RecordBatch>,

    /// The identifier of the partition this data is part of.
    partition_id: TransitionPartitionId,

    /// Chunk ID.
    id: ChunkId,

    /// An interned schema for all [`RecordBatch`] in data.
    schema: Schema,

    /// An interned stats.
    stats: OnceCell<Arc<Statistics>>,
}

impl QueryAdaptor {
    /// Construct a [`QueryAdaptor`].
    ///
    /// # Panics
    ///
    /// This constructor panics if `data` contains no [`RecordBatch`], or all
    /// [`RecordBatch`] are empty.
    pub(crate) fn new(partition_id: TransitionPartitionId, data: Vec<RecordBatch>) -> Self {
        // There must always be at least one record batch and one row.
        //
        // This upholds an invariant that simplifies dealing with empty
        // partitions - if there is a QueryAdaptor, it contains data.
        assert!(data.iter().any(|b| b.num_rows() > 0));

        let schema = merge_record_batch_schemas(&data);
        Self {
            data,
            partition_id,
            // To return a value for debugging and make it consistent with ChunkId created in Compactor,
            // use Uuid for this. Draw this UUID during chunk generation so that it is stable during the whole query process.
            id: ChunkId::new(),
            schema,
            stats: OnceCell::default(),
        }
    }

    /// Returns the [`RecordBatch`] instances in this [`QueryAdaptor`].
    pub(crate) fn record_batches(&self) -> &[RecordBatch] {
        self.data.as_ref()
    }

    /// Unwrap this [`QueryAdaptor`], yielding the inner [`RecordBatch`]
    /// instances.
    pub(crate) fn into_record_batches(self) -> Vec<RecordBatch> {
        self.data
    }

    /// Returns the partition identifier from which the data this [`QueryAdaptor`] was
    /// sourced from.
    pub(crate) fn partition_id(&self) -> &TransitionPartitionId {
        &self.partition_id
    }

    /// Number of rows, useful for building stats
    pub(crate) fn num_rows(&self) -> u64 {
        self.data.iter().map(|b| b.num_rows()).sum::<usize>() as u64
    }

    /// The (inclusive) time range covered by all data in this [`QueryAdaptor`],
    /// if this batch contains a `time` column.
    pub(crate) fn ts_min_max(&self) -> Option<TimestampMinMax> {
        // This batch may have been projected to exclude the time column
        compute_timenanosecond_min_max(self.data.iter()).ok()
    }
}

impl QueryChunk for QueryAdaptor {
    fn stats(&self) -> Arc<Statistics> {
        Arc::clone(self.stats.get_or_init(|| {
            let ts_min_max = self.ts_min_max();

            Arc::new(create_basic_summary(
                self.num_rows(),
                self.schema(),
                ts_min_max,
            ))
        }))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn partition_id(&self) -> &TransitionPartitionId {
        &self.partition_id
    }

    fn sort_key(&self) -> Option<&SortKey> {
        None // Ingester data is not sorted
    }

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
        ChunkOrder::MAX
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
