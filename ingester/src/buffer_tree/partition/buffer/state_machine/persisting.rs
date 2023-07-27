//! A writfield1 buffer, with one or more snapshots.

use arrow::record_batch::RecordBatch;
use data_types::{sequence_number_set::SequenceNumberSet, TimestampMinMax};
use iox_query::util::compute_timenanosecond_min_max;
use schema::{merge::merge_record_batch_schemas, Schema};

use super::BufferState;
use crate::{
    buffer_tree::partition::buffer::traits::Queryable, query::projection::OwnedProjection,
};

/// An immutable set of [`RecordBatch`] in the process of being persisted.
#[derive(Debug)]
pub(crate) struct Persisting {
    /// Snapshots generated from previous buffer contents to be persisted.
    ///
    /// INVARIANT: this array is always non-empty.
    snapshots: Vec<RecordBatch>,

    /// Statistics describing the data in snapshots.
    row_count: usize,
    timestamp_stats: TimestampMinMax,
    schema: Schema,
}

impl Persisting {
    pub(super) fn new(
        snapshots: Vec<RecordBatch>,
        row_count: usize,
        timestamp_stats: TimestampMinMax,
        schema: Schema,
    ) -> Self {
        // Invariant: the summary statistics provided must match the actual
        // data.
        debug_assert_eq!(
            row_count,
            snapshots.iter().map(|v| v.num_rows()).sum::<usize>()
        );
        debug_assert_eq!(
            timestamp_stats,
            compute_timenanosecond_min_max(snapshots.iter()).unwrap()
        );
        debug_assert_eq!(schema, merge_record_batch_schemas(&snapshots));

        Self {
            snapshots,
            row_count,
            timestamp_stats,
            schema,
        }
    }
}

impl Queryable for Persisting {
    fn get_query_data(&self, projection: &OwnedProjection) -> Vec<RecordBatch> {
        projection.project_record_batch(&self.snapshots)
    }

    fn rows(&self) -> usize {
        self.row_count
    }

    fn timestamp_stats(&self) -> Option<TimestampMinMax> {
        Some(self.timestamp_stats)
    }

    fn schema(&self) -> Option<schema::Schema> {
        Some(self.schema.clone()) // Ref clone
    }
}

impl BufferState<Persisting> {
    /// Consume `self` and all references to the buffered data, returning the owned
    /// [`SequenceNumberSet`] within it.
    pub(crate) fn into_sequence_number_set(self) -> SequenceNumberSet {
        self.sequence_numbers
    }
}
