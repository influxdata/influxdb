//! A writfield1 buffer, with one or more snapshots.

use arrow::record_batch::RecordBatch;
use data_types::TimestampMinMax;
use iox_query::util::compute_timenanosecond_min_max;
use schema::{merge::merge_record_batch_schemas, Schema};

use super::BufferState;
use crate::{
    buffer_tree::partition::buffer::{state_machine::persisting::Persisting, traits::Queryable},
    query::projection::OwnedProjection,
};

/// An immutable, queryable FSM state containing at least one buffer snapshot.
#[derive(Debug)]
pub(crate) struct Snapshot {
    /// Snapshots generated from previous buffer contents.
    ///
    /// INVARIANT: this array is always non-empty.
    snapshots: Vec<RecordBatch>,

    /// Statistics describing the data in snapshots.
    row_count: usize,
    timestamp_stats: TimestampMinMax,
    schema: Schema,
}

impl Snapshot {
    pub(super) fn new(snapshots: Vec<RecordBatch>) -> Self {
        assert!(!snapshots.is_empty());

        // Compute some summary statistics for query pruning/reuse later.
        let row_count = snapshots.iter().map(|v| v.num_rows()).sum();
        let timestamp_stats = compute_timenanosecond_min_max(snapshots.iter())
            .expect("non-empty batch must contain timestamps");

        let schema = merge_record_batch_schemas(&snapshots);

        Self {
            snapshots,
            row_count,
            timestamp_stats,
            schema,
        }
    }
}

impl Queryable for Snapshot {
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

impl BufferState<Snapshot> {
    pub(crate) fn into_persisting(self) -> BufferState<Persisting> {
        assert!(!self.state.snapshots.is_empty());
        BufferState {
            state: Persisting::new(
                self.state.snapshots,
                self.state.row_count,
                self.state.timestamp_stats,
                self.state.schema,
            ),
            sequence_numbers: self.sequence_numbers,
        }
    }
}
