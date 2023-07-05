//! A writfield1 buffer, with one or more snapshots.

use arrow::record_batch::RecordBatch;

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
}

impl Snapshot {
    pub(super) fn new(snapshots: Vec<RecordBatch>) -> Self {
        assert!(!snapshots.is_empty());
        Self { snapshots }
    }
}

impl Queryable for Snapshot {
    fn get_query_data(&self, projection: &OwnedProjection) -> Vec<RecordBatch> {
        projection.project_record_batch(&self.snapshots)
    }
}

impl BufferState<Snapshot> {
    pub(crate) fn into_persisting(self) -> BufferState<Persisting> {
        assert!(!self.state.snapshots.is_empty());
        BufferState {
            state: Persisting::new(self.state.snapshots),
            sequence_numbers: self.sequence_numbers,
        }
    }
}
