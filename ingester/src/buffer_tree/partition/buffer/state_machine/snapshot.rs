//! A writfield1 buffer, with one or more snapshots.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;

use super::BufferState;
use crate::buffer_tree::partition::buffer::{
    state_machine::persisting::Persisting, traits::Queryable,
};

/// An immutable, queryable FSM state containing at least one buffer snapshot.
#[derive(Debug)]
pub(crate) struct Snapshot {
    /// Snapshots generated from previous buffer contents.
    ///
    /// INVARIANT: this array is always non-empty.
    snapshots: Vec<Arc<RecordBatch>>,
}

impl Snapshot {
    pub(super) fn new(snapshots: Vec<Arc<RecordBatch>>) -> Self {
        assert!(!snapshots.is_empty());
        Self { snapshots }
    }
}

impl Queryable for Snapshot {
    fn get_query_data(&self) -> Vec<Arc<RecordBatch>> {
        self.snapshots.clone()
    }
}

impl BufferState<Snapshot> {
    pub(crate) fn into_persisting(self) -> BufferState<Persisting> {
        assert!(!self.state.snapshots.is_empty());
        BufferState {
            state: Persisting::new(self.state.snapshots),
            sequence_range: self.sequence_range,
        }
    }
}
