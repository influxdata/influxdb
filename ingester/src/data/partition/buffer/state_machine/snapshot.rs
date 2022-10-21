//! An immutable buffer, containing one or more snapshots in an efficient query
//! format.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;

use crate::data::partition::buffer::{state_machine::persisting::Persisting, traits::Queryable};

use super::{buffering_with_snapshot::BufferingWithSnapshot, BufferState};

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
    fn get_query_data(&self) -> &[Arc<RecordBatch>] {
        &self.snapshots
    }
}

impl BufferState<Snapshot> {
    pub(crate) fn into_buffering(self) -> BufferState<BufferingWithSnapshot> {
        assert!(!self.state.snapshots.is_empty());
        BufferState {
            state: BufferingWithSnapshot::new(self.state.snapshots),
            sequence_range: self.sequence_range,
        }
    }

    pub(crate) fn into_persisting(self) -> BufferState<Persisting> {
        assert!(!self.state.snapshots.is_empty());
        BufferState {
            state: Persisting::new(self.state.snapshots),
            sequence_range: self.sequence_range,
        }
    }
}
