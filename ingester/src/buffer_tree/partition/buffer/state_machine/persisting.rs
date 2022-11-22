//! A writfield1 buffer, with one or more snapshots.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;

use super::BufferState;
use crate::buffer_tree::partition::buffer::traits::Queryable;

/// An immutable set of [`RecordBatch`] in the process of being persisted.
#[derive(Debug)]
pub(crate) struct Persisting {
    /// Snapshots generated from previous buffer contents to be persisted.
    ///
    /// INVARIANT: this array is always non-empty.
    snapshots: Vec<Arc<RecordBatch>>,
}

impl Persisting {
    pub(super) fn new(snapshots: Vec<Arc<RecordBatch>>) -> Self {
        Self { snapshots }
    }
}

impl Queryable for Persisting {
    fn get_query_data(&self) -> Vec<Arc<RecordBatch>> {
        self.snapshots.clone()
    }
}

impl BufferState<Persisting> {
    /// Consume `self`, returning the data it holds as a set of [`RecordBatch`].
    pub(super) fn into_data(self) -> Vec<Arc<RecordBatch>> {
        self.state.snapshots
    }
}
