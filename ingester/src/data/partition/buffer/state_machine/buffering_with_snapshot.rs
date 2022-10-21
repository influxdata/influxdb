//! A mutable buffer, containing one or more snapshots.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use mutable_batch::MutableBatch;

use crate::data::partition::buffer::{mutable_buffer::Buffer, traits::Writeable};

use super::{snapshot::Snapshot, BufferState};

/// A mutable state that buffers incoming writes while holding at least one
/// previously generated buffer snapshot.
#[derive(Debug)]
pub(crate) struct BufferingWithSnapshot {
    /// The buffer for incoming writes.
    ///
    /// NOTE: this buffer MAY be empty.
    buffer: Buffer,

    /// Snapshots generated from previous buffer contents.
    ///
    /// INVARIANT: this array is always non-empty.
    snapshots: Vec<Arc<RecordBatch>>,
}

impl BufferingWithSnapshot {
    pub(super) fn new(snapshots: Vec<Arc<RecordBatch>>) -> Self {
        Self {
            buffer: Buffer::default(),
            snapshots,
        }
    }
}

impl Writeable for BufferingWithSnapshot {
    fn write(&mut self, batch: MutableBatch) -> Result<(), mutable_batch::Error> {
        // TODO(5806): assert schema compatibility with existing snapshots
        self.buffer.buffer_write(batch)
    }
}

impl BufferState<BufferingWithSnapshot> {
    /// Snapshot the current buffer contents and transition to an immutable,
    /// queryable state containing only snapshots.
    ///
    /// This call MAY be a NOP if the buffer has accrued no writes.
    pub(crate) fn snapshot(self) -> BufferState<Snapshot> {
        assert!(!self.state.snapshots.is_empty());

        BufferState {
            state: Snapshot::new(
                self.state
                    .snapshots
                    .into_iter()
                    .chain(self.state.buffer.snapshot())
                    .collect(),
            ),
            sequence_range: self.sequence_range,
        }
    }
}
