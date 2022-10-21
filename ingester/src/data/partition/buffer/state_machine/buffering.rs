//! A write buffer.

use mutable_batch::MutableBatch;

use crate::data::partition::buffer::{mutable_buffer::Buffer, traits::Writeable};

use super::{snapshot::Snapshot, BufferState, Transition};

/// The FSM starting ingest state - a mutable buffer collecting writes.
#[derive(Debug, Default)]
pub(crate) struct Buffering {
    /// The buffer for incoming writes.
    ///
    /// This buffer MAY be empty when no writes have occured since transitioning
    /// to this state.
    buffer: Buffer,
}

impl Writeable for Buffering {
    fn write(&mut self, batch: MutableBatch) -> Result<(), mutable_batch::Error> {
        self.buffer.buffer_write(batch)
    }
}

impl BufferState<Buffering> {
    /// Attempt to generate a snapshot from the data in this buffer.
    ///
    /// This returns [`Transition::Unchanged`] if this buffer contains no data.
    pub(crate) fn snapshot(self) -> Transition<Snapshot, Buffering> {
        if self.state.buffer.is_empty() {
            // It is a logical error to snapshot an empty buffer.
            return Transition::unchanged(self);
        }

        // Generate a snapshot from the buffer.
        let snap = self
            .state
            .buffer
            .snapshot()
            .expect("snapshot of non-empty buffer should succeed");

        // And transition to the WithSnapshot state.
        Transition::ok(Snapshot::new(vec![snap]), self.sequence_range)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_buffer_does_not_snapshot() {
        let b = BufferState::new();
        match b.snapshot() {
            Transition::Ok(_) => panic!("empty buffer should not transition to snapshot state"),
            Transition::Unchanged(_) => {
                // OK!
            }
        }
    }
}
