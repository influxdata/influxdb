use arrow::record_batch::RecordBatch;
use data_types::{SequenceNumber, TimestampMinMax};
use mutable_batch::MutableBatch;

mod always_some;
mod mutable_buffer;
mod state_machine;
pub(crate) mod traits;

use schema::Schema;
pub(crate) use state_machine::*;

use crate::query::projection::OwnedProjection;

use self::{always_some::AlwaysSome, traits::Queryable};

/// The current state of the [`BufferState`] state machine.
///
/// NOTE that this does NOT contain the [`Persisting`] state, as this is a
/// immutable, terminal state that does not accept further writes and is
/// directly queryable.
#[derive(Debug)]
#[must_use = "FSM should not be dropped unused"]
enum FsmState {
    /// The data buffer contains no data snapshots, and is accepting writes.
    Buffering(BufferState<Buffering>),
}

impl Default for FsmState {
    fn default() -> Self {
        Self::Buffering(BufferState::new())
    }
}

/// A helper wrapper over the [`BufferState`] FSM to abstract the caller from
/// state transitions during reads and writes from the underlying buffer.
#[derive(Debug, Default)]
#[must_use = "DataBuffer should not be dropped unused"]
pub(crate) struct DataBuffer(AlwaysSome<FsmState>);

impl DataBuffer {
    /// Buffer the given [`MutableBatch`] in memory, ordered by the specified
    /// [`SequenceNumber`].
    pub(crate) fn buffer_write(
        &mut self,
        mb: MutableBatch,
        sequence_number: SequenceNumber,
    ) -> Result<(), mutable_batch::Error> {
        // Take ownership of the FSM and apply the write.
        self.0.mutate(|fsm| match fsm {
            // Mutable stats simply have the write applied.
            FsmState::Buffering(mut b) => {
                let ret = b.write(mb, sequence_number);
                (FsmState::Buffering(b), ret)
            }
        })
    }

    pub(crate) fn persist_cost_estimate(&self) -> usize {
        match self.0.get() {
            FsmState::Buffering(b) => b.persist_cost_estimate(),
        }
    }

    /// Return all data for this buffer, ordered by the [`SequenceNumber`] from
    /// which it was buffered with.
    pub(crate) fn get_query_data(&mut self, projection: &OwnedProjection) -> Vec<RecordBatch> {
        // Take ownership of the FSM and return the data within it.
        self.0.mutate(|fsm| match fsm {
            // The buffering state can return data.
            FsmState::Buffering(b) => {
                let ret = b.get_query_data(projection);
                (FsmState::Buffering(b), ret)
            }
        })
    }

    /// Return the row count for this buffer.
    pub(crate) fn rows(&self) -> usize {
        match self.0.get() {
            FsmState::Buffering(v) => v.rows(),
        }
    }

    /// Return the timestamp min/max values, if this buffer contains data.
    pub(crate) fn timestamp_stats(&self) -> Option<TimestampMinMax> {
        match self.0.get() {
            FsmState::Buffering(v) => v.timestamp_stats(),
        }
    }

    /// Returns the [`Schema`] for the buffered data.
    pub(crate) fn schema(&self) -> Option<Schema> {
        match self.0.get() {
            FsmState::Buffering(v) => v.schema(),
        }
    }

    // Deconstruct the [`DataBuffer`] into the underlying FSM in a
    // [`Persisting`] state, if the buffer contains any data.
    pub(crate) fn into_persisting(self) -> Option<BufferState<Persisting>> {
        let p = match self.0.into_inner() {
            FsmState::Buffering(b) => {
                // Attempt to snapshot the buffer to an immutable state.
                match b.snapshot() {
                    Transition::Ok(b) => b.into_persisting(),
                    Transition::Unchanged(_) => {
                        // The buffer contains no data.
                        return None;
                    }
                }
            }
        };

        Some(p)
    }
}
