//! A write buffer.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use mutable_batch::MutableBatch;
use schema::Projection;

use super::{snapshot::Snapshot, BufferState, Transition};
use crate::buffer_tree::partition::buffer::{
    mutable_buffer::Buffer,
    traits::{Queryable, Writeable},
};

/// The FSM starting ingest state - a mutable buffer collecting writes.
#[derive(Debug, Default)]
pub(crate) struct Buffering {
    /// The buffer for incoming writes.
    ///
    /// This buffer MAY be empty when no writes have occured since transitioning
    /// to this state.
    buffer: Buffer,
}

/// Implement on-demand querying of the buffered contents without storing the
/// generated snapshot.
///
/// In the future this [`Queryable`] should NOT be implemented for
/// [`Buffering`], and instead snapshots should be incrementally generated and
/// compacted. See <https://github.com/influxdata/influxdb_iox/issues/5805> for
/// context.
///
/// # Panics
///
/// This method panics if converting the buffered data (if any) into an Arrow
/// [`RecordBatch`] fails (a non-transient error).
impl Queryable for Buffering {
    fn get_query_data(&self) -> Vec<Arc<RecordBatch>> {
        let data = self.buffer.buffer().map(|v| {
            Arc::new(
                v.to_arrow(Projection::All)
                    .expect("failed to snapshot buffer data"),
            )
        });

        match data {
            Some(v) => vec![v],
            None => vec![],
        }
    }
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
        Transition::ok(Snapshot::new(vec![snap]))
    }

    pub(crate) fn persist_cost_estimate(&self) -> usize {
        self.state.buffer.persist_cost_estimate()
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
