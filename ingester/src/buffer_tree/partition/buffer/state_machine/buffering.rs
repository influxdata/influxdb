//! A write buffer.

use arrow::record_batch::RecordBatch;
use data_types::{StatValues, TimestampMinMax};
use mutable_batch::{column::ColumnData, MutableBatch};
use schema::{Projection, TIME_COLUMN_NAME};

use super::{snapshot::Snapshot, BufferState, Transition};
use crate::{
    buffer_tree::partition::buffer::{
        mutable_buffer::Buffer,
        traits::{Queryable, Writeable},
    },
    query::projection::OwnedProjection,
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
    fn get_query_data(&self, projection: &OwnedProjection) -> Vec<RecordBatch> {
        self.buffer
            .buffer()
            .map(|v| vec![projection.project_mutable_batches(v)])
            .unwrap_or_default()
    }

    fn rows(&self) -> usize {
        self.buffer.buffer().map(|v| v.rows()).unwrap_or_default()
    }

    fn timestamp_stats(&self) -> Option<TimestampMinMax> {
        self.buffer
            .buffer()
            .map(extract_timestamp_summary)
            // Safety: unwrapping the timestamp bounds is safe, as any non-empty
            // buffer must contain timestamps.
            .map(|v| TimestampMinMax {
                min: v.min.unwrap(),
                max: v.max.unwrap(),
            })
    }

    fn schema(&self) -> Option<schema::Schema> {
        self.buffer.buffer().map(|v| {
            v.schema(Projection::All)
                .expect("failed to construct batch schema")
        })
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
        Transition::ok(Snapshot::new(vec![snap]), self.sequence_numbers)
    }

    pub(crate) fn persist_cost_estimate(&self) -> usize {
        self.state.buffer.persist_cost_estimate()
    }
}

/// Perform an O(1) extraction of the timestamp column statistics.
fn extract_timestamp_summary(batch: &MutableBatch) -> &StatValues<i64> {
    let col = batch
        .column(TIME_COLUMN_NAME)
        .expect("timestamps must exist for non-empty buffer");

    match col.data() {
        ColumnData::I64(_data, stats) => stats,
        _ => unreachable!(),
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
