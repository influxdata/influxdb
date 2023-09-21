use arrow::record_batch::RecordBatch;
use mutable_batch::MutableBatch;
use schema::Projection;

/// A [`Buffer`] is an internal mutable buffer wrapper over a [`MutableBatch`]
/// for the [`BufferState`] FSM.
///
/// A [`Buffer`] can contain no writes.
///
/// [`BufferState`]: super::BufferState
#[derive(Debug, Default)]
pub(super) struct Buffer {
    buffer: Option<MutableBatch>,
}

impl Buffer {
    /// Apply `batch` to the in-memory buffer.
    ///
    /// # Data Loss
    ///
    /// If this method returns an error, the data in `batch` is problematic and
    /// has been discarded.
    pub(super) fn buffer_write(&mut self, batch: MutableBatch) -> Result<(), mutable_batch::Error> {
        match self.buffer {
            Some(ref mut b) => b.extend_from(&batch)?,
            None => self.buffer = Some(batch),
        };

        Ok(())
    }

    /// Generates a [`RecordBatch`] from the data in this [`Buffer`].
    ///
    /// If this [`Buffer`] is empty when this method is called, the call is a
    /// NOP and [`None`] is returned.
    ///
    /// # Panics
    ///
    /// If generating the snapshot fails, this method panics.
    pub(super) fn snapshot(self) -> Option<RecordBatch> {
        Some(
            self.buffer?
                .to_arrow(Projection::All)
                .expect("failed to snapshot buffer data"),
        )
    }

    pub(super) fn is_empty(&self) -> bool {
        self.buffer.is_none()
    }

    /// Returns the underlying buffer if this [`Buffer`] contains data,
    /// otherwise returns [`None`].
    pub(super) fn buffer(&self) -> Option<&MutableBatch> {
        self.buffer.as_ref()
    }

    pub(crate) fn persist_cost_estimate(&self) -> usize {
        self.buffer().map(|v| v.size_data()).unwrap_or_default()
    }
}
