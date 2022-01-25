//! A representation of a single operation sequencer.

use std::{hash::Hash, sync::Arc};

use dml::{DmlMeta, DmlOperation};
use write_buffer::core::{WriteBufferError, WriteBufferWriting};

/// A sequencer tags an write buffer with a sequencer ID.
#[derive(Debug)]
pub struct Sequencer {
    id: usize,
    inner: Arc<dyn WriteBufferWriting>,
}

impl Eq for Sequencer {}

impl PartialEq for Sequencer {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Hash for Sequencer {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl Sequencer {
    /// Tag `inner` with the specified `id`.
    pub fn new(id: usize, inner: Arc<dyn WriteBufferWriting>) -> Self {
        Self { id, inner }
    }

    /// Return the ID of this sequencer.
    pub fn id(&self) -> usize {
        self.id
    }

    /// Enqueue `op` into this sequencer.
    ///
    /// The buffering / async return behaviour of this method is defined by the
    /// behaviour of the [`WriteBufferWriting::store_operation()`]
    /// implementation this [`Sequencer`] wraps.
    pub async fn enqueue<'a>(&self, op: DmlOperation) -> Result<DmlMeta, WriteBufferError> {
        self.inner.store_operation(self.id as u32, &op).await
    }
}
