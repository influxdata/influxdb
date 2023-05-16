//! Private traits for state machine states.

use std::{fmt::Debug, sync::Arc};

use arrow::record_batch::RecordBatch;
use mutable_batch::MutableBatch;

/// A state that can accept writes.
pub(crate) trait Writeable: Debug {
    fn write(&mut self, batch: MutableBatch) -> Result<(), mutable_batch::Error>;
}

/// A state that can return the contents of the buffer as one or more
/// [`RecordBatch`] instances.
pub(crate) trait Queryable: Debug {
    fn get_query_data(&self) -> Vec<Arc<RecordBatch>>;
}
