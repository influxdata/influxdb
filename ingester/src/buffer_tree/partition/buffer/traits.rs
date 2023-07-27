//! Private traits for state machine states.

use std::fmt::Debug;

use arrow::record_batch::RecordBatch;
use data_types::TimestampMinMax;
use mutable_batch::MutableBatch;
use schema::Schema;

use crate::query::projection::OwnedProjection;

/// A state that can accept writes.
pub(crate) trait Writeable: Debug {
    fn write(&mut self, batch: MutableBatch) -> Result<(), mutable_batch::Error>;
}

/// A state that can return the contents of the buffer as one or more
/// [`RecordBatch`] instances.
pub(crate) trait Queryable: Debug {
    fn rows(&self) -> usize;

    fn timestamp_stats(&self) -> Option<TimestampMinMax>;

    fn schema(&self) -> Option<Schema>;

    /// Return the set of [`RecordBatch`] containing ONLY the projected columns.
    fn get_query_data(&self, projection: &OwnedProjection) -> Vec<RecordBatch>;
}
