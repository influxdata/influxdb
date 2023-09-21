use std::fmt::{Debug, Display};

use compactor_scheduler::CompactionJob;
use futures::stream::BoxStream;

pub mod endless;
pub mod once;

/// Source for compaction jobs.
pub trait CompactionJobStream: Debug + Display + Send + Sync {
    /// Create new source stream of compaction jobs.
    ///
    /// This stream may be endless.
    fn stream(&self) -> BoxStream<'_, CompactionJob>;
}
