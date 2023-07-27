use std::fmt::{Debug, Display};

use compactor_scheduler::CompactionJob;
use futures::stream::BoxStream;

pub mod endless;
pub mod once;

/// Source for partitions.
pub trait PartitionStream: Debug + Display + Send + Sync {
    /// Create new source stream of compaction job.
    ///
    /// This stream may be endless.
    fn stream(&self) -> BoxStream<'_, CompactionJob>;
}
