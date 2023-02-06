use std::fmt::{Debug, Display};

use data_types::PartitionId;
use futures::stream::BoxStream;

pub mod endless;
pub mod once;

/// Source for partitions.
pub trait PartitionStream: Debug + Display + Send + Sync {
    /// Create new source stream of partitions.
    ///
    /// This stream may be endless.
    fn stream(&self) -> BoxStream<'_, PartitionId>;
}
