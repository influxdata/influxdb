//! An abstraction for a source of [`PartitionData`].
//!
//! This abstraction allows code that uses a set of [`PartitionData`] to be
//! decoupled from the source/provider of that data.

use parking_lot::Mutex;
use std::{fmt::Debug, sync::Arc};

use crate::buffer_tree::partition::PartitionData;

/// An abstraction over any type that can yield an iterator of (potentially
/// empty) [`PartitionData`].
pub trait PartitionIter: Send + Debug {
    /// Return the set of partitions in `self`.
    fn partition_iter(&self) -> Box<dyn Iterator<Item = Arc<Mutex<PartitionData>>> + Send>;
}

impl<T> PartitionIter for Arc<T>
where
    T: PartitionIter + Send + Sync,
{
    fn partition_iter(&self) -> Box<dyn Iterator<Item = Arc<Mutex<PartitionData>>> + Send> {
        (**self).partition_iter()
    }
}

impl PartitionIter for Vec<Arc<Mutex<PartitionData>>> {
    fn partition_iter(&self) -> Box<dyn Iterator<Item = Arc<Mutex<PartitionData>>> + Send> {
        Box::new(self.clone().into_iter())
    }
}
