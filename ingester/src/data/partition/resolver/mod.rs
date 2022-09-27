//! An abstract resolver of [`PartitionData`] for a given shard & table.
//!
//! [`PartitionData`]: crate::data::partition::PartitionData

mod r#trait;
pub(crate) use r#trait::*;

mod catalog;
pub(crate) use catalog::*;

#[cfg(test)]
mod mock;
#[cfg(test)]
pub(crate) use mock::*;
