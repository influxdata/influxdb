//! An abstract resolver of [`PartitionData`] for a given shard & table.
//!
//! [`PartitionData`]: crate::buffer_tree::partition::PartitionData

mod cache;
pub(crate) use cache::*;

mod r#trait;
pub(crate) use r#trait::*;

mod catalog;
pub(crate) use catalog::*;

mod sort_key;
pub(crate) use sort_key::*;

#[cfg(test)]
mod mock;
#[cfg(test)]
pub(crate) use mock::*;
