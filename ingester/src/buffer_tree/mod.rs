//! A mutable, hierarchical buffer structure of namespace -> table ->
//! partition -> write payloads.

pub(crate) mod namespace;
pub(crate) mod partition;
pub(crate) mod table;

/// The root node of a [`BufferTree`].
mod root;
#[allow(unused_imports)]
pub(crate) use root::*;

pub(crate) mod post_write;

/// This needs to be pub for the benchmarks but should not be used outside the crate.
#[cfg(feature = "benches")]
pub use partition::PartitionData;
