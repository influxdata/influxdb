//! Used in the creation of a [`PartitionsSource`](crate::PartitionsSource) of PartitionIds
//! for the [`LocalScheduler`](crate::LocalScheduler).
//!
//! The filtering and fetching operations in these [`PartitionsSource`](crate::PartitionsSource) implementations
//! are limited to the PartitionId and metadata only, and not dependent upon any file IO.
pub(crate) mod catalog_all;
pub(crate) mod catalog_to_compact;
pub(crate) mod filter;
pub(crate) mod never_skipped;
