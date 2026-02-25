//! IOx sharder implementation.
//!
//! Given a table and a namespace, assign a consistent shard from the set of shards.

#![warn(missing_docs)]

// Workaround for "unused crate" lint false positives.
#[cfg(test)]
use criterion as _;
#[cfg(test)]
use rand as _;
use workspace_hack as _;

mod r#trait;
pub use r#trait::*;

mod round_robin;
pub use round_robin::*;

mod jumphash;
pub use jumphash::*;

#[expect(missing_docs)]
pub mod mock;
