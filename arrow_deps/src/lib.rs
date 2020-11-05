//! This crate exists to add a dependency on (likely as yet
//! unpublished) versions of arrow / parquet / datafusion so we can
//! manage the version used by delorean in a single crate.

// export arrow, parquet, and datafusion publically so we can have a single
// reference in cargo
pub use arrow;
pub use datafusion;
pub use parquet;
