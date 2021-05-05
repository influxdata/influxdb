#![deny(broken_intra_doc_links, rust_2018_idioms)]
#![allow(clippy::clone_on_ref_ptr)]

//! This crate exists to add a dependency on (likely as yet
//! unpublished) versions of arrow / parquet / datafusion so we can
//! manage the version used by InfluxDB IOx in a single crate.

// export arrow, parquet, and datafusion publically so we can have a single
// reference in cargo
pub use arrow;
pub use arrow_flight;
pub use datafusion;
pub use parquet;
pub use parquet_format;

pub mod util;

/// This has a collection of testing helper functions
pub mod test_util;
