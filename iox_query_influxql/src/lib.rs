//! Contains the IOx InfluxQL query planner
#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self,
    clippy::clone_on_ref_ptr,
    // See https://github.com/influxdata/influxdb_iox/pull/1671
    clippy::future_not_send,
    clippy::todo,
    clippy::dbg_macro,
    unused_crate_dependencies
)]

use arrow::datatypes::DataType;

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

mod aggregate;
mod error;
pub mod frontend;
pub mod plan;
mod window;

/// A list of the numeric types supported by InfluxQL that can be be used
/// as input to user-defined functions.
static NUMERICS: &[DataType] = &[DataType::Int64, DataType::UInt64, DataType::Float64];
