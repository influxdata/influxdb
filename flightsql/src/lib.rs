//! InfluxDB IOx implementation of FlightSQL

#![deny(rustdoc::broken_intra_doc_links, rust_2018_idioms)]
#![warn(
    clippy::clone_on_ref_ptr,
    clippy::dbg_macro,
    clippy::explicit_iter_loop,
    // See https://github.com/influxdata/influxdb_iox/pull/1671
    clippy::future_not_send,
    clippy::todo,
    clippy::use_self,
    missing_debug_implementations,
    // Allow missing docs - there's lots missing!
    unused_crate_dependencies
)]

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

mod cmd;
mod error;
mod planner;
mod sql_info;
mod xdbc_type_info;

pub use cmd::{FlightSQLCommand, PreparedStatementHandle};
pub use error::{Error, Result};
pub use planner::FlightSQLPlanner;
