//! Implements a gRPC binary logger. See <https://github.com/grpc/grpc/blob/master/doc/binary-logging.md>
//!
#![deny(rustdoc::broken_intra_doc_links, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    clippy::explicit_iter_loop,
    // See https://github.com/influxdata/influxdb_iox/pull/1671
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr,
    clippy::todo,
    clippy::dbg_macro,
    unused_crate_dependencies
)]

// Workaround for "unused crate" lint false positives.
#[cfg(test)]
use assert_matches as _;
#[cfg(test)]
use grpc_binary_logger_test_proto as _;
#[cfg(test)]
use tokio_stream as _;
use workspace_hack as _;

mod predicate;
pub use self::predicate::{NoReflection, Predicate};
pub mod sink;
pub use self::sink::{DebugSink, FileSink, Sink};

mod middleware;
pub use middleware::BinaryLoggerLayer;

pub use grpc_binary_logger_proto as proto;
