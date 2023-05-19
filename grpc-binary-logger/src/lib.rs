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
    clippy::dbg_macro
)]

mod predicate;
pub use self::predicate::{NoReflection, Predicate};
pub mod sink;
pub use self::sink::{DebugSink, FileSink, Sink};

mod middleware;
pub use middleware::BinaryLoggerLayer;

pub use grpc_binary_logger_proto as proto;
