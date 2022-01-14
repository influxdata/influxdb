//! DML operation handling.
//!
//! An IOx node operating as a router exposes multiple API interfaces (HTTP,
//! gRPC) which funnel requests into a common [`DmlHandler`] implementation,
//! responsible for processing the request before pushing the results into the
//! appropriate write buffer sink.

mod r#trait;
pub use r#trait::*;

pub mod nop;

#[cfg(test)]
pub mod mock;
