//! A module containing crate-local data types that are shared across the
//! ingester's internals for processing DML payloads

mod ingest_op;
pub use ingest_op::*;

pub mod encode;
pub mod write;
