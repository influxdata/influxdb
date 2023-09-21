//! A [`DmlSink`] decorator to make request [`IngestOp`] durable in a
//! write-ahead log.
//!
//! [`DmlSink`]: crate::dml_sink::DmlSink
//! [`IngestOp`]: crate::dml_payload::IngestOp

pub(crate) mod disk_full_protection;
pub(crate) mod reference_tracker;
pub(crate) mod rotate_task;
mod traits;
pub(crate) mod wal_sink;
