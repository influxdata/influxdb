//! A [`DmlSink`] decorator to make request [`DmlOperation`] durable in a
//! write-ahead log.
//!
//! [`DmlSink`]: crate::dml_sink::DmlSink
//! [`DmlOperation`]: dml::DmlOperation

mod traits;
pub(crate) mod wal_sink;
