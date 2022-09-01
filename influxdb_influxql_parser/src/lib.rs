//! # Parse a subset of [InfluxQL]
//!
//! [InfluxQL]: https://docs.influxdata.com/influxdb/v1.8/query_language

#![deny(rustdoc::broken_intra_doc_links, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_docs,
    clippy::explicit_iter_loop,
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]
mod identifier;
mod keywords;
mod literal;
mod string;
