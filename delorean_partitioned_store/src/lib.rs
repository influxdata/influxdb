//! The delorean_partitioned_store crate contains an early
//! implementation of an in-memory database with WAL. It is deprecated
//! and slated for removal when it is superceded by the implementation
//! in delorean_write_buffer.

#![deny(rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self
)]

pub mod line_parser;
pub mod storage;
