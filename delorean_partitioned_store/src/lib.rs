//! The delorean_partitioned_store crate contains an early
//! implementation of an in-memory database with WAL. It is deprecated
//! and slated for removal when it is superceded by the implementation
//! in delorean_write_buffer.

pub mod line_parser;
pub mod storage;
