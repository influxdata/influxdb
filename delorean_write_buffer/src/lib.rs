//! Process incoming data, back up data in a WAL, handle WAL recovery, and return data for queries.

#![deny(rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self
)]

/// Contains the implementation of the in memory write buffer that stores incoming data.
pub mod database;
