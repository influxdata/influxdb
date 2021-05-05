//! This module contains code for writing / reading data to parquet.
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self
)]

pub mod error;
pub mod metadata;
pub mod stats;
pub mod writer;
