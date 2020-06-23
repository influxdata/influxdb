//! This module contains code for writing / reading delorean data to parquet.
#![deny(rust_2018_idioms)]
#![warn(missing_debug_implementations, clippy::explicit_iter_loop)]

// Export the parts of
pub use parquet::{
    errors::ParquetError,
    file::reader::{Length, TryClone},
};

pub mod error;
pub mod metadata;
pub mod stats;
pub mod writer;
