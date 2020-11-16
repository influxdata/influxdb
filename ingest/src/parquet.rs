//! This module contains code for writing / reading data to parquet.
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self
)]

// Export the parts of the parquet crate that are needed to interact with code in this crate
pub use arrow_deps::parquet::{
    errors::ParquetError,
    file::reader::{ChunkReader, Length},
    file::writer::TryClone,
};

pub mod error;
pub mod metadata;
pub mod stats;
pub mod writer;
