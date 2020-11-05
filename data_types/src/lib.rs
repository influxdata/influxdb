//! This crate contains the data types that are shared between delorean servers including
//! replicated data, rules for how data is split up and queried, and what gets stored
//! in the write buffer database.

#![deny(rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self
)]

pub const TIME_COLUMN_NAME: &str = "time";

pub mod data;
pub mod database_rules;
pub mod partition_metadata;
pub mod table_schema;
