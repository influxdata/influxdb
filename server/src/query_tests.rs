//! This module contains "end-to-end" tests for the query layer.
//!
//! These tests consist of loading the same data in several
//! "scenarios" (different distributions across the Mutable Buffer,
//! Immutable Buffer, and (eventually) Parquet files, running queries
//! against it and verifying the same answer is produced in all scenarios

pub mod influxrpc;
pub mod pruning;
pub mod scenarios;
pub mod sql;
pub mod table_schema;
pub mod utils;
