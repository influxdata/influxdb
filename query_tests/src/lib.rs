//! This module contains "end to end" tests for the query layer.
//!
//! These tests consist of loading the same data in several "scenarios", running queries against it
//! and verifying the same answer is produced in all scenarios.

// Actual tests

#[cfg(test)]
#[rustfmt::skip]
mod cases;
#[cfg(test)]
pub mod influxrpc;
#[cfg(test)]
mod runner;
#[cfg(test)]
pub mod sql;
#[cfg(test)]
pub mod table_schema;

pub mod db;
pub mod scenarios;
