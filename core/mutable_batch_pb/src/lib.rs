//! Code to convert between binary write format and [`mutable_batch::MutableBatch`]

#![warn(missing_docs)]

#[cfg(test)]
use criterion as _;
#[cfg(test)]
use data_types as _;
#[cfg(test)]
use mutable_batch_lp as _;
#[cfg(test)]
use partition as _;

pub mod decode;
pub mod encode;
