// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

#[cfg(test)]
use criterion as _;

pub mod bitset;
pub mod dictionary;
pub mod display;
pub mod flight;
pub mod optimize;
pub mod parquet_meta;
pub mod string;
pub mod util;

/// This has a collection of testing helper functions
pub mod test_util;
