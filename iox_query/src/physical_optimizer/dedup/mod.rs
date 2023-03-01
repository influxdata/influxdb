//! Optimizer passes concering de-duplication.

pub mod partition_split;
pub mod remove_dedup;
pub mod time_split;

#[cfg(test)]
mod test_util;
