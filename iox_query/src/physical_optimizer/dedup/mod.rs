//! Optimizer passes concering de-duplication.

mod partition_split;
mod remove_dedup;
mod time_split;

#[cfg(test)]
mod test_util;
