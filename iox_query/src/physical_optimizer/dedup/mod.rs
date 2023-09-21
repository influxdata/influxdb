//! Optimizer passes concering de-duplication.

pub mod dedup_null_columns;
pub mod dedup_sort_order;
pub mod partition_split;
pub mod remove_dedup;
pub mod time_split;

#[cfg(test)]
mod test_util;
