//! Optimizer passes concering de-duplication.

pub mod dedup_null_columns;
pub mod dedup_sort_order;
pub mod split;

#[cfg(test)]
mod test_util;
