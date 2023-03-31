//! Rules specific to [`SortExec`].
//!
//! [`SortExec`]: datafusion::physical_plan::sorts::sort::SortExec

pub mod parquet_sortness;
pub mod redundant_sort;
pub mod sort_pushdown;
