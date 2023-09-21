//! Rules specific to [`SortExec`].
//!
//! [`SortExec`]: datafusion::physical_plan::sorts::sort::SortExec

pub mod parquet_sortness;
pub mod push_sort_through_union;
