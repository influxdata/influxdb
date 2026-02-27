//! Rules specific to [`SortExec`].
//!
//! [`SortExec`]: datafusion::physical_plan::sorts::sort::SortExec

pub mod extract_ranges;

pub mod lexical_range;
pub mod merge_partitions;
pub mod order_union_sorted_inputs;
pub mod order_union_sorted_inputs_for_constants;
pub mod parquet_sortness;
pub mod regroup_files;
pub mod util;
