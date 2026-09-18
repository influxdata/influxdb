//! A k-way merge of sorted streams that emits a block of rows at a time.
//!
//! [`RunOptimizedStreamingMergeBuilder`] is an alternative to DataFusion's
//! `StreamingMergeBuilder` for inputs that are mostly disjoint rather than
//! finely interleaved — the shape IOx's scans produce. It carries the full
//! description of the algorithm.

mod builder;
mod cache;
mod compare;
mod input;
mod merge;
mod metrics;
mod sort_key;
mod stream;

#[cfg(test)]
mod tests;

pub use builder::{
    DEFAULT_COMPARATOR_CACHE_ENTRIES, DEFAULT_DEGRADATION_THRESHOLD_ROWS,
    RunOptimizedStreamingMergeBuilder,
};
pub use metrics::{MeanMetricValue, RunOptimizedMergeMetrics};
