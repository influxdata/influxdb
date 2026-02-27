/// Computes aggregate [`Statistics`](datafusion::common) for a set of [`QueryChunk`](crate)s.
mod aggregate_per_chunk;
pub use aggregate_per_chunk::{NULL_COLUMN_INDICATOR, build_statistics_for_chunks};

/// Computes [`Statistics`](datafusion::common) for a single [`QueryChunk`](crate),
/// providing Absent/default stats for any schema column not found in the chunk.
mod schema_bound;
pub(crate) use schema_bound::SchemaBoundStatistics;

/// Computes [`ColumnStatistics`](datafusion::physical_plan), aggregated across all partitions, for a given [`ExecutionPlan`](datafusion::physical_plan).
mod aggregate_per_plan;
pub use aggregate_per_plan::compute_stats_column_min_max;

/// Computes [`Statistics`](datafusion::physical_plan::Statistics) per partition, in a given [`ExecutionPlan`](datafusion::physical_plan).
pub mod partition_statistics;
pub use partition_statistics::statistics_by_partition;

pub mod stats_utils;
pub use stats_utils::{column_statistics_min_max, overlap};
