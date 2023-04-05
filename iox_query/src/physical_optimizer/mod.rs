use std::sync::Arc;

use datafusion::{execution::context::SessionState, physical_optimizer::PhysicalOptimizerRule};

use crate::influxdb_iox_pre_6098_planner;

use self::{
    combine_chunks::CombineChunks,
    dedup::{
        dedup_null_columns::DedupNullColumns, dedup_sort_order::DedupSortOrder,
        partition_split::PartitionSplit, remove_dedup::RemoveDedup, time_split::TimeSplit,
    },
    predicate_pushdown::PredicatePushdown,
    projection_pushdown::ProjectionPushdown,
    sort::{
        parquet_sortness::ParquetSortness, redundant_sort::RedundantSort,
        sort_pushdown::SortPushdown,
    },
    union::{nested_union::NestedUnion, one_union::OneUnion},
};

mod chunk_extraction;
mod combine_chunks;
mod dedup;
mod predicate_pushdown;
mod projection_pushdown;
mod sort;
mod union;

#[cfg(test)]
mod test_util;

/// Register IOx-specific [`PhysicalOptimizerRule`]s with the SessionContext
pub fn register_iox_physical_optimizers(state: SessionState) -> SessionState {
    if influxdb_iox_pre_6098_planner() {
        // keep default
        state
    } else {
        // prepend IOx-specific rules to DataFusion builtins
        let mut optimizers: Vec<Arc<dyn PhysicalOptimizerRule + Sync + Send>> = vec![
            Arc::new(PartitionSplit::default()),
            Arc::new(TimeSplit::default()),
            Arc::new(RemoveDedup::default()),
            Arc::new(CombineChunks::default()),
            Arc::new(DedupNullColumns::default()),
            Arc::new(DedupSortOrder::default()),
            Arc::new(PredicatePushdown::default()),
            Arc::new(ProjectionPushdown::default()),
            Arc::new(ParquetSortness::default()) as _,
            Arc::new(NestedUnion::default()),
            Arc::new(OneUnion::default()),
        ];
        optimizers.append(&mut state.physical_optimizers().to_vec());
        optimizers.extend([
            Arc::new(SortPushdown::default()) as _,
            Arc::new(RedundantSort::default()) as _,
        ]);

        state.with_physical_optimizer_rules(optimizers)
    }
}
