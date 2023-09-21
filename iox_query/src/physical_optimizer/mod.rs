use std::sync::Arc;

use datafusion::{execution::context::SessionState, physical_optimizer::PhysicalOptimizerRule};

use self::{
    combine_chunks::CombineChunks,
    dedup::{
        dedup_null_columns::DedupNullColumns, dedup_sort_order::DedupSortOrder,
        partition_split::PartitionSplit, remove_dedup::RemoveDedup, time_split::TimeSplit,
    },
    predicate_pushdown::PredicatePushdown,
    projection_pushdown::ProjectionPushdown,
    sort::parquet_sortness::ParquetSortness,
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
    // prepend IOx-specific rules to DataFusion builtins
    // The optimizer rules have to be done in this order
    let mut optimizers: Vec<Arc<dyn PhysicalOptimizerRule + Sync + Send>> = vec![
        Arc::new(PartitionSplit),
        Arc::new(TimeSplit),
        Arc::new(RemoveDedup),
        Arc::new(CombineChunks),
        Arc::new(DedupNullColumns),
        Arc::new(DedupSortOrder),
        Arc::new(PredicatePushdown),
        Arc::new(ProjectionPushdown),
        Arc::new(ParquetSortness) as _,
        Arc::new(NestedUnion),
        Arc::new(OneUnion),
    ];
    optimizers.append(&mut state.physical_optimizers().to_vec());

    state.with_physical_optimizer_rules(optimizers)
}
