use std::sync::Arc;

use datafusion::{execution::context::SessionState, physical_optimizer::PhysicalOptimizerRule};

use self::union::one_union::OneUnion;

mod chunk_extraction;
mod combine_chunks;
mod dedup;
mod predicate_pushdown;
mod union;

#[cfg(test)]
mod test_util;

/// Register IOx-specific [`PhysicalOptimizerRule`]s with the SessionContext
pub fn register_iox_physical_optimizers(state: SessionState) -> SessionState {
    // prepend IOx-specific rules to DataFusion builtins
    let mut optimizers: Vec<Arc<dyn PhysicalOptimizerRule + Sync + Send>> =
        vec![Arc::new(OneUnion::default())];
    optimizers.append(&mut state.physical_optimizers().to_vec());

    state.with_physical_optimizer_rules(optimizers)
}
