use std::sync::Arc;

use datafusion::execution::session_state::SessionStateBuilder;

use self::{extract_sleep::ExtractSleep, handle_gapfill::HandleGapFill};

mod extract_sleep;
mod handle_gapfill;
pub use handle_gapfill::{default_return_value_for_aggr_fn, range_predicate};

/// Register IOx-specific [`AnalyzerRule`]s with the SessionContext
///
/// [`AnalyzerRule`]: datafusion::optimizer::AnalyzerRule
pub fn register_iox_analyzers(state: SessionStateBuilder) -> SessionStateBuilder {
    state
        .with_analyzer_rule(Arc::new(ExtractSleep::new()))
        .with_analyzer_rule(Arc::new(HandleGapFill))
}
