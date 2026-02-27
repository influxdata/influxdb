use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_planner::ExtensionPlanner;
use std::sync::Arc;

/// Trait implemented by extension that add functionality
/// to the IOx querier.
pub trait Extension {
    /// Optional physical planner to configure for the extension.
    ///
    /// The default implementation returns `None`.
    fn planner(&self) -> Option<Arc<dyn ExtensionPlanner + Send + Sync>> {
        None
    }

    /// Add any additional features to the session state that
    /// are provided by this extension.
    ///
    /// The default implementation returns the provided `state` unchanged.
    fn extend_session_state(&self, state: SessionStateBuilder) -> SessionStateBuilder {
        state
    }
}
