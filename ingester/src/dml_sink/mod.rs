//! A composable abstraction for processing write requests.

mod r#trait;
pub use r#trait::*;

pub(crate) mod instrumentation;
pub(crate) mod tracing;

#[cfg(test)]
pub(crate) mod mock_sink;
