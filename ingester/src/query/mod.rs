//! Query execution abstraction & types.

mod r#trait;
pub(crate) use r#trait::*;

// Response types
pub(crate) mod partition_response;
pub(crate) mod response;

// Instrumentation
pub(crate) mod exec_instrumentation;
pub(crate) mod result_instrumentation;
pub(crate) mod tracing;

#[cfg(test)]
pub(crate) mod mock_query_exec;
