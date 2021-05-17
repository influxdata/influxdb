//! Common error utilities
use std::fmt::Debug;

use observability_deps::tracing::error;

/// Add ability for Results to log error messages via `error!` logs.
/// This is useful when using async tasks that may not have any code
/// checking their return values.
pub trait ErrorLogger {
    /// Log the contents of self with a string of context. The context
    /// should appear in a message such as
    ///
    /// "Error <context>: <formatted error message>
    fn log_if_error(self, context: &str) -> Self;

    /// Provided method to log an error via the `error!` macro
    fn log_error<E: Debug>(context: &str, e: E) {
        error!("Error {}: {:?}", context, e);
    }
}

/// Implement logging for all results
impl<T, E: Debug> ErrorLogger for Result<T, E> {
    fn log_if_error(self, context: &str) -> Self {
        if let Err(e) = &self {
            Self::log_error(context, e);
        }
        self
    }
}
