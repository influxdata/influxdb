//! Utilities for structured logging

/// Helper to format anyhow error chains on a single line for structured logging.
///
/// This wrapper formats an error and all its causes as a single-line string using
/// anyhow's alternate format (`{:#}`), which is useful for structured logging systems
/// that split on newlines.
///
/// # Example
///
/// ```rust,ignore
/// use influxdb3_types::logging::ErrorOneLine;
/// use tracing::error;
///
/// let error: anyhow::Error = /* ... */;
/// error!(error = %ErrorOneLine(&error), "operation failed");
/// ```
///
/// This produces a single-line log entry like:
/// ```text
/// error="outer error: middle error: root cause"
/// ```
///
/// instead of the default multiline format:
/// ```text
/// error=outer error
///
/// Caused by:
///     middle error
///     root cause
/// ```
#[derive(Debug)]
pub struct ErrorOneLine<'a>(pub &'a anyhow::Error);

impl<'a> std::fmt::Display for ErrorOneLine<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Use anyhow's alternate format which joins the error chain with ": "
        write!(f, "{:#}", self.0)
    }
}
