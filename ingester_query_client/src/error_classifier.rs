//! Classifies the kind of the error.

use crate::error::{DynError, ErrorChainExt};
use std::{fmt::Debug, sync::Arc};

/// Dynamic classifier.
#[derive(Clone)]
pub struct ErrorClassifier {
    inner: Arc<dyn Fn(&DynError) -> bool + Send + Sync>,
}

impl ErrorClassifier {
    /// Create dyn-typed error classifier.
    pub fn new<F>(f: F) -> Self
    where
        F: Fn(&DynError) -> bool + Send + Sync + 'static,
    {
        Self { inner: Arc::new(f) }
    }

    /// Checks if given error matches this classifier.
    pub fn matches(&self, e: &DynError) -> bool {
        (self.inner)(e)
    }
}

impl Debug for ErrorClassifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ErrorClassifier").finish_non_exhaustive()
    }
}

/// Checks if this is a connection / ingester-state error.
///
/// This can lead to cutting the connection or breaking/opening the circuit.
pub fn is_upstream_error(e: &DynError) -> bool {
    e.error_chain().any(|e| {
        if let Some(e) = e.downcast_ref::<tonic::Status>() {
            return !matches!(
                e.code(),
                tonic::Code::NotFound | tonic::Code::ResourceExhausted
            );
        }

        false
    })
}

/// Simple error for testing purposes that controles [`test_error_classifier`].
#[derive(Debug)]
#[allow(missing_copy_implementations)]
pub struct TestError {
    retry: bool,
}

impl TestError {
    /// Retry.
    pub const RETRY: Self = Self { retry: true };

    /// Do NOT retry.
    pub const NO_RETRY: Self = Self { retry: false };
}

impl std::fmt::Display for TestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "test error, retry={}", self.retry)
    }
}

impl std::error::Error for TestError {}

/// Classifier that checks for [`TestError`].
pub fn test_error_classifier() -> ErrorClassifier {
    ErrorClassifier::new(|e| {
        e.error_chain().any(|e| {
            e.downcast_ref::<TestError>()
                .map(|e| e.retry)
                .unwrap_or_default()
        })
    })
}

#[cfg(test)]
mod tests {
    use crate::assert_impl;

    use super::*;

    assert_impl!(dyn_classifier_is_send, ErrorClassifier, Send);
    assert_impl!(dyn_classifier_is_sync, ErrorClassifier, Sync);
}
