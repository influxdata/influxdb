//! Error handling helper.
//!
//! # Dynamic Error Handling
//! This crate uses mostly dynamic / type-erased errors. The reason for this is that they compose better (also see
//! [`Layer`]). You can always inspect the concrete error by using [`ErrorChainExt`].
//!
//!
//! [`Layer`]: crate::layer::Layer

/// Dynamic error.
///
/// This is a dedicated type because `Box<dyn std::error::Error + Send + Sync>` does NOT implement [`std::error::Error`].
pub struct DynError(Box<dyn std::error::Error + Send + Sync>);

impl DynError {
    /// Create new dyn-typed error.
    pub fn new(e: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
        Self(e.into())
    }
}

impl std::fmt::Debug for DynError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.0, f)
    }
}

impl std::fmt::Display for DynError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl std::error::Error for DynError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

impl From<&str> for DynError {
    fn from(msg: &str) -> Self {
        Self(msg.into())
    }
}

impl From<String> for DynError {
    fn from(msg: String) -> Self {
        Self(msg.into())
    }
}

/// [Iterator] over error [sources](std::error::Error::source).
pub struct ErrorChainIter<'a> {
    current: Option<&'a (dyn std::error::Error + 'static)>,
}

impl<'a> Iterator for ErrorChainIter<'a> {
    type Item = &'a (dyn std::error::Error + 'static);

    fn next(&mut self) -> Option<Self::Item> {
        let mut next = self.current.as_ref().and_then(|e| e.source());
        std::mem::swap(&mut next, &mut self.current);
        next
    }
}

/// Extension trait to access the error source chain.
pub trait ErrorChainExt {
    /// Iterate over error sources, including `self`.
    fn error_chain(&self) -> ErrorChainIter<'_>;
}

impl<E> ErrorChainExt for E
where
    E: std::error::Error + 'static,
{
    fn error_chain(&self) -> ErrorChainIter<'_> {
        ErrorChainIter {
            current: Some(self),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error, ops::Deref};

    use crate::assert_impl;

    use super::*;

    assert_impl!(dyn_error_is_send, DynError, Send);
    assert_impl!(dyn_error_is_sync, DynError, Sync);

    #[test]
    fn test_wrapping() {
        let dyn_error = DynError::new(MyError::default());

        assert_eq!(format!("{dyn_error:?}"), "debug: my error 0");
        assert_eq!(format!("{dyn_error}"), "display: my error 0");

        // `source` should point to the wrapped error, NOT to its source
        assert!(dyn_error
            .source()
            .unwrap()
            .downcast_ref::<MyError>()
            .is_some());
    }

    #[test]
    fn test_chain_iter() {
        let dyn_error = MyError {
            tag: 0,
            source: Some(Box::new(MyError {
                tag: 1,
                source: Some(Box::new(MyError {
                    tag: 2,
                    source: None,
                })),
            })),
        };

        let tags = dyn_error
            .error_chain()
            .map(|e| e.downcast_ref::<MyError>().unwrap().tag)
            .collect::<Vec<_>>();
        assert_eq!(tags, vec![0, 1, 2]);
    }

    #[derive(Default)]
    struct MyError {
        tag: u64,
        source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    }

    impl std::fmt::Debug for MyError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "debug: my error {}", self.tag)
        }
    }

    impl std::fmt::Display for MyError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "display: my error {}", self.tag)
        }
    }

    impl std::error::Error for MyError {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            self.source.as_ref().map(|e| e.deref() as _)
        }
    }
}
