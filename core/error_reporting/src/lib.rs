//! Utilities for error reporting in IOx

use workspace_hack as _;

use core::error::Error;
use core::fmt::Display;

/// `DisplaySourceChain` wraps any other [Error] type and modifies the error's [Display]
/// implementation to write all error messages from all its error sources.
///
/// Without multi-line cause chain reporting, error messages reported by IOx look like this:
///
/// "top-level error: source error 1: source error 2: source error 3 ..."
///
/// However, this relies on the [Display] implementation of each error in the chain to
/// follow the same convention of embedding the message of its source error into its own error
/// message. Many third-party crates do not follow this convention, for good reason, and assume
/// that the application will use an error reporter the walks the source chain itself.
///
/// This wrapper attempts to resolve the incompatibility between these two conventions by walking
/// the error chain, checking to see if the source error's message is already contained within the
/// top-level error string, and appends it to the string if it is not already present. It uses the
/// conventional ": " substring as a delimiter to detect different error message parts
///
/// Here is an example to show this behavior in code:
///
/// ```rust
/// use error_reporting::DisplaySourceChain;
/// use snafu::prelude::*;
/// use core::error::Error;
///
/// #[derive(Debug, Snafu)]
/// /// An example error enum
/// enum MyError {
///    /// A simple variant with no inner source error
///    #[snafu(display("simple error"))]
///    Simple,
///
///    /// This variant displays its source error in its display message
///    #[snafu(display("{message}: {source}"))]
///    Chatty {
///        message: &'static str,
///        source: Box<MyError>,
///    },
///
///    /// This variant does not display its source error in its display message
///    #[snafu(display("{message}"))]
///    Concise {
///        message: &'static str,
///        source: Box<MyError>,
///    },
/// }
///
/// // Create a complicated error chain
/// let err = MyError::Chatty {
///     message: "Top-level Error",
///     source: Box::new(MyError::Concise {
///         message: "Source Error 1",
///         source: Box::new(MyError::Chatty {
///             message: "Source Error 2",
///             source: Box::new(MyError::Concise {
///                 message: "Source Error 3",
///                 source: Box::new(MyError::Simple)
///             }),
///         }),
///     }),
/// };
///
/// // Without DisplaySourceChain, the error is truncated
/// assert_eq!(err.to_string(), "Top-level Error: Source Error 1");
///
/// // With DisplaySourceChain, Every message will be displayed exactly once
/// assert_eq!(DisplaySourceChain::new(err).to_string(), "Top-level Error: Source Error 1: Source Error 2: Source Error 3: simple error");
/// ```
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct DisplaySourceChain<T>(T);

impl<T: Error + 'static> DisplaySourceChain<T> {
    ///  Wrap an error in DisplaySourceChain
    pub fn new(err: T) -> Self {
        Self(err)
    }

    /// Take the inner error value
    pub fn into_inner(self) -> T {
        self.0
    }

    /// Reference the inner error value
    pub fn inner(&self) -> &T {
        &self.0
    }
}

impl<T: Error + 'static> Display for DisplaySourceChain<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // walk the source chain and collect error messages
        let mut err_msgs = Vec::new();
        let mut current_err = Some(&self.0 as &(dyn Error + 'static));
        while let Some(err) = current_err {
            let err_msg = err.to_string();
            err_msgs.push(err_msg);
            current_err = err.source();
        }
        // produce output message parts from source error messages
        // message parts are delimited by the substring ": "
        let mut out_parts = Vec::with_capacity(err_msgs.capacity());
        for err_msg in &err_msgs {
            // not very clean but std lib doesn't easily support splitting on two substrings
            for err_part in err_msg.split(": ").flat_map(|s| s.split("\ncaused by\n")) {
                if !err_part.is_empty() && !out_parts.contains(&err_part) {
                    out_parts.push(err_part);
                }
            }
        }
        write!(f, "{}", out_parts.join(": "))?;
        Ok(())
    }
}

impl<T: Error + 'static> Error for DisplaySourceChain<T> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.source()
    }
}

#[cfg(test)]
mod test {
    use datafusion::error::DataFusionError;
    use snafu::Snafu;

    use super::*;

    #[derive(Debug, Snafu)]
    /// An example error enum
    enum TestError {
        /// Error with no inner source error
        #[snafu(display("{message}"))]
        NoSource { message: &'static str },

        /// Error with an inner source error
        #[snafu(display("{message}"))]
        WithSource {
            message: &'static str,
            source: Box<dyn Error>,
        },
    }

    #[test]
    fn display_source_chain_overlapping_msgs() {
        // Create a complicated error chain with overlapping messages
        let err = TestError::WithSource {
            message: "Top-level Error: Source Error 1: Source Error 2",
            source: Box::new(TestError::WithSource {
                message: "Source Error 1: Source Error 2",
                source: Box::new(TestError::WithSource {
                    message: "Source Error 2: Source Error 3 (http://some/url)",
                    source: Box::new(TestError::WithSource {
                        message: "Source Error 3 (http://some/url): Source Error 4 (some::rust::namespace) : a: a: b: c",
                        source: Box::new(TestError::NoSource {
                            message: "Source Error 4 (some::rust::namespace) : a: a: b: c",
                        }),
                    }),
                }),
            }),
        };
        assert_eq!(
            err.to_string(),
            "Top-level Error: Source Error 1: Source Error 2"
        );
        //known issue: cannot handle duplicated message parts
        assert_eq!(
            DisplaySourceChain::new(err).to_string(),
            "Top-level Error: Source Error 1: Source Error 2: Source Error 3 (http://some/url): Source Error 4 (some::rust::namespace) : a: b: c"
        );
    }

    #[test]
    // Test that DataFusion error context is displayed at least once
    fn display_df_context() {
        // Create a complicated error chain with overlapping messages
        let err = DataFusionError::Plan("test error".to_string())
            .context("context message 1")
            .context("context message 2");
        assert_eq!(
            DisplaySourceChain::new(err).to_string(),
            "context message 2: context message 1: Error during planning: test error"
        );
    }
}
