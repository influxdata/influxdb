/// An HTTP request error.
///
/// This is a non-application level error returned when an HTTP request to the
/// IOx server has failed.
#[derive(Debug)]
pub struct HttpError(reqwest::Error);

// This wrapper type decouples the underlying HTTP client, ensuring the HTTP
// library error doesn't become part of the public API. This makes bumping the
// internal http client version / swapping it for something else a backwards
// compatible change.
//
// The reqwest error isn't exactly full of useful information beyond the Display
// impl (which this wrapper passes through) anyway.

impl std::fmt::Display for HttpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for HttpError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}

/// Convert errors from the underlying HTTP client into `HttpError` instances.
impl From<reqwest::Error> for HttpError {
    fn from(v: reqwest::Error) -> Self {
        Self(v)
    }
}
