//! HTTP authorisation helpers.

use http::HeaderValue;

/// We strip off the "authorization" header from the request, to prevent it from being accidentally logged
/// and we put it in an extension of the request. Extensions are typed and this is the typed wrapper that
/// holds an (optional) authorization header value.
pub struct AuthorizationHeaderExtension(Option<HeaderValue>);

impl AuthorizationHeaderExtension {
    /// Construct new extension wrapper for a possible header value
    pub fn new(header: Option<HeaderValue>) -> Self {
        Self(header)
    }
}

impl std::fmt::Debug for AuthorizationHeaderExtension {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AuthorizationHeaderExtension(...)")
    }
}

impl std::ops::Deref for AuthorizationHeaderExtension {
    type Target = Option<HeaderValue>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
