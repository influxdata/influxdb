use std::error::Error as StdError;
use std::fmt;

#[derive(Debug, Clone)]
pub(crate) struct ServiceError(pub String);

impl fmt::Display for ServiceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Service error: {}", self.0)
    }
}

impl StdError for ServiceError {}

impl From<String> for ServiceError {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for ServiceError {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl From<hyper::Error> for ServiceError {
    fn from(err: hyper::Error) -> Self {
        Self(err.to_string())
    }
}

impl From<std::io::Error> for ServiceError {
    fn from(err: std::io::Error) -> Self {
        Self(err.to_string())
    }
}

impl From<Box<dyn StdError + Send + Sync>> for ServiceError {
    fn from(err: Box<dyn StdError + Send + Sync>) -> Self {
        Self(err.to_string())
    }
}

impl From<std::convert::Infallible> for ServiceError {
    fn from(_: std::convert::Infallible) -> Self {
        unreachable!("Infallible error should never occur")
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RouterError(pub String);

impl fmt::Display for RouterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Router error: {}", self.0)
    }
}

impl StdError for RouterError {}

impl From<String> for RouterError {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for RouterError {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl From<hyper::Error> for RouterError {
    fn from(err: hyper::Error) -> Self {
        Self(err.to_string())
    }
}

impl From<std::io::Error> for RouterError {
    fn from(err: std::io::Error) -> Self {
        Self(err.to_string())
    }
}

impl From<Box<dyn StdError + Send + Sync>> for RouterError {
    fn from(err: Box<dyn StdError + Send + Sync>) -> Self {
        Self(err.to_string())
    }
}
