use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub enum Error {
    Serialization(serde_json::Error),
    Yaml(serde_yaml::Error),
    Http(http::Error),
    Hyper(hyper::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Serialization(e) => e.fmt(f),
            Self::Yaml(e) => e.fmt(f),
            Self::Http(e) => e.fmt(f),
            Self::Hyper(e) => e.fmt(f),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Serialization(e) => Some(e),
            Self::Yaml(e) => Some(e),
            Self::Http(e) => Some(e),
            Self::Hyper(e) => Some(e),
        }
    }
}

impl From<serde_json::Error> for Error {
    fn from(value: serde_json::Error) -> Self {
        Self::Serialization(value)
    }
}

impl From<serde_yaml::Error> for Error {
    fn from(value: serde_yaml::Error) -> Self {
        Self::Yaml(value)
    }
}

impl From<http::Error> for Error {
    fn from(value: http::Error) -> Self {
        Self::Http(value)
    }
}

impl From<hyper::Error> for Error {
    fn from(value: hyper::Error) -> Self {
        Self::Hyper(value)
    }
}
