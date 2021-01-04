//! This module contains code for abstracting object locations that work
//! across different backing implementations and platforms.
use percent_encoding::{percent_decode, percent_encode};
use std::{
    fmt::{self, Formatter},
    path::PathBuf,
};

/// Universal interface for handling paths and locations for objects and
/// directories in the object store.
pub struct ObjectStorePath {
    path: PathSource,
}

impl ObjectStorePath {
    /// Pushes a part onto the path. Ensures the part is percent encoded
    pub fn push(&mut self, part: &str) {}

    /// Returns a path that is safe for use in S3, GCP, Azure, and Memory
    pub fn to_object_store_path(&self) -> &str {
        unimplemented!()
    }

    /// Returns a path that is safe for use in File store
    pub fn to_path_buf(&self) -> &PathBuf {
        unimplemented!()
    }
}

enum PathSource {
    String(String),
    PathBuf(PathBuf),
}

impl From<&str> for ObjectStorePath {
    fn from(path: &str) -> Self {
        Self {
            path: PathSource::String(path.into()),
        }
    }
}

impl From<PathBuf> for ObjectStorePath {
    fn from(path_buf: PathBuf) -> Self {
        Self {
            path: PathSource::PathBuf(path_buf),
        }
    }
}

// Show the path as it was passed in (not percent encoded)
impl fmt::Display for ObjectStorePath {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        unimplemented!()
    }
}

// Show the PathBuf debug or the percent encoded and display path
impl fmt::Debug for ObjectStorePath {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        unimplemented!()
    }
}

// The delimiter to separate object namespaces, creating a directory structure.
const DELIMITER: &str = "/";

#[cfg(test)]
mod tests {
    use super::*;
}
