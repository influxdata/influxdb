//! The encoding mechanism for list streams
//!
//! This is capable of streaming both keys and values, this saves round-trips when hydrating
//! a cache from a remote, and avoids creating a flood of HTTP GET requests

/// The maximum value size to send in a [`ListEntry`]
///
/// This primarily exists as a self-protection limit to prevent large or corrupted streams
/// from swamping the client, but also mitigates Head-Of-Line blocking resulting from
/// large cache values
pub const MAX_VALUE_SIZE: usize = 1024 * 10;

pub mod v2;

use bytes::Bytes;
use generated_types::prost;
use snafu::Snafu;
use std::sync::Arc;

use crate::{CacheKey, CacheValue};

/// Error type for list streams
#[derive(Debug, Snafu)]
#[expect(missing_docs)]
pub enum Error {
    #[snafu(display("Unexpected EOF whilst decoding list stream"))]
    UnexpectedEOF,

    #[snafu(display("List value of {size} bytes too large"))]
    ValueTooLarge { size: usize },

    #[snafu(display("Invalid entry: {source}"), context(false))]
    InvalidEntry { source: prost::DecodeError },

    #[snafu(display("List request error: {source}"), context(false))]
    Reqwest { source: reqwest::Error },

    #[snafu(display("Server indicates unsupported LIST protocol version: {version}"))]
    UnsupportedProtocol { version: String },
}

/// Result type for list streams
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A key value pair encoded as part of a list
///
/// Unlike [`CacheKey`] and [`CacheValue`] this allows:
///
/// * Non-fatal handling of unknown key variants
/// * The option to not include the value data, e.g. if too large
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ListEntry {
    key: Option<CacheKey>,
    generation: u64,
    data: Option<Bytes>,
    etag: Option<Arc<str>>,
}

impl ListEntry {
    /// Create a new [`ListEntry`] from the provided key and value
    pub fn new(key: CacheKey, value: CacheValue) -> Self {
        Self {
            key: Some(key),
            generation: value.generation,
            data: value.data,
            etag: value.etag,
        }
    }

    /// The key if it matches a known variant of [`CacheKey`]
    ///
    /// Returns `None` otherwise
    pub fn key(&self) -> Option<CacheKey> {
        self.key
    }

    /// The generation of this entry
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Returns the value data if present
    pub fn value(&self) -> Option<&Bytes> {
        self.data.as_ref()
    }

    /// Returns the etag if any
    pub fn etag(&self) -> Option<&Arc<str>> {
        self.etag.as_ref()
    }
}
