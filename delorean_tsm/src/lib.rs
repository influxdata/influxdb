#![deny(rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self
)]

pub mod encoders;
pub mod key;
pub mod mapper;
pub mod reader;

use std::convert::TryFrom;
use std::error;
use std::fmt;
use std::io;

pub use key::ParsedTSMKey;

#[derive(Clone, Debug, Copy, PartialEq)]
pub enum BlockType {
    Float,
    Integer,
    Bool,
    Str,
    Unsigned,
}

impl TryFrom<u8> for BlockType {
    type Error = TSMError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Float),
            1 => Ok(Self::Integer),
            2 => Ok(Self::Bool),
            3 => Ok(Self::Str),
            4 => Ok(Self::Unsigned),
            _ => Err(TSMError {
                description: format!("{:?} is invalid block type", value),
            }),
        }
    }
}

/// `Block` holds information about location and time range of a block of data.
#[derive(Debug, Copy, Clone, PartialEq)]
#[allow(dead_code)]
pub struct Block {
    pub min_time: i64,
    pub max_time: i64,
    pub offset: u64,
    pub size: u32,
    pub typ: BlockType,
    pub reader_idx: usize,
}

// MAX_BLOCK_VALUES is the maximum number of values a TSM block can store.
const MAX_BLOCK_VALUES: usize = 1000;

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
/// `InfluxID` represents an InfluxDB ID used in InfluxDB 2.x to represent
/// organization and bucket identifiers.
pub struct InfluxID(u64);

#[allow(dead_code)]
impl InfluxID {
    fn new_str(s: &str) -> Result<Self, TSMError> {
        let v = u64::from_str_radix(s, 16).map_err(|e| TSMError {
            description: e.to_string(),
        })?;
        Ok(Self(v))
    }

    fn from_be_bytes(bytes: [u8; 8]) -> Self {
        Self(u64::from_be_bytes(bytes))
    }
}

impl std::fmt::Display for InfluxID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(f, "{:016x}", self.0)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TSMError {
    pub description: String,
}

impl fmt::Display for TSMError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.description)
    }
}

impl error::Error for TSMError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        // Generic error, underlying cause isn't tracked.
        None
    }
}

impl From<io::Error> for TSMError {
    fn from(e: io::Error) -> Self {
        Self {
            description: format!("TODO - io error: {} ({:?})", e, e),
        }
    }
}

impl From<std::str::Utf8Error> for TSMError {
    fn from(e: std::str::Utf8Error) -> Self {
        Self {
            description: format!("TODO - utf8 error: {} ({:?})", e, e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn influx_id() {
        let id = InfluxID::new_str("20aa9b0").unwrap();
        assert_eq!(id, InfluxID(34_253_232));
        assert_eq!(format!("{}", id), "00000000020aa9b0");
    }
}
