#![deny(broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]

pub mod encoders;
pub mod key;
pub mod mapper;
pub mod reader;

use std::convert::TryFrom;
use std::error;
use std::fmt;
use std::io;

pub use key::ParsedTsmKey;

#[derive(Clone, Debug, Copy, PartialEq)]
pub enum BlockType {
    Float,
    Integer,
    Bool,
    Str,
    Unsigned,
}

impl TryFrom<u8> for BlockType {
    type Error = TsmError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Float),
            1 => Ok(Self::Integer),
            2 => Ok(Self::Bool),
            3 => Ok(Self::Str),
            4 => Ok(Self::Unsigned),
            _ => Err(TsmError {
                description: format!("{:?} is invalid block type", value),
            }),
        }
    }
}

/// `Block` holds information about location and time range of a block of data.
#[derive(Debug, Copy, Clone, PartialEq)]
pub struct Block {
    pub min_time: i64,
    pub max_time: i64,
    pub offset: u64,
    pub size: u32,
    pub typ: BlockType,

    // This index is used to track an associated reader needed to decode the
    // data this block holds.
    pub reader_idx: usize,
}

impl Block {
    /// Determines if this block overlaps the provided block.
    ///
    /// Blocks overlap when the time-range of the data within the block can
    /// overlap.
    pub fn overlaps(&self, other: &Self) -> bool {
        self.min_time <= other.max_time && other.min_time <= self.max_time
    }
}

// MAX_BLOCK_VALUES is the maximum number of values a TSM block can store.
const MAX_BLOCK_VALUES: usize = 1000;

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
/// `InfluxId` represents an InfluxDB ID used in InfluxDB 2.x to represent
/// organization and bucket identifiers.
pub struct InfluxId(u64);

impl InfluxId {
    #[allow(dead_code)]
    fn new_str(s: &str) -> Result<Self, TsmError> {
        let v = u64::from_str_radix(s, 16).map_err(|e| TsmError {
            description: e.to_string(),
        })?;
        Ok(Self(v))
    }

    fn from_be_bytes(bytes: [u8; 8]) -> Self {
        Self(u64::from_be_bytes(bytes))
    }
}

impl std::fmt::Display for InfluxId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(f, "{:016x}", self.0)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TsmError {
    pub description: String,
}

impl fmt::Display for TsmError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.description)
    }
}

impl error::Error for TsmError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        // Generic error, underlying cause isn't tracked.
        None
    }
}

impl From<io::Error> for TsmError {
    fn from(e: io::Error) -> Self {
        Self {
            description: format!("TODO - io error: {} ({:?})", e, e),
        }
    }
}

impl From<std::str::Utf8Error> for TsmError {
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
        let id = InfluxId::new_str("20aa9b0").unwrap();
        assert_eq!(id, InfluxId(34_253_232));
        assert_eq!(format!("{}", id), "00000000020aa9b0");
    }

    #[test]
    fn block_overlaps() {
        // ((0, 0), (0, 0), false)
        let inputs = vec![
            ((0, 10), (11, 12), false), // *---* +---+
            ((10, 20), (3, 5), false),  // +---+ *---*
            ((0, 0), (0, 0), true),     //
            ((0, 1), (1, 2), true),     // +----*----*
            ((0, 2), (1, 5), true),     // *--+-*----+
            ((0, 5), (3, 10), true),    // *--+-*----+
            ((3, 7), (0, 10), true),    // +--*----*-+
            ((0, 10), (2, 2), true),    // *--++-----*
        ];

        for (a, b, expected) in inputs {
            let block_a = Block {
                min_time: a.0,
                max_time: a.1,
                offset: 0,
                reader_idx: 0,
                typ: BlockType::Float,
                size: 0,
            };
            let block_b = Block {
                min_time: b.0,
                max_time: b.1,
                offset: 0,
                reader_idx: 0,
                typ: BlockType::Float,
                size: 0,
            };
            assert_eq!(block_a.overlaps(&block_b), expected);
            assert_eq!(block_b.overlaps(&block_a), expected);
        }
    }
}
