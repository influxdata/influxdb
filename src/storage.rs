use std::convert::TryFrom;
use std::error;
use std::fmt;

pub mod config_store;
pub mod database;
pub mod inverted_index;
pub mod memdb;
pub mod partitioned_store;
pub mod predicate;
pub mod remote_partition;
pub mod rocksdb;
pub mod s3_partition;
pub mod series_store;

// The values for these enum variants have no real meaning, but they
// are serialized to disk. Revisit these whenever it's time to decide
// on an on-disk format.
#[repr(u8)]
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum SeriesDataType {
    I64 = 0,
    F64 = 1,
    //    U64,
    //    String,
    //    Bool,
}

impl From<SeriesDataType> for u8 {
    fn from(other: SeriesDataType) -> Self {
        other as u8
    }
}

impl TryFrom<u8> for SeriesDataType {
    type Error = u8;

    fn try_from(other: u8) -> Result<Self, Self::Error> {
        use SeriesDataType::*;

        match other {
            v if v == I64 as u8 => Ok(I64),
            v if v == F64 as u8 => Ok(F64),
            _ => Err(other),
        }
    }
}

#[derive(Debug, Clone)]
pub struct StorageError {
    pub description: String,
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.description)
    }
}

impl error::Error for StorageError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        // Generic error, underlying cause isn't tracked.
        None
    }
}
