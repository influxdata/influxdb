use serde::Deserialize;
use serde::Serialize;
use std::fmt::Display;
use std::hash::Hash;
use std::num::ParseIntError;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};

mod serialize;
pub use serialize::{SerdeVecMap, SerdeVecSet};

pub trait CatalogId: Default + Hash + Eq + Copy + Ord + Serialize {
    type Integer;

    fn next(&self) -> Self;
}

#[derive(Debug, thiserror::Error)]
#[error("failed to parse as integer: {0}")]
pub struct IdParseError(#[from] ParseIntError);

macro_rules! catalog_identifier_type {
    ($name:ident, $ty:ty) => {
        #[derive(
            Debug, Copy, Clone, Eq, PartialOrd, Ord, PartialEq, Serialize, Deserialize, Hash,
        )]
        pub struct $name($ty);

        impl CatalogId for $name {
            type Integer = $ty;

            fn next(&self) -> Self {
                Self::new(self.0.checked_add(1).expect("incrementing id overflow"))
            }
        }

        impl $name {
            pub fn new(id: $ty) -> Self {
                Self(id)
            }

            pub fn get(&self) -> $ty {
                self.0
            }
        }

        impl From<$ty> for $name {
            fn from(int: $ty) -> Self {
                Self::new(int)
            }
        }

        impl Default for $name {
            /// The default for any identifier type is 0
            fn default() -> Self {
                Self::new(0)
            }
        }

        impl Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        impl FromStr for $name {
            type Err = IdParseError;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                s.parse().map(Self).map_err(Into::into)
            }
        }
    };
}

catalog_identifier_type!(NodeId, u32);
catalog_identifier_type!(DbId, u32);
catalog_identifier_type!(TableId, u32);
catalog_identifier_type!(TriggerId, u32);
catalog_identifier_type!(ColumnId, u16);
catalog_identifier_type!(LastCacheId, u16);
catalog_identifier_type!(DistinctCacheId, u16);
catalog_identifier_type!(TokenId, u64);

/// The next file id to be used when persisting `ParquetFile`s
pub static NEXT_FILE_ID: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Copy, Clone, PartialOrd, Ord, Hash)]
/// A newtype wrapper for ids used with `ParquetFile`
pub struct ParquetFileId(u64);

impl ParquetFileId {
    pub fn new() -> Self {
        Self(
            NEXT_FILE_ID
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_add(1))
                .expect("Overflowed with Parquet File IDs"),
        )
    }

    pub fn next_id() -> Self {
        Self(NEXT_FILE_ID.load(Ordering::SeqCst))
    }

    pub fn set_next_id(&self) {
        NEXT_FILE_ID.store(self.0, Ordering::SeqCst)
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl From<u64> for ParquetFileId {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl Default for ParquetFileId {
    fn default() -> Self {
        Self::new()
    }
}
