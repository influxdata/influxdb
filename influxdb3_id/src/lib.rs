use serde::Deserialize;
use serde::Serialize;
use std::fmt::Display;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

mod serialize;
pub use serialize::SerdeVecMap;

macro_rules! catalog_identifier_type {
    ($name:ident, $atomic:ident) => {
        #[derive(
            Debug, Copy, Clone, Eq, PartialOrd, Ord, PartialEq, Serialize, Deserialize, Hash,
        )]
        pub struct $name(u32);

        static $atomic: AtomicU32 = AtomicU32::new(0);

        impl $name {
            pub fn new() -> Self {
                Self(
                    $atomic
                        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_add(1))
                        .expect("overflowed u32 when generating new identifier"),
                )
            }

            pub fn next_id() -> $name {
                Self($atomic.load(Ordering::SeqCst))
            }

            pub fn set_next_id(&self) {
                $atomic.store(self.0, Ordering::SeqCst)
            }

            pub fn as_u32(&self) -> u32 {
                self.0
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl From<u32> for $name {
            fn from(value: u32) -> Self {
                Self(value)
            }
        }
        impl Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }
    };
}

catalog_identifier_type!(DbId, NEXT_DB_ID);
catalog_identifier_type!(TableId, NEXT_TABLE_ID);
catalog_identifier_type!(ColumnId, NEXT_COLUMN_ID);

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

#[cfg(test)]
mod tests {
    use crate::DbId;

    #[test]
    #[should_panic]
    fn test_overflow_error() {
        DbId::from(u32::MAX).set_next_id();
        DbId::new();
    }
}
