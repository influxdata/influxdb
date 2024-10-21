use serde::Deserialize;
use serde::Serialize;
use std::fmt::Display;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

#[derive(Debug, Copy, Clone, Eq, PartialOrd, Ord, PartialEq, Serialize, Deserialize, Hash)]
pub struct DbId(u32);

static NEXT_DB_ID: AtomicU32 = AtomicU32::new(0);

impl DbId {
    pub fn new() -> Self {
        Self(NEXT_DB_ID.fetch_add(1, Ordering::SeqCst))
    }

    pub fn next_id() -> DbId {
        Self(NEXT_DB_ID.load(Ordering::SeqCst))
    }

    pub fn set_next_id(&self) {
        NEXT_DB_ID.store(self.0, Ordering::SeqCst)
    }

    pub fn as_u32(&self) -> u32 {
        self.0
    }
}

impl Default for DbId {
    fn default() -> Self {
        Self::new()
    }
}

impl From<u32> for DbId {
    fn from(value: u32) -> Self {
        Self(value)
    }
}
impl Display for DbId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialOrd, Ord, PartialEq, Serialize, Deserialize, Hash)]
pub struct TableId(u32);

static NEXT_TABLE_ID: AtomicU32 = AtomicU32::new(0);

impl TableId {
    pub fn new() -> Self {
        Self(NEXT_TABLE_ID.fetch_add(1, Ordering::SeqCst))
    }

    pub fn next_id() -> Self {
        Self(NEXT_TABLE_ID.load(Ordering::SeqCst))
    }

    pub fn set_next_id(&self) {
        NEXT_TABLE_ID.store(self.0, Ordering::SeqCst)
    }

    pub fn as_u32(&self) -> u32 {
        self.0
    }
}

impl Default for TableId {
    fn default() -> Self {
        Self::new()
    }
}

impl From<u32> for TableId {
    fn from(value: u32) -> Self {
        Self(value)
    }
}

impl Display for TableId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialOrd, Ord, PartialEq, Serialize, Deserialize, Hash)]
pub struct ColumnId(u16);

impl ColumnId {
    pub fn new(id: u16) -> Self {
        Self(id)
    }

    pub fn next_id(&self) -> Self {
        Self(self.0 + 1)
    }

    pub fn as_u16(&self) -> u16 {
        self.0
    }
}
impl From<u16> for ColumnId {
    fn from(value: u16) -> Self {
        Self(value)
    }
}

impl Display for ColumnId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// The next file id to be used when persisting `ParquetFile`s
pub static NEXT_FILE_ID: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Copy, Clone, PartialOrd, Ord, Hash)]
/// A newtype wrapper for ids used with `ParquetFile`
pub struct ParquetFileId(u64);

impl ParquetFileId {
    pub fn new() -> Self {
        Self(NEXT_FILE_ID.fetch_add(1, Ordering::SeqCst))
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
