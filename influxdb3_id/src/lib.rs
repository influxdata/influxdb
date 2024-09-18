use serde::Deserialize;
use serde::Serialize;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;

#[derive(Debug, Copy, Clone, Eq, PartialOrd, Ord, PartialEq, Serialize, Deserialize)]
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
