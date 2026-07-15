//! Type-safe record ID with core/enterprise partitioning.
//!
//! Bit 15 of the raw `u16` value partitions the ID space:
//! - Core records: bit 15 = 0 (raw values 0x0001–0x7FFF)
//! - Enterprise records: bit 15 = 1 (raw values 0x8001–0xFFFF)

const ENTERPRISE_BIT: u16 = 0x8000;
const ENTERPRISE_BIT_MASK: u16 = !ENTERPRISE_BIT;

/// A catalog record identifier.
///
/// Use `RecordId::core(seq)` for core records and `RecordId::enterprise(seq)`
/// for enterprise-only records. The type enforces correct partition membership
/// at compile time via `const` constructors with assertions.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize)]
#[serde(transparent)]
pub struct RecordId(u16);

impl RecordId {
    /// Create a core record ID. Raw value = seq (bit 15 = 0).
    ///
    /// # Panics
    /// Panics at compile time if seq is 0 or >= 0x8000.
    pub const fn core(seq: u16) -> Self {
        assert!(
            seq > 0 && seq < ENTERPRISE_BIT,
            "core seq must be in 1..32767"
        );
        Self(seq)
    }

    /// Create an enterprise record ID. Raw value = seq | 0x8000 (bit 15 = 1).
    ///
    /// # Panics
    /// Panics at compile time if seq is 0 or >= 0x8000.
    pub const fn enterprise(seq: u16) -> Self {
        assert!(
            seq > 0 && seq < ENTERPRISE_BIT,
            "enterprise seq must be in 1..32767"
        );
        Self(seq | ENTERPRISE_BIT)
    }

    /// Whether this is an enterprise record ID (bit 15 set).
    pub const fn is_enterprise(self) -> bool {
        self.0 & ENTERPRISE_BIT != 0
    }

    pub fn kind(self) -> RecordIdKind {
        if self.is_enterprise() {
            RecordIdKind::Enterprise(self.0 & ENTERPRISE_BIT_MASK)
        } else {
            RecordIdKind::Core(self.0)
        }
    }

    pub fn as_enterprise(self) -> Option<u16> {
        self.is_enterprise().then_some(self.0 & ENTERPRISE_BIT_MASK)
    }

    /// The raw `u16` value persisted in record headers.
    pub const fn raw(self) -> u16 {
        self.0
    }

    /// Construct from a raw `u16` read from a record header.
    ///
    /// This does not validate the value — it may be an unknown ID.
    /// Use this only on the read path when decoding persisted data.
    pub const fn from_raw(raw: u16) -> Self {
        Self(raw)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecordIdKind {
    Core(u16),
    Enterprise(u16),
}

impl std::fmt::Display for RecordId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_enterprise() {
            write!(f, "enterprise({})", self.0 & !ENTERPRISE_BIT)
        } else {
            write!(f, "core({})", self.0)
        }
    }
}

#[cfg(test)]
mod tests;
