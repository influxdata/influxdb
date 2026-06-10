//! Record handling for catalog binary format.
//!
//! Each record consists of a 16-byte header followed by variable-length data.
//!
//! # Record Header Layout (16 bytes, little-endian)
//!
//! The two `u16` fields (`id` and `flags`) are packed into the first 4-byte
//! word. All subsequent fields are 4-byte aligned.
//!
//! | Offset | Size | Type  | Field    | Description                              |
//! |--------|------|-------|----------|------------------------------------------|
//! | 0x00   | 2    | `u16` | id       | Record type identifier (`RecordId`)      |
//! | 0x02   | 2    | `u16` | flags    | `RecordFlags` bitfield                       |
//! | 0x04   | 8    | `u64` | sequence | Catalog sequence when record was written |
//! | 0x0C   | 4    | `u32` | length   | Byte length of data following header     |
//!
//! Total header: 0x10 = 16 bytes

use std::io::{Cursor, Read};

use bytes::{Buf, Bytes};

use super::registry::{CatalogRecord, MakeRecord};
use super::{FormatError, RecordFlags};

/// Size of the record header in bytes.
pub const RECORD_HEADER_SIZE: usize = 16;

// Compile-time assertion
const _: () = assert!(RECORD_HEADER_SIZE == 16);

/// Header for a single record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordHeader {
    /// Record type identifier.
    pub id: u16,
    /// Record flags.
    pub flags: RecordFlags,
    /// Catalog sequence number when this record was written.
    pub sequence: u64,
    /// Length of data following this header.
    pub length: u32,
}

impl RecordHeader {
    /// Parse record header from bytes.
    pub fn from_bytes(buf: &[u8]) -> Result<Self, FormatError> {
        if buf.len() < RECORD_HEADER_SIZE {
            return Err(FormatError::BufferTooShort {
                expected: RECORD_HEADER_SIZE,
                actual: buf.len(),
            });
        }

        let mut reader = buf;
        let id = reader.get_u16_le();
        let flags = RecordFlags::from_u16(reader.get_u16_le());
        let sequence = reader.get_u64_le();
        let length = reader.get_u32_le();

        Ok(Self {
            id,
            flags,
            sequence,
            length,
        })
    }

    /// Serialize record header to bytes.
    pub fn to_bytes(&self) -> [u8; RECORD_HEADER_SIZE] {
        let mut buf = [0u8; RECORD_HEADER_SIZE];

        buf[0x00..0x02].copy_from_slice(&self.id.to_le_bytes());
        buf[0x02..0x04].copy_from_slice(&self.flags.to_u16().to_le_bytes());
        buf[0x04..0x0C].copy_from_slice(&self.sequence.to_le_bytes());
        buf[0x0C..0x10].copy_from_slice(&self.length.to_le_bytes());

        buf
    }

    /// Parse record header from a cursor.
    pub fn read_from<T: AsRef<[u8]>>(cursor: &mut Cursor<T>) -> Result<Self, FormatError> {
        let mut buf = [0u8; RECORD_HEADER_SIZE];
        cursor
            .read_exact(&mut buf)
            .map_err(|_| FormatError::BufferTooShort {
                expected: RECORD_HEADER_SIZE,
                actual: cursor.get_ref().as_ref().len(),
            })?;
        Self::from_bytes(&buf)
    }

    /// Create a new record header.
    pub fn new(id: u16, flags: RecordFlags, sequence: u64, length: u32) -> Self {
        Self {
            id,
            flags,
            sequence,
            length,
        }
    }
}

/// A complete record with header and data.
#[derive(Debug, Clone)]
pub struct Record {
    /// Record header.
    pub header: RecordHeader,
    /// Record data (zero-copy reference).
    pub data: Bytes,
}

impl Record {
    /// Create a new record with the given catalog sequence.
    pub fn new(id: u16, flags: RecordFlags, sequence: u64, data: Bytes) -> Self {
        let header = RecordHeader::new(id, flags, sequence, data.len() as u32);
        Self { header, data }
    }

    /// Parse a record from a cursor.
    ///
    /// Reads the record header, then reads `length` bytes of data.
    pub fn read_from<T: AsRef<[u8]>>(cursor: &mut Cursor<T>) -> Result<Self, FormatError> {
        let header = RecordHeader::read_from(cursor)?;
        let mut data = vec![0u8; header.length as usize];
        cursor
            .read_exact(&mut data)
            .map_err(|_| FormatError::BufferTooShort {
                expected: header.length as usize,
                actual: cursor.get_ref().as_ref().len(),
            })?;
        Ok(Self {
            header,
            data: Bytes::from(data),
        })
    }

    /// Serialize the record to bytes (header + data).
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut result = Vec::with_capacity(RECORD_HEADER_SIZE + self.data.len());
        result.extend_from_slice(&self.header.to_bytes());
        result.extend_from_slice(&self.data);
        result
    }

    /// Get the record type ID.
    pub fn id(&self) -> u16 {
        self.header.id
    }

    /// Get the flags for this record.
    pub fn flags(&self) -> RecordFlags {
        self.header.flags
    }

    /// Get the sequence number for this record.
    pub fn sequence(&self) -> u64 {
        self.header.sequence
    }

    /// Check if this record is upgrade-safe (can be skipped if unknown).
    pub fn is_upgrade_safe(&self) -> bool {
        self.header.flags.is_upgrade_safe()
    }
}

/// A batch of records produced by a single catalog operation or transaction.
///
/// On the write path, `CatalogOp::prepare` accumulates records into a batch,
/// which is then serialized and persisted as a single log file. The transaction
/// path (`Catalog::commit`) uses the same mechanism for schema-on-write changes.
#[derive(Debug, Clone)]
pub struct RecordBatch {
    records: Vec<Record>,
    sequence: u64,
}

impl RecordBatch {
    pub fn new(sequence: u64) -> Self {
        Self {
            records: Vec::new(),
            sequence,
        }
    }

    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Encode a record, stamp it with the batch's sequence, and append it.
    pub fn push<R: CatalogRecord>(&mut self, record: &R) {
        self.records.push(record.make_record(self.sequence));
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    pub fn as_slice(&self) -> &[Record] {
        &self.records
    }

    pub fn len(&self) -> usize {
        self.records.len()
    }
}

#[cfg(test)]
mod tests;
