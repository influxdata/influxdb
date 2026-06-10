//! File header for catalog binary format.
//!
//! The header is 64 bytes with all fields 4-byte aligned.
//!
//! # Layout (64 bytes, little-endian)
//!
//! | Offset | Size | Type       | Field           | Description                                     |
//! |--------|------|------------|-----------------|-------------------------------------------------|
//! | 0x00   | 4    | `[u8; 4]`  | magic           | `"IDB3"`                                        |
//! | 0x04   | 4    | `u32`      | format_version  | Currently `1`                                   |
//! | 0x08   | 4    | `u32`      | header_crc      | CRC32 of header bytes 0x0C–0x3F                 |
//! | 0x0C   | 2    | `u16`      | flags           | File flags (SNAPSHOT)                            |
//! | 0x0E   | 2    | N/A        | reserved        | Zeros                                           |
//! | 0x10   | 16   | `[u8; 16]` | catalog_uuid    | Unique catalog instance ID                      |
//! | 0x20   | 8    | `u64`      | sequence_number | Catalog sequence for this file                  |
//! | 0x28   | 4    | `u32`      | record_count    | Total number of records in payload               |
//! | 0x2C   | 4    | N/A        | reserved        | Zeros                                           |
//! | 0x30   | 8    | `u64`      | payload_len     | Byte length of payload                           |
//! | 0x38   | 4    | `u32`      | payload_crc     | CRC32 of the payload bytes                       |
//! | 0x3C   | 4    | N/A        | reserved        | Zeros                                           |
//!
//! Total: 0x40 = 64 bytes

use std::io::{Cursor, Read};

use bytes::Buf;
use uuid::Uuid;

use super::{FormatError, MAGIC};

/// File flags bitfield.
pub mod file_flags {
    /// No flags set — plain log file.
    pub const NONE: u16 = 0;
    /// File is a snapshot. If unset, file is a log.
    pub const SNAPSHOT: u16 = 1 << 0;
}

/// File header containing metadata about the catalog file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    /// Wire format version (currently 1).
    pub format_version: u32,
    /// File flags.
    pub flags: u16,
    /// Catalog UUID for the catalog that created the file.
    pub catalog_uuid: u128,
    /// Catalog sequence number.
    pub sequence_number: u64,
    /// Number of records in the file.
    pub record_count: u32,
    /// Length of the record payload in bytes.
    pub payload_len: u64,
    /// CRC32 of the record payload.
    pub payload_crc: u32,
}

impl Header {
    /// Size of the file header in bytes.
    pub const SIZE: usize = 64;

    /// Current format version.
    pub const CURRENT_VERSION: u32 = 1;

    /// Offset where header_crc coverage begins.
    const CRC_START: usize = 0x0C;

    /// Parse header from bytes.
    ///
    /// Validates magic bytes, format version, and header CRC.
    fn from_bytes(buf: &[u8]) -> Result<Self, FormatError> {
        if buf.len() < Self::SIZE {
            return Err(FormatError::BufferTooShort {
                expected: Self::SIZE,
                actual: buf.len(),
            });
        }

        // Check magic bytes (0x00)
        let magic: [u8; 4] = buf[0..4].try_into().unwrap();
        if magic != MAGIC {
            return Err(FormatError::InvalidMagic {
                expected: MAGIC,
                actual: magic,
            });
        }

        // Read format_version (0x04) and header_crc (0x08)
        let mut reader = &buf[4..];
        let format_version = reader.get_u32_le();
        if format_version != Self::CURRENT_VERSION {
            return Err(FormatError::UnsupportedVersion {
                version: format_version,
            });
        }
        let header_crc = reader.get_u32_le();

        // Verify header CRC over bytes 0x0C–0x3F
        let computed_crc = crc32fast::hash(&buf[Self::CRC_START..Self::SIZE]);
        if computed_crc != header_crc {
            return Err(FormatError::HeaderCrc32Mismatch {
                expected: header_crc,
                actual: computed_crc,
            });
        }

        // Read remaining fields (0x0C onwards)
        let flags = reader.get_u16_le();
        let _reserved = reader.get_u16_le();
        let catalog_uuid = reader.get_u128_le();
        let sequence_number = reader.get_u64_le();
        let record_count = reader.get_u32_le();
        let reserved = reader.get_u32_le();
        if reserved != 0 {
            return Err(FormatError::InvalidHeader {
                reason: "reserved field at 0x2C is nonzero",
            });
        }
        let payload_len = reader.get_u64_le();
        let payload_crc = reader.get_u32_le();
        // skip trailing 4-byte reserved

        Ok(Self {
            format_version,
            flags,
            catalog_uuid,
            sequence_number,
            record_count,
            payload_len,
            payload_crc,
        })
    }

    /// Serialize header to bytes.
    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut buf = [0u8; Self::SIZE];

        // Magic (0x00)
        buf[0x00..0x04].copy_from_slice(&MAGIC);
        // Format version (0x04)
        buf[0x04..0x08].copy_from_slice(&self.format_version.to_le_bytes());
        // header_crc (0x08) — written after computing over 0x0C–0x3F
        // Flags (0x0C)
        buf[0x0C..0x0E].copy_from_slice(&self.flags.to_le_bytes());
        // Reserved (0x0E) — already zero
        // Catalog UUID (0x10)
        buf[0x10..0x20].copy_from_slice(&self.catalog_uuid.to_le_bytes());
        // Sequence number (0x20)
        buf[0x20..0x28].copy_from_slice(&self.sequence_number.to_le_bytes());
        // Record count (0x28)
        buf[0x28..0x2C].copy_from_slice(&self.record_count.to_le_bytes());
        // Reserved (0x2C) — already zero
        // Payload length (0x30)
        buf[0x30..0x38].copy_from_slice(&self.payload_len.to_le_bytes());
        // Payload CRC (0x38)
        buf[0x38..0x3C].copy_from_slice(&self.payload_crc.to_le_bytes());
        // Reserved (0x3C) — already zero

        // Compute and write header CRC over 0x0C–0x3F
        let header_crc = crc32fast::hash(&buf[Self::CRC_START..Self::SIZE]);
        buf[0x08..0x0C].copy_from_slice(&header_crc.to_le_bytes());

        buf
    }

    /// Parse header from a cursor.
    ///
    /// Reads [`Self::SIZE`] bytes from the cursor, then validates using
    /// `Self::from_bytes`.
    pub fn read_from<T: AsRef<[u8]>>(cursor: &mut Cursor<T>) -> Result<Self, FormatError> {
        let mut buf = [0u8; Self::SIZE];
        cursor
            .read_exact(&mut buf)
            .map_err(|_| FormatError::BufferTooShort {
                expected: Self::SIZE,
                actual: cursor.get_ref().as_ref().len(),
            })?;
        Self::from_bytes(&buf)
    }

    /// Returns true if this file is a snapshot.
    pub fn is_snapshot(&self) -> bool {
        self.flags & file_flags::SNAPSHOT != 0
    }

    pub fn catalog_uuid(&self) -> Uuid {
        Uuid::from_u128_le(self.catalog_uuid)
    }
}

#[cfg(test)]
mod tests;
