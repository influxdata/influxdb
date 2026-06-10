//! Catalog file reader with CRC verification.
//!
//! Provides parsing and validation for binary catalog files (logs and snapshots).

use std::io::Cursor;

use super::{FormatError, Header, RECORD_HEADER_SIZE, Record};

/// A parsed catalog file: header plus records.
///
/// Logs and snapshots share the same payload shape — records back-to-back in
/// write order — and are distinguished only by the header `SNAPSHOT` flag
/// (`Header::is_snapshot`).
#[derive(Debug, Clone)]
pub struct CatalogFile {
    /// File header with metadata.
    pub header: Header,
    /// Records in write order; for snapshots, this is application order.
    pub records: Vec<Record>,
}

impl CatalogFile {
    /// Parse and validate a catalog file from a cursor, verifying the payload
    /// CRC.
    pub fn read_from<T: AsRef<[u8]>>(cursor: &mut Cursor<T>) -> Result<Self, FormatError> {
        Self::read_inner(cursor, true)
    }

    /// Parse a catalog file WITHOUT verifying the payload CRC. The header CRC is
    /// still verified. Use for best-effort inspection of suspected-corrupt
    /// files.
    pub fn read_from_lenient<T: AsRef<[u8]>>(cursor: &mut Cursor<T>) -> Result<Self, FormatError> {
        Self::read_inner(cursor, false)
    }

    fn read_inner<T: AsRef<[u8]>>(
        cursor: &mut Cursor<T>,
        verify_payload_crc: bool,
    ) -> Result<Self, FormatError> {
        let header = Header::read_from(cursor)?;

        // Verify payload CRC over the next payload_len bytes.
        let pos = cursor.position() as usize;
        let payload_len = header.payload_len as usize;
        let underlying = cursor.get_ref().as_ref();
        let payload_end = pos + payload_len;
        if payload_end > underlying.len() {
            return Err(FormatError::BufferTooShort {
                expected: payload_end,
                actual: underlying.len(),
            });
        }
        if verify_payload_crc {
            let computed_crc = crc32fast::hash(&underlying[pos..payload_end]);
            if computed_crc != header.payload_crc {
                return Err(FormatError::Crc32Mismatch {
                    expected: header.payload_crc,
                    computed: computed_crc,
                });
            }
        }

        let records = read_records(cursor, &header)?;

        Ok(Self { header, records })
    }

    /// Get the sequence number from the file header.
    pub fn sequence_number(&self) -> u64 {
        self.header.sequence_number
    }

    /// Get the number of records in the file.
    pub fn record_count(&self) -> usize {
        self.records.len()
    }
}

fn read_records<T: AsRef<[u8]>>(
    cursor: &mut Cursor<T>,
    header: &Header,
) -> Result<Vec<Record>, FormatError> {
    let max_possible = header.payload_len as usize / RECORD_HEADER_SIZE;
    let capacity = (header.record_count as usize).min(max_possible);
    let mut records = Vec::with_capacity(capacity);
    for _ in 0..header.record_count {
        records.push(Record::read_from(cursor)?);
    }
    Ok(records)
}

#[cfg(test)]
mod tests;
