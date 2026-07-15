//! Catalog file reader with CRC verification.
//!
//! Provides parsing and validation for binary catalog files (logs and snapshots).

use std::io::Cursor;

use super::{FormatError, Header, RECORD_HEADER_SIZE, Record};

/// On-disk size of a group-index entry: the legacy `GroupIndexEntry` —
/// group_type, group_key, record_count and byte_length (4 bytes each) plus
/// byte_offset (8). Pre-#4026 snapshots wrote one entry per group; snapshots
/// written since carry a single backward-compatibility entry (see
/// `serialize_snapshot_file`). The reader skips them either way.
pub(crate) const LEGACY_GROUP_INDEX_ENTRY_SIZE: usize = 24;

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
        let header = Self::read_validated_header(cursor, verify_payload_crc)?;
        let records = read_records(cursor, &header)?;
        Ok(Self { header, records })
    }

    /// Validate a catalog file and return its [`Header`] without decoding the
    /// records.
    ///
    /// Performs the same integrity checks as [`Self::read_from`] — header CRC,
    /// payload-length bounds, and payload CRC32 — but skips record decoding.
    /// Use when only header metadata (e.g. the sequence number) is needed yet a
    /// truncated or corrupt file must still be rejected rather than passed
    /// through to be discovered later.
    pub fn read_verified_header<T: AsRef<[u8]>>(
        cursor: &mut Cursor<T>,
    ) -> Result<Header, FormatError> {
        Self::read_validated_header(cursor, true)
    }

    /// Read the header and validate the payload-length bounds and (when
    /// `verify_payload_crc`) the payload CRC, leaving the cursor at the start of
    /// the payload. Shared by [`Self::read_inner`] and
    /// [`Self::read_verified_header`] so the integrity checks are identical
    /// whether or not the records are then decoded.
    fn read_validated_header<T: AsRef<[u8]>>(
        cursor: &mut Cursor<T>,
        verify_payload_crc: bool,
    ) -> Result<Header, FormatError> {
        let header = Header::read_from(cursor)?;

        // Verify the payload occupies its declared `payload_len` bytes and, when
        // requested, matches the recorded CRC over that range.
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

        Ok(header)
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
    // Snapshots prefix the record payload with a group index of `group_count`
    // fixed-size entries — one per group in files written before #4026, a
    // single backward-compatibility entry in files written since. The records
    // that follow are contiguous and in application order (the pre-#4026
    // writer emitted the Global group first, then databases ascending), so
    // skip the index and read the records flat. Log files set group_count to
    // 0, making this a no-op.
    let index_len = header.group_count as usize * LEGACY_GROUP_INDEX_ENTRY_SIZE;
    if index_len > 0 {
        cursor.set_position(cursor.position() + index_len as u64);
    }

    let records_len = (header.payload_len as usize).saturating_sub(index_len);
    let max_possible = records_len / RECORD_HEADER_SIZE;
    let capacity = (header.record_count as usize).min(max_possible);
    let mut records = Vec::with_capacity(capacity);
    for _ in 0..header.record_count {
        records.push(Record::read_from(cursor)?);
    }
    Ok(records)
}

#[cfg(test)]
mod tests;
