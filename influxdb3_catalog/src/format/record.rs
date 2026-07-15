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

use super::{
    FormatError, RecordFlags,
    record_id::RecordId,
    registry::{CatalogRecord, MakeRecord},
};

/// Size of the record header in bytes.
pub const RECORD_HEADER_SIZE: usize = 16;

// Compile-time assertion
const _: () = assert!(RECORD_HEADER_SIZE == 16);

/// Header for a single record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordHeader {
    /// Record type identifier.
    pub id: RecordId,
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
        let id = RecordId::from_raw(reader.get_u16_le());
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

        buf[0x00..0x02].copy_from_slice(&self.id.raw().to_le_bytes());
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
            id: RecordId::from_raw(id),
            flags,
            sequence,
            length,
        }
    }
}

/// A complete record with header and data.
#[derive(Debug, Clone)]
#[cfg_attr(test, derive(PartialEq))]
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
    pub fn id(&self) -> RecordId {
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

#[cfg(feature = "true_deletion")]
fn best_effort_remove_records_with(
    records: &mut Vec<Record>,
    mut condition: impl FnMut(&mut Record) -> Result<bool, FormatError>,
) -> Result<(), FormatError> {
    let mut res = Ok(());
    records.retain_mut(|rec| match condition(rec) {
        Ok(should_remove) => !should_remove,
        Err(e) => {
            res = Err(e);
            true
        }
    });
    res
}

#[cfg(feature = "true_deletion")]
/// Remove all records from `records` that only contain information relevant to a database in
/// `db_ids` or a table in `table_ids`. This allows us to truly hard-delete metadata, as these
/// records will be persisted to object store and we want to ensure that once we delete something,
/// it is truly permanently gone.
///
/// This also transforms the creation records for these databases into [`SetNextId`] records. This
/// is done to ensure that we don't potentially lose track of what the latest Id is in any category.
/// This also needs to be done at the creation records instead of the deletion record because that
/// is when the Id was actually incremented - if we replaced the delete record with a [`SetNextId`],
/// we might run into a situation where the list of records is transformed from:
///
/// \[
///   CreateDatabase { id: 1 }
///   CreateDatabase { id: 2 }
///   DeleteDatabase { id: 1 }
/// \]
///
/// into:
///
/// \[
///   CreateDatabase { id: 2 },
///   SetNextId { id: 1 }
/// \]
///
/// and would thus imply that the most recently used-up Id is `1`. If we instead replace the
/// creation event with the [`SetNextId`], we see that the id-incrementing records stay in their
/// natural, expected order.
///
/// # Errors
///
/// This function is best-effort; if it encounters an error, it will return that error, but not
/// before it finishes iterating through records to try to find ones relevant to these identifiers.
pub(crate) fn hard_delete_records_for(
    records: &mut Vec<Record>,
    db_ids: &std::collections::BTreeSet<influxdb3_id::DbId>,
    table_ids: &std::collections::BTreeSet<influxdb3_id::TableId>,
) -> Result<(), FormatError> {
    use super::{
        Decode, record_ids,
        records::{
            AddColumns, ClearDbRetentionPeriod, CreateDatabase, CreateDistinctCache,
            CreateLastCache, CreateTable, CreateTrigger, DeleteDistinctCache, DeleteLastCache,
            DeleteTrigger, DisableTrigger, EnableTrigger, HardDeleteDatabase, HardDeleteTable,
            NextIdScope, SetDbRetentionPeriod, SetNextId, SoftDeleteDatabase, SoftDeleteTable,
        },
    };
    use influxdb3_id::{DbId, TableId};

    best_effort_remove_records_with(records, |rec| {
        /// if these macros were fns instead, they wouldn't really save much verbosity since we'd
        /// need to define some way for the `table_id` and `database_id` fields to be extracted from
        /// the types.
        macro_rules! contains_db {
            ($t:ty) => {
                contains_db!($t, database_id)
            };
            ($t:ty, $field:ident) => {
                <$t>::decode(&rec.data).map(|rec| db_ids.contains(&DbId::new(rec.$field)))
            };
        }

        macro_rules! contains_either {
            ($t:ty) => {
                contains_either!($t, database_id)
            };
            ($t:ty, $db_field:ident) => {
                <$t>::decode(&rec.data).map(|rec| {
                    db_ids.contains(&DbId::new(rec.$db_field))
                        || table_ids.contains(&TableId::new(rec.table_id))
                })
            };
        }

        match rec.id() {
            record_ids::CREATE_DATABASE => {
                let decoded = CreateDatabase::decode(&rec.data)?;

                // If it is relevant, we want to replace it with the placeholder record.
                if db_ids.contains(&DbId::new(decoded.database_id)) {
                    *rec = SetNextId {
                        id: u64::from(decoded.database_id),
                        scope: NextIdScope::Databases,
                    }
                    .make_record(rec.sequence());
                }

                // and we never want to remove it, regardless of if it's relevant to us.
                Ok(false)
            }
            record_ids::SOFT_DELETE_DATABASE => contains_db!(SoftDeleteDatabase),
            record_ids::DELETE_DATABASE => contains_db!(HardDeleteDatabase, db_id),
            record_ids::CREATE_TABLE => {
                let decoded = CreateTable::decode(&rec.data)?;

                // if it contains a database that we want to completely delete, get rid of it.
                if db_ids.contains(&DbId::new(decoded.database_id)) {
                    return Ok(true);
                }

                // and if it instead just contains a table we're getting rid of, just replace it and
                // keep the item in the vec.
                if table_ids.contains(&TableId::new(decoded.table_id)) {
                    *rec = SetNextId {
                        id: u64::from(decoded.table_id),
                        scope: NextIdScope::Tables {
                            database_id: decoded.database_id,
                        },
                    }
                    .make_record(rec.sequence());
                }

                Ok(false)
            }
            record_ids::SOFT_DELETE_TABLE => contains_either!(SoftDeleteTable),
            record_ids::ADD_COLUMNS => contains_either!(AddColumns),
            record_ids::DELETE_TABLE => contains_either!(HardDeleteTable, db_id),
            record_ids::CREATE_TRIGGER => contains_db!(CreateTrigger),
            record_ids::DELETE_TRIGGER => contains_db!(DeleteTrigger),
            record_ids::ENABLE_TRIGGER => contains_db!(EnableTrigger, db_id),
            record_ids::DISABLE_TRIGGER => contains_db!(DisableTrigger, db_id),
            record_ids::SET_DB_RETENTION_PERIOD => contains_db!(SetDbRetentionPeriod),
            record_ids::CLEAR_DB_RETENTION_PERIOD => contains_db!(ClearDbRetentionPeriod),
            record_ids::CREATE_DISTINCT_CACHE => contains_either!(CreateDistinctCache, db_id),
            record_ids::DELETE_DISTINCT_CACHE => contains_either!(DeleteDistinctCache, db_id),
            record_ids::CREATE_LAST_CACHE => contains_either!(CreateLastCache, db_id),
            record_ids::DELETE_LAST_CACHE => contains_either!(DeleteLastCache, db_id),
            // these `SetNextId`s may not be relevant anymore if the container that they're relevant
            // to is being completely removed. E.g. if we previously hard-deleted a table, replacing
            // its creation record with a SetNextId, and then hard-delete the database it's a part
            // of, we want that `SetNextId` to be removed as well.
            record_ids::SET_NEXT_ID => SetNextId::decode(&rec.data).map(|rec| match rec.scope {
                NextIdScope::Tables { database_id } | NextIdScope::Triggers { database_id } => {
                    db_ids.contains(&DbId::new(database_id))
                }
                NextIdScope::Columns {
                    database_id,
                    table_id,
                }
                | NextIdScope::FieldFamilies {
                    database_id,
                    table_id,
                }
                | NextIdScope::LastCaches {
                    database_id,
                    table_id,
                }
                | NextIdScope::DistinctCaches {
                    database_id,
                    table_id,
                } => {
                    db_ids.contains(&DbId::new(database_id))
                        || table_ids.contains(&TableId::new(table_id))
                }
                NextIdScope::Nodes
                | NextIdScope::Databases
                | NextIdScope::Tokens
                | NextIdScope::Roles
                | NextIdScope::QueryGroups => false,
            }),
            // Ideally, `RecordId` should be an enum so that we can be truly exhaustive here. But
            // alas. Maybe I'll change that soon
            _ => Ok(false),
        }
    })
}

#[cfg(test)]
mod tests;
