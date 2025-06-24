//! Module for serializing and deserializing the contents of a single WAL file. Since the WAL is
//! buffered in memory before writing it in a single PUT operation to object store, this works
//! a little differently than a traditional WAL that appends.

use crate::WalContents;
use byteorder::{BigEndian, ReadBytesExt};
use bytes::Bytes;
use std::io::Cursor;
use std::mem::size_of;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Invalid wal file identifier")]
    InvalidWalFile,

    #[error("WAL file too small: expected at least {expected} bytes, but got {actual} bytes")]
    WalFileTooSmall { expected: usize, actual: usize },

    #[error("crc32 checksum mismatch")]
    Crc32Mismatch,

    #[error("bitcode error: {0}")]
    Bitcode(#[from] bitcode::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("try from slice error {0}")]
    TryFromSlice(#[from] std::array::TryFromSliceError),
}

pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;

/// The first bytes written into a wal file to identify it and its version.
const FILE_TYPE_IDENTIFIER: &[u8] = b"idb3.001";

#[inline(always)]
pub fn verify_file_type_and_deserialize(b: Bytes) -> Result<WalContents> {
    let contents = b.to_vec();

    let pos = FILE_TYPE_IDENTIFIER.len();
    const CHECKSUM_LEN: usize = size_of::<u32>();
    let min_file_size = pos + CHECKSUM_LEN;

    // Check if file has minimum required bytes
    if contents.len() < min_file_size {
        return Err(Error::WalFileTooSmall {
            expected: min_file_size,
            actual: contents.len(),
        });
    }

    // Read and verify the file type identifier
    let file_type = &contents[..pos];

    if file_type != FILE_TYPE_IDENTIFIER {
        return Err(Error::InvalidWalFile);
    }

    // Read the crc32 checksum
    let checksum_slice = &contents[pos..pos + CHECKSUM_LEN]; // Ensure this slice covers the 4 bytes for the checksum
    let mut cursor = Cursor::new(checksum_slice);
    let crc32_checksum = cursor.read_u32::<BigEndian>().unwrap();

    // Validate the data against the checksum
    let data = &contents[pos + CHECKSUM_LEN..];

    let mut hasher = crc32fast::Hasher::new();
    hasher.update(data);
    let checksum = hasher.finalize();
    if checksum != crc32_checksum {
        return Err(Error::Crc32Mismatch);
    }

    // Deserialize the data into a WalContents
    let contents: WalContents = bitcode::deserialize(data)?;

    Ok(contents)
}

pub(crate) fn serialize_to_file_bytes(contents: &WalContents) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    buf.extend_from_slice(FILE_TYPE_IDENTIFIER);

    // serialize the contents into bitcode bytes
    let data = bitcode::serialize(contents)?;

    // calculate the crc32 checksum
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&data);
    let checksum = hasher.finalize();

    // write the checksum and data to the buffer
    buf.extend_from_slice(&checksum.to_be_bytes());
    buf.extend_from_slice(&data);

    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Field, FieldData, Row, TableChunk, TableChunks, WalFileSequenceNumber, WalOp, WriteBatch,
    };
    use influxdb3_id::{ColumnId, DbId, SerdeVecMap, TableId};

    #[test]
    fn test_serialize_deserialize() {
        let chunk = TableChunk {
            rows: vec![Row {
                time: 1,
                fields: vec![
                    Field {
                        id: ColumnId::from(0),
                        value: FieldData::Integer(10),
                    },
                    Field {
                        id: ColumnId::from(1),
                        value: FieldData::Timestamp(1),
                    },
                ],
            }],
        };
        let chunks = TableChunks {
            min_time: 0,
            max_time: 10,
            chunk_time_to_chunk: [(1, chunk)].iter().cloned().collect(),
        };
        let table_id = TableId::from(2);
        let mut table_chunks = SerdeVecMap::new();
        table_chunks.insert(table_id, chunks);

        let contents = WalContents {
            persist_timestamp_ms: 10,
            min_timestamp_ns: 0,
            max_timestamp_ns: 10,
            wal_file_number: WalFileSequenceNumber::new(1),
            ops: vec![WalOp::Write(WriteBatch {
                catalog_sequence: 0,
                database_id: DbId::from(0),
                database_name: "foo".into(),
                table_chunks,
                min_time_ns: 0,
                max_time_ns: 10,
            })],
            snapshot: None,
        };

        let bytes = serialize_to_file_bytes(&contents).unwrap();
        let deserialized = verify_file_type_and_deserialize(Bytes::from(bytes)).unwrap();

        assert_eq!(contents, deserialized);
    }

    #[test]
    fn test_empty_wal_file() {
        let empty_bytes = Bytes::new();
        let result = verify_file_type_and_deserialize(empty_bytes);

        match result {
            Err(Error::WalFileTooSmall {
                expected: 12,
                actual: 0,
            }) => {
                // Expected error
            }
            _ => panic!("Expected WalFileTooSmall error for empty file"),
        }
    }

    #[test]
    fn test_truncated_wal_file() {
        // File with only 5 bytes (less than minimum required)
        let truncated_bytes = Bytes::from(vec![b'i', b'd', b'b', b'3', b'.']);
        let result = verify_file_type_and_deserialize(truncated_bytes);

        match result {
            Err(Error::WalFileTooSmall {
                expected: 12,
                actual: 5,
            }) => {
                // Expected error
            }
            _ => panic!("Expected WalFileTooSmall error for truncated file"),
        }
    }

    #[test]
    fn test_wal_file_with_header_but_no_checksum() {
        // File with complete header but no checksum
        let header_only = Bytes::from(FILE_TYPE_IDENTIFIER);
        let result = verify_file_type_and_deserialize(header_only);

        match result {
            Err(Error::WalFileTooSmall {
                expected: 12,
                actual: 8,
            }) => {
                // Expected error
            }
            _ => panic!("Expected WalFileTooSmall error for file with only header"),
        }
    }
}
