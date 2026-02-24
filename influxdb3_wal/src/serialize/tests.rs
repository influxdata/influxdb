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
