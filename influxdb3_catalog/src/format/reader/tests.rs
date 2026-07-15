use bytes::Bytes;

use super::*;
use crate::format::{FORMAT_VERSION, RecordFlags, file_flags};

fn create_test_file(records: &[Record]) -> Bytes {
    // Build payload
    let mut payload = Vec::new();
    for record in records {
        payload.extend_from_slice(&record.to_bytes());
    }

    // Compute CRC
    let payload_crc = crc32fast::hash(&payload);

    // Build header
    let header = Header {
        format_version: FORMAT_VERSION,
        flags: 0,
        catalog_uuid: 1337,
        sequence_number: 12345,
        record_count: records.len() as u32,
        group_count: 0,
        payload_crc,
        payload_len: payload.len() as u64,
    };

    // Combine header and payload
    let mut file = Vec::new();
    file.extend_from_slice(&header.to_bytes());
    file.extend_from_slice(&payload);

    Bytes::from(file)
}

#[test]
fn parse_empty_file() {
    let file_bytes = create_test_file(&[]);
    let mut cursor = Cursor::new(file_bytes);
    let file = CatalogFile::read_from(&mut cursor).unwrap();

    assert_eq!(file.record_count(), 0);
    assert_eq!(file.sequence_number(), 12345);
}

fn log_records(file: &CatalogFile) -> &[Record] {
    assert!(
        !file.header.is_snapshot(),
        "expected Log content, got Snapshot"
    );
    &file.records
}

#[test]
fn parse_single_record() {
    let data = Bytes::from_static(b"test payload");
    let record = Record::new(1, RecordFlags::none(), 0, data.clone());
    let file_bytes = create_test_file(&[record]);
    let mut cursor = Cursor::new(file_bytes);

    let file = CatalogFile::read_from(&mut cursor).unwrap();
    let records = log_records(&file);

    assert_eq!(file.record_count(), 1);
    assert_eq!(records[0].id().raw(), 1);
    assert_eq!(records[0].data, data);
}

#[test]
fn parse_multiple_records() {
    let records_in = [
        Record::new(1, RecordFlags::none(), 0, Bytes::from_static(b"first")),
        Record::new(2, RecordFlags::none(), 0, Bytes::from_static(b"second")),
        Record::new(3, RecordFlags::none(), 0, Bytes::from_static(b"third")),
    ];
    let file_bytes = create_test_file(&records_in);
    let mut cursor = Cursor::new(file_bytes);

    let file = CatalogFile::read_from(&mut cursor).unwrap();
    let records = log_records(&file);

    assert_eq!(file.record_count(), 3);
    assert_eq!(records[0].id().raw(), 1);
    assert_eq!(records[1].id().raw(), 2);
    assert_eq!(records[2].id().raw(), 3);
}

#[test]
fn buffer_too_short_for_header() {
    let bytes = Bytes::from_static(&[0u8; 10]);
    let mut cursor = Cursor::new(bytes);
    let result = CatalogFile::read_from(&mut cursor);

    assert!(matches!(
        result,
        Err(FormatError::BufferTooShort {
            expected: Header::SIZE,
            actual: 10
        })
    ));
}

#[test]
fn buffer_too_short_for_payload() {
    let data = Bytes::from_static(b"test");
    let record = Record::new(1, RecordFlags::none(), 0, data);
    let mut file_bytes = create_test_file(&[record]).to_vec();

    // Truncate the file to remove part of the payload
    file_bytes.truncate(Header::SIZE + 5);
    let bytes = Bytes::from(file_bytes);
    let mut cursor = Cursor::new(bytes);

    let result = CatalogFile::read_from(&mut cursor);
    assert!(matches!(result, Err(FormatError::BufferTooShort { .. })));
}

#[test]
fn crc_mismatch() {
    let data = Bytes::from_static(b"test payload");
    let record = Record::new(1, RecordFlags::none(), 0, data);
    let mut file_bytes = create_test_file(&[record]).to_vec();

    // Corrupt the payload
    file_bytes[Header::SIZE + 5] ^= 0xFF;
    let bytes = Bytes::from(file_bytes);
    let mut cursor = Cursor::new(bytes);

    let result = CatalogFile::read_from(&mut cursor);
    assert!(matches!(result, Err(FormatError::Crc32Mismatch { .. })));
}

#[test]
fn read_verified_header_returns_header_for_valid_file() {
    let data = Bytes::from_static(b"test payload");
    let record = Record::new(1, RecordFlags::none(), 0, data);
    let file_bytes = create_test_file(&[record]);
    let mut cursor = Cursor::new(file_bytes);

    let header = CatalogFile::read_verified_header(&mut cursor).unwrap();
    assert_eq!(header.sequence_number, 12345);
}

#[test]
fn read_verified_header_rejects_corrupt_payload_crc() {
    let data = Bytes::from_static(b"test payload");
    let record = Record::new(1, RecordFlags::none(), 0, data);
    let mut file_bytes = create_test_file(&[record]).to_vec();

    // Flip a payload byte: the header CRC still passes, but the payload CRC
    // must not, so the header-only read must reject it just like read_from.
    file_bytes[Header::SIZE + 5] ^= 0xFF;
    let mut cursor = Cursor::new(Bytes::from(file_bytes));

    let result = CatalogFile::read_verified_header(&mut cursor);
    assert!(matches!(result, Err(FormatError::Crc32Mismatch { .. })));
}

#[test]
fn read_verified_header_rejects_truncated_payload() {
    let data = Bytes::from_static(b"test payload");
    let record = Record::new(1, RecordFlags::none(), 0, data);
    let mut file_bytes = create_test_file(&[record]).to_vec();

    // Drop the final payload byte while the header still declares the full
    // payload_len, so the bounds check must fail.
    file_bytes.pop();
    let mut cursor = Cursor::new(Bytes::from(file_bytes));

    let result = CatalogFile::read_verified_header(&mut cursor);
    assert!(matches!(result, Err(FormatError::BufferTooShort { .. })));
}

#[test]
fn invalid_magic() {
    let mut file_bytes = create_test_file(&[]).to_vec();

    // Corrupt the magic bytes
    file_bytes[0] = b'X';
    let bytes = Bytes::from(file_bytes);
    let mut cursor = Cursor::new(bytes);

    let result = CatalogFile::read_from(&mut cursor);
    assert!(matches!(result, Err(FormatError::InvalidMagic { .. })));
}

#[test]
fn truncated_record_header() {
    // Create a file with one record, then corrupt the record count to claim more
    let data = Bytes::from_static(b"test");
    let record = Record::new(1, RecordFlags::none(), 0, data);

    // Build payload
    let mut payload = Vec::new();
    payload.extend_from_slice(&record.to_bytes());
    let payload_crc = crc32fast::hash(&payload);

    // Build header claiming 2 records when we only have 1
    let header = Header {
        format_version: FORMAT_VERSION,
        flags: 0,
        catalog_uuid: 1337,
        sequence_number: 12345,
        record_count: 2, // Claim 2 records
        group_count: 0,
        payload_crc,
        payload_len: payload.len() as u64,
    };

    let mut file = Vec::new();
    file.extend_from_slice(&header.to_bytes());
    file.extend_from_slice(&payload);
    let bytes = Bytes::from(file);
    let mut cursor = Cursor::new(bytes);

    let result = CatalogFile::read_from(&mut cursor);
    assert!(matches!(result, Err(FormatError::BufferTooShort { .. })));
}

/// Build a raw catalog file from header fields plus a payload. The caller
/// owns validity — this helper exists so corruption tests can construct
/// malformed files on purpose.
fn build_raw_file(header_flags: u16, record_count: u32, payload: Vec<u8>) -> Bytes {
    let payload_crc = crc32fast::hash(&payload);
    let header = Header {
        format_version: FORMAT_VERSION,
        flags: header_flags,
        catalog_uuid: 1337,
        sequence_number: 1,
        record_count,
        group_count: 0,
        payload_crc,
        payload_len: payload.len() as u64,
    };
    header.to_bytes().into_iter().chain(payload).collect()
}

#[test]
fn snapshot_payload_parses_like_log() {
    // A SNAPSHOT-flagged file has the same payload shape as a log file:
    // records back-to-back, no index.
    let records = [
        Record::new(1, RecordFlags::none(), 1, Bytes::from_static(b"first")),
        Record::new(2, RecordFlags::none(), 2, Bytes::from_static(b"second")),
    ];
    let mut payload = Vec::new();
    for record in &records {
        payload.extend_from_slice(&record.to_bytes());
    }
    let bytes = build_raw_file(file_flags::SNAPSHOT, 2, payload);
    let mut cursor = Cursor::new(bytes);
    let file = CatalogFile::read_from(&mut cursor).unwrap();

    assert!(
        file.header.is_snapshot(),
        "expected Snapshot content, got Log"
    );
    let parsed = &file.records;
    assert_eq!(parsed.len(), 2);
    assert_eq!(parsed[0].id().raw(), 1);
    assert_eq!(parsed[1].id().raw(), 2);
}

#[test]
fn legacy_grouped_snapshot_skips_index_and_reads_records() {
    // Regression for influxdb_pro#4026: snapshots written by the pre-#4026
    // engine prefix the record payload with a group index of `group_count`
    // 24-byte entries. The new flat reader must skip that index and return the
    // records (already in application order) rather than rejecting the file.
    let records = [
        Record::new(1, RecordFlags::none(), 1, Bytes::from_static(b"alpha")),
        Record::new(2, RecordFlags::none(), 2, Bytes::from_static(b"bravo")),
    ];
    let group_count = 2u32; // e.g. Global + one database
    // Group-index bytes — opaque to the new reader, which only skips them.
    let mut payload = vec![0u8; group_count as usize * 24];
    for record in &records {
        payload.extend_from_slice(&record.to_bytes());
    }
    let payload_crc = crc32fast::hash(&payload);
    let header = Header {
        format_version: FORMAT_VERSION,
        flags: file_flags::SNAPSHOT,
        catalog_uuid: 1337,
        sequence_number: 7,
        record_count: records.len() as u32,
        group_count,
        payload_crc,
        payload_len: payload.len() as u64,
    };
    let mut file = Vec::new();
    file.extend_from_slice(&header.to_bytes());
    file.extend_from_slice(&payload);
    let mut cursor = Cursor::new(Bytes::from(file));

    let parsed = CatalogFile::read_from(&mut cursor).unwrap();
    assert!(parsed.header.is_snapshot());
    assert_eq!(parsed.records.len(), 2);
    assert_eq!(parsed.records[0].id().raw(), 1);
    assert_eq!(parsed.records[1].id().raw(), 2);
    assert_eq!(&parsed.records[0].data[..], b"alpha");
    assert_eq!(&parsed.records[1].data[..], b"bravo");
}

#[test]
fn snapshot_rejects_truncated_payload() {
    let record = Record::new(1, RecordFlags::none(), 1, Bytes::from_static(b"data"));
    let bytes = build_raw_file(file_flags::SNAPSHOT, 1, record.to_bytes());
    let mut truncated = bytes.to_vec();
    truncated.truncate(Header::SIZE + 5);
    let mut cursor = Cursor::new(truncated);
    assert!(matches!(
        CatalogFile::read_from(&mut cursor),
        Err(FormatError::BufferTooShort { .. }),
    ));
}

#[test]
fn snapshot_rejects_corrupt_payload_crc() {
    let record = Record::new(1, RecordFlags::none(), 1, Bytes::from_static(b"data"));
    let bytes = build_raw_file(file_flags::SNAPSHOT, 1, record.to_bytes());
    let mut corrupted = bytes.to_vec();
    corrupted[Header::SIZE + RECORD_HEADER_SIZE + 1] ^= 0xFF;
    let mut cursor = Cursor::new(corrupted);
    assert!(matches!(
        CatalogFile::read_from(&mut cursor),
        Err(FormatError::Crc32Mismatch { .. }),
    ));
}

#[test]
fn snapshot_rejects_record_count_exceeding_payload() {
    // header.record_count claims more records than the payload holds.
    let record = Record::new(1, RecordFlags::none(), 1, Bytes::from_static(b"data"));
    let bytes = build_raw_file(file_flags::SNAPSHOT, 2, record.to_bytes());
    let mut cursor = Cursor::new(bytes);
    assert!(matches!(
        CatalogFile::read_from(&mut cursor),
        Err(FormatError::BufferTooShort { .. }),
    ));
}

#[cfg(test)]
mod skip_crc_tests {
    use super::*;
    use crate::format::MakeRecord;
    use crate::format::apply::serialize_log_file;
    use crate::format::records::CreateDatabase;
    use crate::format::records::types::RetentionPeriod;
    use std::io::Cursor;
    use uuid::Uuid;

    fn corrupt_payload(mut bytes: Vec<u8>) -> Vec<u8> {
        // Flip a byte within the record DATA region so the record framing
        // (file header + 16-byte record header) stays intact — only the CRC
        // changes. The record data starts at Header::SIZE + RECORD_HEADER_SIZE.
        let i = Header::SIZE + RECORD_HEADER_SIZE + 1;
        bytes[i] ^= 0xFF;
        bytes
    }

    #[test]
    fn strict_read_rejects_bad_payload_crc_but_lenient_accepts() {
        let record = CreateDatabase {
            database_id: 1,
            database_name: "x".to_string(),
            retention_period: RetentionPeriod::Indefinite,
        }
        .make_record(1);
        let good = serialize_log_file(Uuid::nil(), 1, std::slice::from_ref(&record));
        let bad = corrupt_payload(good.to_vec());

        let strict = CatalogFile::read_from(&mut Cursor::new(bad.as_slice()));
        assert!(matches!(strict, Err(FormatError::Crc32Mismatch { .. })));

        let lenient = CatalogFile::read_from_lenient(&mut Cursor::new(bad.as_slice()))
            .expect("lenient read should succeed");
        assert_eq!(lenient.record_count(), 1);
    }
}
