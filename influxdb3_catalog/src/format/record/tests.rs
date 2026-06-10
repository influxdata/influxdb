use super::*;

#[test]
fn record_header_round_trip() {
    let header = RecordHeader::new(42, RecordFlags::none(), 9999, 11);

    let bytes = header.to_bytes();
    assert_eq!(bytes.len(), RECORD_HEADER_SIZE);
    let mut cursor = Cursor::new(bytes);

    let parsed = RecordHeader::read_from(&mut cursor).unwrap();
    assert_eq!(parsed.id, 42);
    assert!(!parsed.flags.is_upgrade_safe());
    assert_eq!(parsed.sequence, 9999);
    assert_eq!(parsed.length, 11);
}

#[test]
fn record_round_trip() {
    let data = Bytes::from_static(b"test data for record");
    let record = Record::new(100, RecordFlags::default(), 42, data.clone());

    let bytes = record.to_bytes();
    let buf = Bytes::from(bytes);
    let mut cursor = Cursor::new(buf);

    let parsed = Record::read_from(&mut cursor).unwrap();
    assert_eq!(cursor.position() as usize, RECORD_HEADER_SIZE + data.len());
    assert_eq!(parsed.id(), 100);
    assert_eq!(parsed.sequence(), 42);
    assert!(!parsed.is_upgrade_safe());
    assert_eq!(parsed.data, data);
}

#[test]
fn record_header_buffer_too_short() {
    let bytes = [0u8; 5];
    let mut cursor = Cursor::new(bytes);
    let result = RecordHeader::read_from(&mut cursor);

    assert!(matches!(
        result,
        Err(FormatError::BufferTooShort {
            expected: 16,
            actual: 5
        })
    ));
}

#[test]
fn record_data_buffer_too_short() {
    let header = RecordHeader::new(1, RecordFlags::default(), 0, 100);

    let mut buf = Vec::new();
    buf.extend_from_slice(&header.to_bytes());
    buf.extend_from_slice(b"short");

    let buf = Bytes::from(buf);
    let mut cursor = Cursor::new(buf);
    let result = Record::read_from(&mut cursor);

    assert!(matches!(result, Err(FormatError::BufferTooShort { .. })));
}

#[test]
fn multiple_records_parsing() {
    let record1 = Record::new(1, RecordFlags::default(), 10, Bytes::from_static(b"first"));
    let record2 = Record::new(
        2,
        RecordFlags::upgrade_safe(),
        11,
        Bytes::from_static(b"second record"),
    );

    let mut buf = Vec::new();
    buf.extend_from_slice(&record1.to_bytes());
    buf.extend_from_slice(&record2.to_bytes());

    let buf = Bytes::from(buf);
    let mut cursor = Cursor::new(buf);

    let parsed1 = Record::read_from(&mut cursor).unwrap();
    assert_eq!(parsed1.id(), 1);
    assert_eq!(parsed1.sequence(), 10);
    assert_eq!(parsed1.data, Bytes::from_static(b"first"));

    let parsed2 = Record::read_from(&mut cursor).unwrap();
    assert_eq!(parsed2.id(), 2);
    assert_eq!(parsed2.sequence(), 11);
    assert!(parsed2.is_upgrade_safe());
    assert_eq!(parsed2.data, Bytes::from_static(b"second record"));
}

#[test]
fn record_header_byte_layout() {
    let header = RecordHeader::new(
        0x0102,
        RecordFlags::upgrade_safe(),
        0x0304050607080910,
        0x11121314,
    );

    let bytes = header.to_bytes();

    // id (little-endian)
    assert_eq!(&bytes[0x00..0x02], &[0x02, 0x01]);
    // flags (little-endian, UPGRADE_SAFE = 0x0001)
    assert_eq!(&bytes[0x02..0x04], &[0x01, 0x00]);
    // sequence (little-endian)
    assert_eq!(
        &bytes[0x04..0x0C],
        &[0x10, 0x09, 0x08, 0x07, 0x06, 0x05, 0x04, 0x03]
    );
    // length (little-endian)
    assert_eq!(&bytes[0x0C..0x10], &[0x14, 0x13, 0x12, 0x11]);
}

#[test]
fn record_batch_stamps_pushed_records_with_sequence() {
    use crate::format::records::SetGenerationDuration;

    let mut batch = RecordBatch::new(42);
    batch.push(&SetGenerationDuration {
        level: 1,
        duration_ns: 1_000_000,
    });
    batch.push(&SetGenerationDuration {
        level: 2,
        duration_ns: 2_000_000,
    });

    assert_eq!(batch.sequence(), 42);
    assert_eq!(batch.len(), 2);
    for record in batch.as_slice() {
        assert_eq!(record.sequence(), 42);
    }
}
