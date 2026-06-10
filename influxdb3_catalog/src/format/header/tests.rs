use super::*;

#[test]
fn header_round_trip() {
    let header = Header {
        format_version: 1,
        flags: 0,
        catalog_uuid: 0x0102030405060708090a0b0c0d0e0f10,
        sequence_number: 12345,
        record_count: 100,
        payload_crc: 0xDEADBEEF,
        payload_len: 50000,
    };

    let bytes = header.to_bytes();
    assert_eq!(bytes.len(), Header::SIZE);

    let mut cursor = Cursor::new(bytes);
    let parsed = Header::read_from(&mut cursor).unwrap();
    assert_eq!(parsed.format_version, 1);
    assert_eq!(parsed.flags, 0);
    assert_eq!(parsed.catalog_uuid, header.catalog_uuid);
    assert_eq!(parsed.sequence_number, 12345);
    assert_eq!(parsed.record_count, 100);
    assert_eq!(parsed.payload_crc, 0xDEADBEEF);
    assert_eq!(parsed.payload_len, 50000);
}

#[test]
fn snapshot_header_round_trip() {
    let header = Header {
        format_version: 1,
        flags: file_flags::SNAPSHOT,
        catalog_uuid: 0,
        sequence_number: 500,
        record_count: 10000,
        payload_crc: 0x12345678,
        payload_len: 1_000_000,
    };

    let bytes = header.to_bytes();
    let mut cursor = Cursor::new(bytes);
    let parsed = Header::read_from(&mut cursor).unwrap();
    assert!(parsed.is_snapshot());
    assert_eq!(parsed.record_count, 10000);
}

#[test]
fn nonzero_reserved_field_rejected() {
    // The reserved u32 at 0x2C must be zero; a nonzero value (e.g. a file
    // written by a stale pre-release format) is rejected loudly.
    let header = Header {
        format_version: 1,
        flags: file_flags::SNAPSHOT,
        catalog_uuid: 0,
        sequence_number: 1,
        record_count: 0,
        payload_crc: 0,
        payload_len: 0,
    };
    let mut bytes = header.to_bytes();
    bytes[0x2C..0x30].copy_from_slice(&1u32.to_le_bytes());
    // Recompute the header CRC so only the reserved-field check can fail.
    let crc = crc32fast::hash(&bytes[0x0C..Header::SIZE]);
    bytes[0x08..0x0C].copy_from_slice(&crc.to_le_bytes());

    let mut cursor = Cursor::new(bytes);
    let result = Header::read_from(&mut cursor);
    assert!(matches!(result, Err(FormatError::InvalidHeader { .. })));
}

#[test]
fn invalid_magic_rejected() {
    let mut bytes = [0u8; Header::SIZE];
    bytes[0..4].copy_from_slice(b"XXXX");
    let mut cursor = Cursor::new(bytes);

    let result = Header::read_from(&mut cursor);
    assert!(matches!(result, Err(FormatError::InvalidMagic { .. })));
}

#[test]
fn buffer_too_short_rejected() {
    let bytes = [0u8; 10];
    let mut cursor = Cursor::new(bytes);
    let result = Header::read_from(&mut cursor);

    assert!(matches!(
        result,
        Err(FormatError::BufferTooShort {
            expected: 64,
            actual: 10
        })
    ));
}

#[test]
fn header_crc_mismatch_rejected() {
    let header = Header {
        format_version: 1,
        flags: 0,
        catalog_uuid: 0,
        sequence_number: 1,
        record_count: 1,
        payload_crc: 0,
        payload_len: 0,
    };

    let mut bytes = header.to_bytes();
    // Corrupt a byte in the CRC-covered range
    bytes[0x20] ^= 0xFF;
    let mut cursor = Cursor::new(bytes);

    let result = Header::read_from(&mut cursor);
    assert!(matches!(
        result,
        Err(FormatError::HeaderCrc32Mismatch { .. })
    ));
}

#[test]
fn header_alignment() {
    // Verify all multi-byte fields are at 4-byte aligned offsets
    let offsets: &[(usize, &str)] = &[
        (0x00, "magic"),
        (0x04, "format_version"),
        (0x08, "header_crc"),
        (0x0C, "flags"),
        (0x10, "catalog_uuid"),
        (0x20, "sequence_number"),
        (0x28, "record_count"),
        (0x2C, "reserved"),
        (0x30, "payload_len"),
        (0x38, "payload_crc"),
        (0x3C, "reserved"),
    ];
    for &(offset, name) in offsets {
        assert_eq!(
            offset % 4,
            0,
            "{name} at 0x{offset:02X} is not 4-byte aligned"
        );
    }
}
