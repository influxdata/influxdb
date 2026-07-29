use super::*;

#[test]
fn test_parse_memory_size() {
    // With 'mb' suffix (case insensitive)
    assert_ok("5mb", 5 * 1024 * 1024);
    assert_ok("5MB", 5 * 1024 * 1024);
    assert_ok("5Mb", 5 * 1024 * 1024);
    assert_ok("100mb", 100 * 1024 * 1024);
    assert_ok("0mb", 0);

    // With 'kb' suffix
    assert_ok("1kb", 1024);
    assert_ok("512kb", 512 * 1024);
    assert_ok("1024KB", 1024 * 1024);

    // With 'gb' suffix
    assert_ok("1gb", 1024 * 1024 * 1024);
    assert_ok("2GB", 2 * 1024 * 1024 * 1024);

    // With 'tb' suffix
    assert_ok("1tb", 1024 * 1024 * 1024 * 1024);

    // Overflow is an error, not a wrap
    assert_err("99999999999tb", "overflows");

    // With 'b' suffix (raw bytes)
    assert_ok("0b", 0);
    assert_ok("1024b", 1024);
    assert_ok("1048576b", 1048576);

    // Percentage
    assert_gt_zero("50%");
    assert_ok("0%", 0);

    // With whitespace
    assert_ok(" 5 mb", 5 * 1024 * 1024);
    assert_ok("5 MB", 5 * 1024 * 1024);

    // Bare numbers are rejected: units are always explicit
    assert_err("0", "specify an explicit unit suffix");
    assert_err("100", "bare number '100' is not accepted");
    assert_err("1048576", "specify an explicit unit suffix");
    assert_err(" 42 ", "bare number '42' is not accepted");
    assert_err(
        "99999999999999999999999999",
        "expected a number with a unit suffix",
    );

    // Other error cases
    assert_err("-1mb", "invalid digit found in string");
    assert_err("foo", "expected a number with a unit suffix");
    assert_err("-1%", "invalid digit found in string");
    assert_err(
        "101%",
        "relative memory size must be in [0, 100] but is 101",
    );
}

#[test]
fn test_parse_memory_size_mb() {
    // Bare numbers are rejected: they used to mean megabytes here, and
    // sizes now always require an explicit unit.
    assert_mb_err("0", "previously meant megabytes");
    assert_mb_err("1", "previously meant megabytes");
    assert_mb_err("100", "specify an explicit unit suffix");

    // With 'mb' suffix (case insensitive)
    assert_mb_ok("5mb", 5 * 1024 * 1024);
    assert_mb_ok("5MB", 5 * 1024 * 1024);
    assert_mb_ok("5Mb", 5 * 1024 * 1024);
    assert_mb_ok("100mb", 100 * 1024 * 1024);

    // With 'kb' suffix
    assert_mb_ok("1kb", 1024);
    assert_mb_ok("512kb", 512 * 1024);
    assert_mb_ok("1024KB", 1024 * 1024);

    // With 'gb' suffix
    assert_mb_ok("1gb", 1024 * 1024 * 1024);
    assert_mb_ok("2GB", 2 * 1024 * 1024 * 1024);

    // With 'b' suffix (raw bytes)
    assert_mb_ok("1024b", 1024);
    assert_mb_ok("1048576b", 1048576);

    // Percentage
    assert_mb_gt_zero("50%");
    assert_mb_ok("0%", 0);

    // With whitespace
    assert_mb_ok(" 5 mb", 5 * 1024 * 1024);
    assert_mb_ok("5 MB", 5 * 1024 * 1024);

    // Error cases
    assert_mb_err("-1", "failed to parse");
    assert_mb_err("foo", "failed to parse");
    assert_mb_err("-1%", "invalid digit found in string");
    assert_mb_err(
        "101%",
        "relative memory size must be in [0, 100] but is 101",
    );
}

#[track_caller]
fn assert_ok(s: &'static str, expected: usize) {
    let parsed: MemorySize = s.parse().unwrap();
    assert_eq!(parsed.as_num_bytes(), expected, "parsing '{}'", s);
}

#[track_caller]
fn assert_gt_zero(s: &'static str) {
    let parsed: MemorySize = s.parse().unwrap();
    assert!(parsed.as_num_bytes() > 0);
}

#[track_caller]
fn assert_err(s: &'static str, expected_substring: &'static str) {
    let err = MemorySize::from_str(s).unwrap_err();
    assert!(
        err.contains(expected_substring),
        "error for '{}' should contain '{}', got: {}",
        s,
        expected_substring,
        err
    );
}

#[track_caller]
fn assert_mb_ok(s: &'static str, expected: usize) {
    let parsed: MemorySizeMb = s.parse().unwrap();
    assert_eq!(parsed.as_num_bytes(), expected, "parsing '{}'", s);
}

#[track_caller]
fn assert_mb_gt_zero(s: &'static str) {
    let parsed: MemorySizeMb = s.parse().unwrap();
    assert!(parsed.as_num_bytes() > 0);
}

#[track_caller]
fn assert_mb_err(s: &'static str, expected_substring: &'static str) {
    let err = MemorySizeMb::from_str(s).unwrap_err();
    assert!(
        err.contains(expected_substring),
        "error for '{}' should contain '{}', got: {}",
        s,
        expected_substring,
        err
    );
}

#[test]
fn test_byte_size_rejects_percentage() {
    let err = ByteSize::from_str("50%").unwrap_err();
    assert!(
        err.contains("percentage") && err.contains("not supported"),
        "error should say percentages are unsupported, got: {err}"
    );
}

#[test]
fn test_byte_size_parsing() {
    #[track_caller]
    fn assert_bytes_ok(s: &str, expected: usize) {
        let parsed: ByteSize = s.parse().unwrap();
        assert_eq!(parsed.as_num_bytes(), expected, "parsing '{}'", s);
    }
    // Bare numbers are rejected: units are always explicit
    for bare in ["0", "2048", " 42 "] {
        let err = ByteSize::from_str(bare).unwrap_err();
        assert!(
            err.contains("specify an explicit unit suffix"),
            "error for '{bare}' should demand a unit, got: {err}"
        );
    }
    // Explicit bytes
    assert_bytes_ok("0b", 0);
    assert_bytes_ok("2048b", 2048);
    // Unit suffixes convert
    assert_bytes_ok("1kb", 1024);
    assert_bytes_ok("5 MB", 5 * 1024 * 1024);
    // Overflow is an error, not a wrap
    assert!(ByteSize::from_str("99999999999tb").is_err());
    assert!(ByteSize::from_str("99999999999999999999999999").is_err());
    // Malformed input is an error
    assert!(ByteSize::from_str("foo").is_err());
    assert!(ByteSize::from_str("-1").is_err());
}

#[test]
fn test_should_warn_memory_reservations() {
    let total = 100;
    // Below threshold: no warn.
    assert!(!should_warn_memory_reservations(total, 89));
    // At threshold: warn.
    assert!(should_warn_memory_reservations(total, 90));
    // Above threshold but under detected total: warn.
    assert!(should_warn_memory_reservations(total, 95));
    // Over-committed (reserved > detected): warn.
    assert!(should_warn_memory_reservations(total, 110));
    // No reservations: no warn.
    assert!(!should_warn_memory_reservations(total, 0));
    // Detected unknown: no signal, no warn even if reservations are large.
    assert!(!should_warn_memory_reservations(0, 100));
}

#[test]
fn test_format_bytes() {
    // Sub-KiB values
    assert_eq!(format_bytes(0), "0B");
    assert_eq!(format_bytes(1), "1B");
    assert_eq!(format_bytes(512), "512B");
    assert_eq!(format_bytes(1023), "1023B");

    // KiB range
    assert_eq!(format_bytes(1024), "1.00KiB");
    assert_eq!(format_bytes(1536), "1.50KiB");
    assert_eq!(format_bytes(1024 * 1024 - 1), "1024.00KiB");

    // MiB range
    assert_eq!(format_bytes(1024 * 1024), "1.00MiB");
    assert_eq!(format_bytes(1024 * 1024 + 1024 * 256), "1.25MiB");
    assert_eq!(format_bytes(1024 * 1024 * 1024 - 1), "1024.00MiB");

    // GiB range
    assert_eq!(format_bytes(1024 * 1024 * 1024), "1.00GiB");
    assert_eq!(
        format_bytes(1024 * 1024 * 1024 * 2 + 1024 * 1024 * 512),
        "2.50GiB"
    );
}

#[test]
fn test_legacy_memory_size_mb_bare_number_means_megabytes() {
    // The pre-3.11 format used by hidden legacy option spellings: a bare
    // number is megabytes, matching the 3.10 MemorySizeMb parser.
    let parsed: LegacyMemorySizeMb = "500".parse().unwrap();
    assert_eq!(parsed.as_num_bytes(), 500 * 1024 * 1024);

    // Unit suffixes and percentages parse like the strict type.
    let parsed: LegacyMemorySizeMb = "2gb".parse().unwrap();
    assert_eq!(parsed.as_num_bytes(), 2 * 1024 * 1024 * 1024);
    let parsed: LegacyMemorySizeMb = "50%".parse().unwrap();
    assert!(parsed.as_num_bytes() > 0);

    let err = LegacyMemorySizeMb::from_str("banana").unwrap_err();
    assert!(err.contains("failed to parse"), "got: {err}");

    // Conversion to the strict type preserves the byte value.
    let strict: MemorySizeMb = LegacyMemorySizeMb(123).into();
    assert_eq!(strict.as_num_bytes(), 123);
}

#[test]
fn test_lenient_byte_size_bare_number_means_bytes() {
    // The pre-3.11 format of --max-http-request-size: a bare number is
    // bytes, and bare-ness is reported so serve can warn.
    let parsed: LenientByteSize = "10485760".parse().unwrap();
    assert_eq!(parsed.as_num_bytes(), 10485760);
    assert!(parsed.is_bare());

    let parsed: LenientByteSize = "10mb".parse().unwrap();
    assert_eq!(parsed.as_num_bytes(), 10 * 1024 * 1024);
    assert!(!parsed.is_bare());

    let err = LenientByteSize::from_str("50%").unwrap_err();
    assert!(
        err.contains("percentage") && err.contains("not supported"),
        "got: {err}"
    );
    let err = LenientByteSize::from_str("banana").unwrap_err();
    assert!(err.contains("failed to parse"), "got: {err}");
}
