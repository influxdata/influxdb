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

    // Bare numbers are bytes
    assert_ok("0", 0);
    assert_ok("100", 100);
    assert_ok("1048576", 1048576);
    assert_ok(" 42 ", 42);

    // Bare-number overflow is an error, not a wrap
    assert_err("99999999999999999999999999", "number too large");

    // Other error cases
    assert_err("-1mb", "invalid digit found in string");
    assert_err("foo", "invalid digit found in string");
    assert_err("-1%", "invalid digit found in string");
    assert_err(
        "101%",
        "relative memory size must be in [0, 100] but is 101",
    );
}

#[test]
fn test_parse_memory_size_mb() {
    // Bare numbers are rejected with a transitional error: they used to
    // mean megabytes and will mean bytes in a future release.
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
    assert!(err.contains("requires a unit suffix"));
}

#[test]
fn test_byte_size_parsing() {
    #[track_caller]
    fn assert_bytes_ok(s: &str, expected: usize) {
        let parsed: ByteSize = s.parse().unwrap();
        assert_eq!(parsed.as_num_bytes(), expected, "parsing '{}'", s);
    }
    // Bare numbers are bytes
    assert_bytes_ok("0", 0);
    assert_bytes_ok("2048", 2048);
    assert_bytes_ok(" 42 ", 42);
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
