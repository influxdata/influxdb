use super::*;

#[test]
fn test_parse_memory_size() {
    assert_ok("0", 0);
    assert_ok("1", 1);
    assert_ok("1024", 1024);
    assert_ok("0%", 0);

    assert_gt_zero("50%");

    assert_err("-1", "invalid digit found in string");
    assert_err("foo", "invalid digit found in string");
    assert_err("-1%", "invalid digit found in string");
    assert_err(
        "101%",
        "relative memory size must be in [0, 100] but is 101",
    );
}

#[test]
fn test_parse_memory_size_mb() {
    // Plain number = megabytes
    assert_mb_ok("0", 0);
    assert_mb_ok("1", 1024 * 1024);
    assert_mb_ok("5", 5 * 1024 * 1024);
    assert_mb_ok("100", 100 * 1024 * 1024);

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
}

#[track_caller]
fn assert_ok(s: &'static str, expected: usize) {
    let parsed: MemorySize = s.parse().unwrap();
    assert_eq!(parsed.bytes(), expected);
}

#[track_caller]
fn assert_gt_zero(s: &'static str) {
    let parsed: MemorySize = s.parse().unwrap();
    assert!(parsed.bytes() > 0);
}

#[track_caller]
fn assert_err(s: &'static str, expected: &'static str) {
    let err = MemorySize::from_str(s).unwrap_err();
    assert_eq!(err, expected);
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
