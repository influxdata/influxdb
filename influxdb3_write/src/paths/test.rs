use super::*;
use rstest::rstest;

#[test]
fn validate_table_index_snapshot_path_regex() {
    struct Case {
        path: ObjPath,
        expected: TableIndexId,
    }

    let path_fn = |host_id: &str, d: u32, t: u32, seq: u64| -> ObjPath {
        format!("{host_id}/table-snapshots/{d}/{t}/{seq:020}.info.json").into()
    };

    let cases = vec![
        Case {
            path: path_fn("host-id", 0, 0, 0),
            expected: TableIndexId::new("host-id", 0.into(), 0.into()),
        },
        Case {
            path: path_fn("host-id", u32::MAX, u32::MAX, u64::MAX),
            expected: TableIndexId::new("host-id", u32::MAX.into(), u32::MAX.into()),
        },
    ];

    for case in cases {
        let actual = TableIndexSnapshotPath::try_from(case.path.clone())
            .expect("always succeed constructing from valid path");
        let actual = actual.full_table_id();
        assert_eq!(case.expected, actual, "failed to parse {}", case.path);
    }
}

#[test]
fn validate_table_index_snapshot_sequence_number_extraction() {
    // Round-trip validation tests
    let test_cases = vec![
        (123u64, "host-prefix"),
        (42u64, "prefix"),
        (0u64, "test-host"),
        (u64::MAX, "another-host"),
    ];

    for (seq_num, host_prefix) in test_cases {
        let snapshot_seq = SnapshotSequenceNumber::new(seq_num);
        let db_id = 10;
        let table_id = 20;

        // Construct a new TableIndexSnapshotPath
        let path = TableIndexSnapshotPath::new(host_prefix, db_id, table_id, snapshot_seq);

        // Extract the inner object store path and convert to string
        let path_str = path.as_ref().to_string();

        // Validate the full path structure
        let expected_path =
            format!("{host_prefix}/table-snapshots/{db_id}/{table_id}/{seq_num:020}.info.json",);
        assert_eq!(
            path_str, expected_path,
            "Path structure is incorrect for sequence number {seq_num}",
        );

        // Parse the sequence number from the filename
        let parsed = TableIndexSnapshotPath::parse_sequence_number(&path_str);

        // Verify round-trip
        assert_eq!(
            parsed,
            Some(snapshot_seq),
            "Round-trip failed for sequence number {seq_num} with path {path_str}",
        );
    }

    // Direct parsing tests for valid cases
    assert_eq!(
        TableIndexSnapshotPath::parse_sequence_number("0000000123.info.json"),
        Some(SnapshotSequenceNumber::new(123))
    );
    assert_eq!(
        TableIndexSnapshotPath::parse_sequence_number("0000000000.info.json"),
        Some(SnapshotSequenceNumber::new(0))
    );
    assert_eq!(
        TableIndexSnapshotPath::parse_sequence_number("9999999999.info.json"),
        Some(SnapshotSequenceNumber::new(9999999999))
    );

    // Invalid cases - missing .info.json suffix
    assert_eq!(
        TableIndexSnapshotPath::parse_sequence_number("0000000123"),
        None
    );
    assert_eq!(
        TableIndexSnapshotPath::parse_sequence_number("0000000123.json"),
        None
    );
    assert_eq!(
        TableIndexSnapshotPath::parse_sequence_number("0000000123.info"),
        None
    );

    // Invalid cases - non-numeric prefix
    assert_eq!(
        TableIndexSnapshotPath::parse_sequence_number("abc123.info.json"),
        None
    );
    assert_eq!(
        TableIndexSnapshotPath::parse_sequence_number("not_a_number.info.json"),
        None
    );

    // Edge case - empty string
    assert_eq!(TableIndexSnapshotPath::parse_sequence_number(""), None);
    assert_eq!(
        TableIndexSnapshotPath::parse_sequence_number(".info.json"),
        None
    );
}

#[rstest]
#[case::standard_path("host-prefix/snapshots/12345678901234567890.info.json")]
#[case::nested_prefix("another/host/prefix/snapshots/00000000000000000000.info.json")]
#[case::max_inverted_value("simple/snapshots/18446744073709551615.info.json")] // u64::MAX - 0
fn test_snapshot_info_file_path_valid_paths(#[case] path: &str) {
    use object_store::path::Path as ObjPath;

    assert!(SnapshotInfoFilePath::from_path(ObjPath::from(path)).is_ok());
}

#[rstest]
#[case::invalid_format("invalid/path", PathError::InvalidFormat { expected: String::new(), actual: String::new() })]
#[case::wrong_directory(
    "prefix/wrong-dir/12345678901234567890.info.json",
    PathError::InvalidFormat { expected: String::new(), actual: String::new() }
)]
#[case::wrong_directory_singular(
    "host-prefix/snapshot/12345678901234567890.info.json",
    PathError::InvalidFormat { expected: String::new(), actual: String::new() }
)]
#[case::wrong_directory_name(
    "host-prefix/other-dir/12345678901234567890.info.json",
    PathError::InvalidFormat { expected: String::new(), actual: String::new() }
)]
#[case::too_few_digits(
    "prefix/snapshots/123.info.json",
    PathError::InvalidFormat { expected: String::new(), actual: String::new() }
)]
#[case::wrong_extension_json_only(
    "prefix/snapshots/12345678901234567890.json",
    PathError::InvalidFormat { expected: String::new(), actual: String::new() }
)]
#[case::wrong_extension_info_only(
    "host-prefix/snapshots/12345678901234567890.info",
    PathError::InvalidFormat { expected: String::new(), actual: String::new() }
)]
#[case::not_a_number(
    "host-prefix/snapshots/not-a-number.info.json",
    PathError::InvalidFormat { expected: String::new(), actual: String::new() }
)]
#[case::nineteen_digits(
    "host-prefix/snapshots/1234567890123456789.info.json",
    PathError::InvalidFormat { expected: String::new(), actual: String::new() }
)]
#[case::twenty_one_digits(
    "host-prefix/snapshots/123456789012345678901.info.json",
    PathError::InvalidFormat { expected: String::new(), actual: String::new() }
)]
#[case::missing_prefix(
    "snapshots/12345678901234567890.info.json",
    PathError::InvalidFormat { expected: String::new(), actual: String::new() }
)]
#[case::no_directory(
    "12345678901234567890.info.json",
    PathError::InvalidFormat { expected: String::new(), actual: String::new() }
)]
fn test_snapshot_info_file_path_errors(#[case] path: &str, #[case] expected_error: PathError) {
    use object_store::path::Path as ObjPath;
    use std::mem::discriminant;

    let result = SnapshotInfoFilePath::from_path(ObjPath::from(path));
    assert!(result.is_err());

    let err = result.unwrap_err();
    assert_eq!(
        discriminant(&err),
        discriminant(&expected_error),
        "Expected error type {expected_error:?}, got {err:?}",
    );
}

#[rstest]
#[case::standard_path("host-prefix/table-snapshots/123/456/12345678901234567890.info.json")]
#[case::nested_prefix_with_zeros(
    "another/prefix/table-snapshots/0/0/00000000000000000000.info.json"
)]
#[case::max_u32_values(
    "simple/table-snapshots/4294967295/4294967295/12345678901234567890.info.json"
)]
fn test_table_index_snapshot_path_valid_paths(#[case] path: &str) {
    use object_store::path::Path as ObjPath;

    assert!(TableIndexSnapshotPath::from_path(ObjPath::from(path)).is_ok());
}

#[rstest]
#[case::invalid_format("invalid/path", PathError::InvalidFormat { expected: String::new(), actual: String::new() })]
#[case::missing_directory(
    "prefix/wrong-dir/123/456/12345678901234567890.info.json",
    PathError::InvalidFormat { expected: String::new(), actual: String::new() }
)]
#[case::wrong_directory_singular(
    "host-prefix/table-snapshot/123/456/12345678901234567890.info.json",
    PathError::InvalidFormat { expected: String::new(), actual: String::new() }
)]
#[case::wrong_directory_name(
    "host-prefix/snapshots/123/456/12345678901234567890.info.json",
    PathError::InvalidFormat { expected: String::new(), actual: String::new() }
)]
#[case::invalid_extension(
    "prefix/table-snapshots/123/456/12345678901234567890.json",
    PathError::InvalidFormat { expected: String::new(), actual: String::new() }
)]
#[case::wrong_extension_info_only(
    "prefix/table-snapshots/123/456/12345678901234567890.info",
    PathError::InvalidFormat { expected: String::new(), actual: String::new() }
)]
#[case::too_few_digits_in_sequence(
    "prefix/table-snapshots/123/456/123.info.json",
    PathError::InvalidFormat { expected: String::new(), actual: String::new() }
)]
#[case::not_a_number_filename(
    "host-prefix/table-snapshots/123/456/not-a-number.info.json",
    PathError::InvalidFormat { expected: String::new(), actual: String::new() }
)]
#[case::invalid_db_id_not_number(
    "host-prefix/table-snapshots/not-a-number/456/12345678901234567890.info.json",
    PathError::InvalidFormat { expected: String::new(), actual: String::new() }
)]
#[case::invalid_table_id_not_number(
    "host-prefix/table-snapshots/123/not-a-number/12345678901234567890.info.json",
    PathError::InvalidFormat { expected: String::new(), actual: String::new() }
)]
#[case::table_id_too_large_for_u32(
    "host-prefix/table-snapshots/123/4294967296/12345678901234567890.info.json",
    PathError::InvalidTableId(String::new())
)]
#[case::db_id_too_large_for_u32(
    "host-prefix/table-snapshots/99999999999999999999/456/12345678901234567890.info.json",
    PathError::InvalidDbId(String::new())
)]
#[case::sequence_number_too_large_for_u32(
    "host-prefix/table-snapshots/123/456/99999999999999999999.info.json",
    PathError::InvalidSequenceNumber{context: String::new(), filename: String::new()}
)]
#[case::missing_table_id(
    "host-prefix/table-snapshots/123/12345678901234567890.info.json",
    PathError::InvalidFormat { expected: String::new(), actual: String::new() }
)]
#[case::missing_prefix(
    "table-snapshots/123/456/12345678901234567890.info.json",
    PathError::InvalidFormat { expected: String::new(), actual: String::new() }
)]
#[case::extra_path_components(
    "host-prefix/table-snapshots/123/456/extra/12345678901234567890.info.json",
    PathError::InvalidFormat { expected: String::new(), actual: String::new() }
)]
fn test_table_index_snapshot_path_errors(#[case] path: &str, #[case] expected_error: PathError) {
    use object_store::path::Path as ObjPath;
    use std::mem::discriminant;

    let result = TableIndexSnapshotPath::from_path(ObjPath::from(path));
    assert!(result.is_err());

    let err = result.unwrap_err();
    assert_eq!(
        discriminant(&err),
        discriminant(&expected_error),
        "Expected error type {expected_error:?}, got {err:?}",
    );
}

#[rstest]
#[case::zero_ids(0, 0)]
#[case::regular_ids(123, 456)]
#[case::max_ids(u32::MAX, u32::MAX)]
#[case::mixed_ids(0, u32::MAX)]
#[case::another_mixed(u32::MAX, 0)]
fn test_table_index_snapshot_path_extract_ids(#[case] db_id: u32, #[case] table_id: u32) {
    use object_store::path::Path as ObjPath;

    // Create a path with known IDs
    let path = TableIndexSnapshotPath::new(
        "test-prefix",
        db_id,
        table_id,
        SnapshotSequenceNumber::new(12345),
    );

    // Extract IDs and verify they match
    let id = path.full_table_id();
    assert_eq!(id.node_id(), "test-prefix");
    assert_eq!(id.db_id(), DbId::from(db_id));
    assert_eq!(id.table_id(), TableId::from(table_id));

    // Also test with a path created via from_path
    let path_str =
        format!("test-prefix/table-snapshots/{db_id}/{table_id}/12345678901234567890.info.json");
    let parsed_path =
        TableIndexSnapshotPath::from_path(ObjPath::from(path_str)).expect("path should be valid");

    let parsed_id = parsed_path.full_table_id();
    assert_eq!(parsed_id.node_id(), "test-prefix");
    assert_eq!(parsed_id.db_id(), DbId::from(db_id));
    assert_eq!(parsed_id.table_id(), TableId::from(table_id));
}

#[rstest]
#[case::missing_db_indices(
    "invalid/path",
    PathError::InvalidFormat { expected: String::new(), actual: String::new() }
)]
#[case::invalid_db_id(
    "prefix/db-indices/not-a-number/456/index.info.json",
    PathError::InvalidFormat { expected: String::new(), actual: String::new() }
)]
#[case::invalid_table_id(
    "prefix/db-indices/123/not-a-number/index.info.json",
    PathError::InvalidFormat { expected: String::new(), actual: String::new() }
)]
#[case::table_id_overflow_u32(
    "prefix/db-indices/123/4294967296/index.info.json",
    PathError::InvalidTableId(String::new())
)]
#[case::db_id_overflow_u32(
    "prefix/db-indices/4294967296/123/index.info.json",
    PathError::InvalidDbId(String::new())
)]
#[case::wrong_filename(
    "prefix/db-indices/123/456/wrong-file.json",
    PathError::InvalidFormat { expected: String::new(), actual: String::new() }
)]
fn test_table_index_path_errors(#[case] path: &str, #[case] expected_error: PathError) {
    use object_store::path::Path as ObjPath;
    use std::mem::discriminant;

    let result = TableIndexPath::from_path(ObjPath::from(path));
    assert!(result.is_err());

    let err = result.unwrap_err();
    assert_eq!(
        discriminant(&err),
        discriminant(&expected_error),
        "Expected error type {expected_error:?}, got {err:?}",
    );
}

#[test]
fn snapshot_checkpoint_path_new() {
    let jan_2025 = YearMonth::new_unchecked(2025, 1);
    assert_eq!(
        *SnapshotCheckpointPath::new("my_host", &jan_2025, SnapshotSequenceNumber::new(0)),
        ObjPath::from("my_host/snapshot-checkpoints/2025-01/18446744073709551615.checkpoint.json")
    );
}

#[test]
fn snapshot_checkpoint_path_parse_year_month() {
    assert_eq!(
        SnapshotCheckpointPath::parse_year_month(
            "my_host/snapshot-checkpoints/2025-01/18446744073709551615.checkpoint.json"
        ),
        Some(YearMonth::new_unchecked(2025, 1))
    );
    assert_eq!(
        SnapshotCheckpointPath::parse_year_month(
            "my_host/snapshot-checkpoints/2024-12/00000000000000000000.checkpoint.json"
        ),
        Some(YearMonth::new_unchecked(2024, 12))
    );
    assert_eq!(
        SnapshotCheckpointPath::parse_year_month("invalid/path"),
        None
    );
}

#[test]
fn snapshot_checkpoint_path_parse_sequence_number() {
    // Sequence 0 inverts to u64::MAX
    assert_eq!(
        SnapshotCheckpointPath::parse_sequence_number("18446744073709551615.checkpoint.json"),
        Some(SnapshotSequenceNumber::new(0))
    );
    // Round-trip test
    let seq = SnapshotSequenceNumber::new(42);
    let jan_2025 = YearMonth::new_unchecked(2025, 1);
    let path = SnapshotCheckpointPath::new("host", &jan_2025, seq);
    assert_eq!(
        SnapshotCheckpointPath::parse_sequence_number(path.as_ref().as_ref()),
        Some(seq)
    );
}

#[rstest]
#[case::standard_path(
    "host-prefix/snapshot-checkpoints/2025-01/12345678901234567890.checkpoint.json"
)]
#[case::nested_prefix(
    "another/host/prefix/snapshot-checkpoints/2024-12/00000000000000000000.checkpoint.json"
)]
#[case::max_inverted_value(
    "simple/snapshot-checkpoints/2025-06/18446744073709551615.checkpoint.json"
)]
fn test_snapshot_checkpoint_path_valid_paths(#[case] path: &str) {
    assert!(SnapshotCheckpointPath::from_path(ObjPath::from(path)).is_ok());
}

#[rstest]
#[case::invalid_format("invalid/path", PathError::InvalidFormat { expected: String::new(), actual: String::new() })]
#[case::wrong_directory(
    "prefix/wrong-dir/2025-01/12345678901234567890.checkpoint.json",
    PathError::InvalidFormat { expected: String::new(), actual: String::new() }
)]
#[case::missing_month(
    "host-prefix/snapshot-checkpoints/12345678901234567890.checkpoint.json",
    PathError::InvalidFormat { expected: String::new(), actual: String::new() }
)]
#[case::invalid_month_format(
    "host-prefix/snapshot-checkpoints/2025-1/12345678901234567890.checkpoint.json",
    PathError::InvalidFormat { expected: String::new(), actual: String::new() }
)]
#[case::wrong_extension(
    "prefix/snapshot-checkpoints/2025-01/12345678901234567890.json",
    PathError::InvalidFormat { expected: String::new(), actual: String::new() }
)]
#[case::too_few_digits(
    "prefix/snapshot-checkpoints/2025-01/123.checkpoint.json",
    PathError::InvalidFormat { expected: String::new(), actual: String::new() }
)]
fn test_snapshot_checkpoint_path_errors(#[case] path: &str, #[case] expected_error: PathError) {
    use std::mem::discriminant;

    let result = SnapshotCheckpointPath::from_path(ObjPath::from(path));
    assert!(result.is_err());

    let err = result.unwrap_err();
    assert_eq!(
        discriminant(&err),
        discriminant(&expected_error),
        "Expected error type {expected_error:?}, got {err:?}",
    );
}
