use super::*;

const TEST_TS_SECONDS: i64 = 1_761_048_000;
const TEST_TS_NANOS: i64 = 1_761_048_000_123_456_789;

#[test]
fn test_precision_serialization_and_parsing() {
    let deserialization_expectations = [
        ("auto", Precision::Auto),
        ("s", Precision::Second),
        ("second", Precision::Second),
        ("ms", Precision::Millisecond),
        ("millisecond", Precision::Millisecond),
        ("us", Precision::Microsecond),
        ("u", Precision::Microsecond),
        ("microsecond", Precision::Microsecond),
        ("ns", Precision::Nanosecond),
        ("n", Precision::Nanosecond),
        ("nanosecond", Precision::Nanosecond),
    ];

    for (input, expected) in deserialization_expectations {
        let from_str_result = input.parse::<Precision>();
        assert!(
            from_str_result.is_ok(),
            "from_str failed for input: {}",
            input
        );
        let from_str_value = from_str_result.unwrap();
        assert_eq!(
            from_str_value, expected,
            "from_str produced wrong result for input: {}",
            input
        );

        let serde_result = serde_json::from_str::<Precision>(&format!("\"{}\"", input));
        assert!(
            serde_result.is_ok(),
            "serde deserialization failed for input: {}",
            input
        );
        let serde_value = serde_result.unwrap();
        assert_eq!(
            serde_value, expected,
            "serde produced wrong result for input: {}",
            input
        );

        // Verify from_str and deserialization match
        assert_eq!(
            from_str_value, serde_value,
            "from_str and serde mismatch for input: {}",
            input
        );
    }

    let serialization_tests = [
        (Precision::Auto, "auto"),
        (Precision::Second, "second"),
        (Precision::Millisecond, "millisecond"),
        (Precision::Microsecond, "microsecond"),
        (Precision::Nanosecond, "nanosecond"),
    ];

    for (precision, expected_output) in serialization_tests {
        let serialized = serde_json::to_string(&precision).unwrap();
        assert_eq!(
            serialized,
            format!("\"{}\"", expected_output),
            "Serialization failed for {:?}",
            precision
        );

        let deserialized = serde_json::from_str::<Precision>(&serialized).unwrap();
        assert_eq!(
            deserialized, precision,
            "Round-trip failed for {:?}",
            precision
        );
    }

    let invalid_inputs = ["invalid", "sec", "milliseconds", "micro", "nano"];
    for input in invalid_inputs {
        let from_str_result = input.parse::<Precision>();
        let serde_result = serde_json::from_str::<Precision>(&format!("\"{}\"", input));

        assert!(
            from_str_result.is_err(),
            "from_str should reject invalid input: {}",
            input
        );
        assert!(
            serde_result.is_err(),
            "serde should reject invalid input: {}",
            input
        );
    }
}

#[test]
fn test_guess_precision() {
    assert_eq!(Precision::guess_precision(0), Precision::Second);
    assert_eq!(Precision::guess_precision(1), Precision::Second);
    assert_eq!(Precision::guess_precision(4_999_999_999), Precision::Second);
    assert_eq!(
        Precision::guess_precision(5_000_000_000),
        Precision::Millisecond
    );
    assert_eq!(
        Precision::guess_precision(TEST_TS_SECONDS),
        Precision::Second
    );
    assert_eq!(
        Precision::guess_precision(TEST_TS_SECONDS * 1_000),
        Precision::Millisecond
    );
    assert_eq!(
        Precision::guess_precision(4_999_999_999_999),
        Precision::Millisecond
    );
    assert_eq!(
        Precision::guess_precision(5_000_000_000_000),
        Precision::Microsecond
    );
    assert_eq!(
        Precision::guess_precision(TEST_TS_SECONDS * 1_000_000),
        Precision::Microsecond
    );
    assert_eq!(
        Precision::guess_precision(4_999_999_999_999_999),
        Precision::Microsecond
    );
    assert_eq!(
        Precision::guess_precision(5_000_000_000_000_000),
        Precision::Nanosecond
    );
    assert_eq!(
        Precision::guess_precision(TEST_TS_SECONDS * 1_000_000_000),
        Precision::Nanosecond
    );

    assert_eq!(Precision::guess_precision(-1), Precision::Second);
    assert_eq!(
        Precision::guess_precision(-4_999_999_999),
        Precision::Second
    );
    assert_eq!(
        Precision::guess_precision(-5_000_000_000),
        Precision::Millisecond
    );
    assert_eq!(
        Precision::guess_precision(-TEST_TS_SECONDS * 1_000),
        Precision::Millisecond
    );
    assert_eq!(
        Precision::guess_precision(-TEST_TS_SECONDS * 1_000_000),
        Precision::Microsecond
    );
    assert_eq!(
        Precision::guess_precision(-TEST_TS_SECONDS * 1_000_000_000),
        Precision::Nanosecond
    );
}

#[test]
fn test_multiplier_all_precisions() {
    assert_eq!(Precision::Second.multiplier(), 1_000_000_000);
    assert_eq!(Precision::Millisecond.multiplier(), 1_000_000);
    assert_eq!(Precision::Microsecond.multiplier(), 1_000);
    assert_eq!(Precision::Nanosecond.multiplier(), 1);
}

#[test]
fn test_truncate_to_precision() {
    assert_eq!(
        Precision::Second.truncate_to_precision(1_500_000_000),
        1_000_000_000
    );
    assert_eq!(
        Precision::Second.truncate_to_precision(-1_500_000_000),
        -1_000_000_000
    );
    assert_eq!(Precision::Second.truncate_to_precision(0), 0);
    assert_eq!(Precision::Second.truncate_to_precision(1), 0);
    assert_eq!(
        Precision::Second.truncate_to_precision(TEST_TS_NANOS),
        TEST_TS_SECONDS * 1_000_000_000
    );

    assert_eq!(
        Precision::Millisecond.truncate_to_precision(1_500_000),
        1_000_000
    );
    assert_eq!(
        Precision::Millisecond.truncate_to_precision(-1_500_000),
        -1_000_000
    );
    assert_eq!(
        Precision::Millisecond.truncate_to_precision(TEST_TS_NANOS / 1_000),
        TEST_TS_SECONDS * 1_000_000
    );

    assert_eq!(Precision::Microsecond.truncate_to_precision(1_500), 1_000);
    assert_eq!(Precision::Microsecond.truncate_to_precision(-1_500), -1_000);
    assert_eq!(
        Precision::Microsecond.truncate_to_precision(TEST_TS_NANOS / 1_000),
        (TEST_TS_NANOS / 1_000_000) * 1_000
    );

    assert_eq!(
        Precision::Nanosecond.truncate_to_precision(TEST_TS_NANOS),
        TEST_TS_NANOS
    );
    assert_eq!(
        Precision::Nanosecond.truncate_to_precision(-TEST_TS_NANOS),
        -TEST_TS_NANOS
    );

    assert_eq!(
        Precision::Auto.truncate_to_precision(TEST_TS_NANOS),
        TEST_TS_NANOS
    );
    assert_eq!(
        Precision::Auto.truncate_to_precision(-TEST_TS_NANOS),
        -TEST_TS_NANOS
    );
}

#[test]
fn test_to_nanos_explicit_with_overflow() {
    let result = Precision::Second.to_nanos(Some(i64::MAX), 0);
    assert!(result.is_err());

    let result = Precision::Millisecond.to_nanos(Some(i64::MIN), 0);
    assert!(result.is_err());

    let near_max = i64::MAX / 1_000_000_000;
    let result = Precision::Second.to_nanos(Some(near_max), 0);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), near_max * 1_000_000_000);

    let near_min = i64::MIN / 1_000_000_000;
    let result = Precision::Second.to_nanos(Some(near_min), 0);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), near_min * 1_000_000_000);
}

#[test]
fn test_to_nanos_explicit_all_precisions() {
    let expected_nanos = TEST_TS_SECONDS * 1_000_000_000;

    assert_eq!(
        Precision::Second
            .to_nanos(Some(TEST_TS_SECONDS), 0)
            .unwrap(),
        expected_nanos
    );
    assert_eq!(
        Precision::Auto.to_nanos(Some(TEST_TS_SECONDS), 0).unwrap(),
        expected_nanos
    );
}

#[test]
fn test_write_line_error_serialize_truncates_at_utf8_boundary() {
    // Serialize caps `original_line` at this many bytes (mirrors the constant in the impl).
    const TRUNCATE_LIMIT: usize = 20;

    // 19 ASCII bytes followed by a 2-byte 'é' starting at byte 19. A naive `[..TRUNCATE_LIMIT]`
    // slice lands in the middle of 'é' and would panic; floor_char_boundary backs up to byte 19.
    let long_line = format!("{}érest of the line", "a".repeat(19));
    assert!(
        !long_line.is_char_boundary(TRUNCATE_LIMIT),
        "test premise: byte {TRUNCATE_LIMIT} must fall inside a multi-byte char",
    );

    let err = WriteLineError {
        original_line: long_line,
        line_number: 1,
        error_message: "bad line".to_string(),
    };
    let json = serde_json::to_string(&err).expect("serialize must not panic");
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
    let emitted = parsed["original_line"]
        .as_str()
        .expect("original_line must serialize as a JSON string");

    assert_eq!(emitted, "a".repeat(19));

    // Short input passes through unchanged.
    let short = "short line";
    assert!(short.len() < TRUNCATE_LIMIT);
    let err = WriteLineError {
        original_line: short.to_string(),
        line_number: 2,
        error_message: "bad".to_string(),
    };
    let json = serde_json::to_string(&err).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed["original_line"].as_str().unwrap(), short);
}

#[test]
fn test_to_nanos_default_truncation() {
    assert_eq!(
        Precision::Second.to_nanos(None, TEST_TS_NANOS).unwrap(),
        TEST_TS_SECONDS * 1_000_000_000
    );
    assert_eq!(
        Precision::Millisecond
            .to_nanos(None, TEST_TS_NANOS)
            .unwrap(),
        1_761_048_000_123_000_000
    );
    assert_eq!(
        Precision::Microsecond
            .to_nanos(None, TEST_TS_NANOS)
            .unwrap(),
        1_761_048_000_123_456_000
    );
    assert_eq!(
        Precision::Nanosecond.to_nanos(None, TEST_TS_NANOS).unwrap(),
        TEST_TS_NANOS
    );

    assert_eq!(
        Precision::Auto.to_nanos(None, TEST_TS_NANOS).unwrap(),
        TEST_TS_NANOS
    );

    assert_eq!(Precision::Second.to_nanos(None, 0).unwrap(), 0);
    assert_eq!(Precision::Millisecond.to_nanos(None, 0).unwrap(), 0);
}
