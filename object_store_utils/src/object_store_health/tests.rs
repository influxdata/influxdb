use super::*;

fn boxed_err(msg: &'static str) -> Box<dyn std::error::Error + Send + Sync + 'static> {
    Box::<dyn std::error::Error + Send + Sync>::from(msg)
}

#[test]
fn new_state_is_not_ok() {
    let health = ObjectStoreHealth::new();
    assert!(!health.is_ok());
    assert_eq!(health.last_success_at(), None);
    assert_eq!(health.last_error_at(), None);
    assert_eq!(health.last_error_category(), None);
}

#[test]
fn success_then_nothing_is_ok() {
    let health = ObjectStoreHealth::new();
    health.record_success(Time::from_timestamp_nanos(1_000));
    assert!(health.is_ok());
    assert_eq!(
        health.last_success_at(),
        Some(Time::from_timestamp_nanos(1_000))
    );
    assert_eq!(health.last_error_at(), None);
}

#[test]
fn success_then_error_is_not_ok() {
    let health = ObjectStoreHealth::new();
    health.record_success(Time::from_timestamp_nanos(1_000));
    health.record_error(
        Time::from_timestamp_nanos(2_000),
        ErrorCategory::AuthFailure,
    );
    assert!(!health.is_ok());
    assert_eq!(
        health.last_error_category(),
        Some(ErrorCategory::AuthFailure)
    );
}

#[test]
fn error_then_success_is_ok() {
    let health = ObjectStoreHealth::new();
    health.record_error(Time::from_timestamp_nanos(1_000), ErrorCategory::NotFound);
    health.record_success(Time::from_timestamp_nanos(2_000));
    assert!(health.is_ok());
    assert_eq!(health.last_error_category(), Some(ErrorCategory::NotFound));
}

#[test]
fn categorize_maps_object_store_errors() {
    use object_store::Error;

    let permission_denied = Error::PermissionDenied {
        path: "p".to_string(),
        source: boxed_err("denied"),
    };
    assert_eq!(
        ErrorCategory::categorize(&permission_denied),
        ErrorCategory::AuthFailure
    );

    let unauthenticated = Error::Unauthenticated {
        path: "p".to_string(),
        source: boxed_err("no creds"),
    };
    assert_eq!(
        ErrorCategory::categorize(&unauthenticated),
        ErrorCategory::AuthFailure
    );

    let not_found = Error::NotFound {
        path: "p".to_string(),
        source: boxed_err("missing"),
    };
    assert_eq!(
        ErrorCategory::categorize(&not_found),
        ErrorCategory::NotFound
    );

    let not_supported = Error::NotSupported {
        source: boxed_err("nope"),
    };
    assert_eq!(
        ErrorCategory::categorize(&not_supported),
        ErrorCategory::ConfigError
    );

    let generic = Error::Generic {
        store: "s3",
        source: boxed_err("boom"),
    };
    assert_eq!(ErrorCategory::categorize(&generic), ErrorCategory::Unknown);
}

// The error state is packed into a single AtomicU64 (category in the top
// byte, 56-bit nanos in the low bits). The tests below exercise that packing
// layout explicitly -- a regression in the masking/shifting would show up
// here rather than deeper in the integration tests.

#[test]
fn record_error_roundtrips_realistic_timestamp() {
    // Exercise the 56-bit truncation with a real wall-clock timestamp. Any
    // time after ~1972 has bit 56 or higher set, so this reliably catches a
    // regression where the mask was widened/narrowed incorrectly or the
    // shift was off-by-one.
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time >= UNIX_EPOCH")
        .as_nanos() as i64;
    assert!(
        (nanos as u64 & !0x00FF_FFFF_FFFF_FFFF) != 0,
        "test timestamp must exercise the high bits cleared by the mask"
    );

    let health = ObjectStoreHealth::new();
    health.record_error(Time::from_timestamp_nanos(nanos), ErrorCategory::Timeout);

    // The stored timestamp is the low 56 bits of the original; the high 8
    // bits are lost to the mask. Both accessors should observe the same
    // masked value and the category is not corrupted by the masking.
    let expected_nanos = (nanos as u64 & 0x00FF_FFFF_FFFF_FFFF) as i64;
    assert_eq!(
        health.last_error_at(),
        Some(Time::from_timestamp_nanos(expected_nanos))
    );
    assert_eq!(health.last_error_category(), Some(ErrorCategory::Timeout));
}

#[test]
fn record_error_overwrites_both_fields() {
    // Verify that a second record_error updates BOTH the timestamp and the
    // category together. If the pack/unpack logic were buggy, a reader could
    // see the new timestamp with the old category (or vice versa).
    let health = ObjectStoreHealth::new();
    health.record_error(Time::from_timestamp_nanos(1_000), ErrorCategory::NotFound);
    assert_eq!(
        health.last_error_at(),
        Some(Time::from_timestamp_nanos(1_000))
    );
    assert_eq!(health.last_error_category(), Some(ErrorCategory::NotFound));

    health.record_error(
        Time::from_timestamp_nanos(5_000),
        ErrorCategory::ConfigError,
    );
    assert_eq!(
        health.last_error_at(),
        Some(Time::from_timestamp_nanos(5_000))
    );
    assert_eq!(
        health.last_error_category(),
        Some(ErrorCategory::ConfigError)
    );
}

#[test]
fn every_category_roundtrips_through_packing() {
    // Each discriminant must round-trip correctly; a typo in the reverse
    // match in last_error_category would break silently for one variant.
    let cases = [
        ErrorCategory::AuthFailure,
        ErrorCategory::NotFound,
        ErrorCategory::Timeout,
        ErrorCategory::ConfigError,
        ErrorCategory::Unknown,
    ];
    for category in cases {
        let health = ObjectStoreHealth::new();
        health.record_error(Time::from_timestamp_nanos(42), category);
        assert_eq!(
            health.last_error_category(),
            Some(category),
            "category {:?} did not round-trip",
            category
        );
        assert_eq!(health.last_error_at(), Some(Time::from_timestamp_nanos(42)));
    }
}

#[test]
fn record_error_at_epoch_zero_is_readable() {
    // Edge case: nanos == 0 packs to (category << 56) | 0. The packed value
    // is non-zero as long as category != 0, so the "packed == 0 means unset"
    // sentinel still works. Recording an error at epoch 0 with any real
    // category must remain observable as "an error exists".
    let health = ObjectStoreHealth::new();
    health.record_error(Time::from_timestamp_nanos(0), ErrorCategory::AuthFailure);

    assert_eq!(health.last_error_at(), Some(Time::from_timestamp_nanos(0)));
    assert_eq!(
        health.last_error_category(),
        Some(ErrorCategory::AuthFailure)
    );
}
