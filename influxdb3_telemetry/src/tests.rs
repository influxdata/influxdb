use super::*;

#[test]
fn test_roundtrip_display_and_as_u64() {
    let test_cases = vec![
        (ServeInvocationMethod::Explicit, "explicit", 0),
        (ServeInvocationMethod::QuickStart, "quick-start", 1),
        (ServeInvocationMethod::InstallScript, "install-script", 2),
        (ServeInvocationMethod::DockerHub, "docker-hub", 3),
        (ServeInvocationMethod::DockerOther, "docker-other", 4),
        (ServeInvocationMethod::Tests, "tests", 0x1F643),
        (ServeInvocationMethod::Unknown, "unknown", 0xFFFFFFFF),
        (ServeInvocationMethod::Custom(42), "42", 42),
    ];

    for (variant, expected_display, expected_u64) in test_cases {
        assert_eq!(variant.to_string(), expected_display);
        assert_eq!(variant.as_u64(), expected_u64);

        let display_str = variant.to_string();
        let parsed_from_display = ServeInvocationMethod::parse(&display_str).unwrap();
        assert_eq!(
            std::mem::discriminant(&variant),
            std::mem::discriminant(&parsed_from_display),
            "Display->parse roundtrip failed for {:?}: {} -> {:?}",
            variant,
            display_str,
            parsed_from_display
        );

        let u64_value = variant.as_u64();
        let u64_str = u64_value.to_string();
        let parsed_from_u64 = ServeInvocationMethod::parse(&u64_str).unwrap();
        assert_eq!(
            std::mem::discriminant(&variant),
            std::mem::discriminant(&parsed_from_u64),
            "as_u64->parse roundtrip failed for {:?}: {} -> {:?}",
            variant,
            u64_value,
            parsed_from_u64
        );
    }
}
