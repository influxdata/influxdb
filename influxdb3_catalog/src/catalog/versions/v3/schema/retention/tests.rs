use super::*;

#[test]
fn test_format_v1() {
    let cases = [
        (RetentionPeriod::Indefinite, "0s"),
        (RetentionPeriod::Duration(Duration::from_secs(0)), "0s"),
        (RetentionPeriod::Duration(Duration::from_secs(90)), "1m30s"),
        (
            RetentionPeriod::Duration(Duration::from_secs(3_600)),
            "1h0m0s",
        ),
        (
            RetentionPeriod::Duration(Duration::from_secs(7 * 24 * 3_600)),
            "168h0m0s",
        ),
    ];

    for (rp, expected) in cases {
        assert_eq!(rp.format_v1(), expected);
    }
}
