use super::*;

#[tokio::test]
async fn write_accumulator_rejects_internal_writes() {
    let accumulator = WriteAccumulator::default();

    let err = accumulator
        .write_lp(
            WriteTarget::Internal,
            "cpu x=1",
            Time::from_timestamp_nanos(1),
            false,
        )
        .await
        .expect_err("internal writes should be rejected");

    assert!(matches!(err, WriteError::InternalWriteUnsupported));

    let err = accumulator
        .write_lp(
            WriteTarget::User(DatabaseName::new(INTERNAL_DB_NAME).unwrap()),
            "cpu x=1",
            Time::from_timestamp_nanos(1),
            false,
        )
        .await
        .expect_err("user-targeted _internal writes should be rejected");

    assert!(matches!(err, WriteError::InternalWriteUnsupported));
    assert!(accumulator.flush().is_empty());
}
