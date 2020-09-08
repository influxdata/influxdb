use delorean_wal::{WalBuilder, WritePayload};

type TestError = Box<dyn std::error::Error + Send + Sync + 'static>;
type Result<T = (), E = TestError> = std::result::Result<T, E>;

#[test]
fn no_concurrency() -> Result {
    let dir = delorean_test_helpers::tmp_dir()?;
    let builder = WalBuilder::new(dir.as_ref());
    let mut wal = builder.clone().wal()?;

    let data = Vec::from("somedata");
    let payload = WritePayload::new(data)?;
    let sequence_number = wal.append(payload)?;
    wal.sync_all()?;

    assert_eq!(0, sequence_number);

    let wal_entries: Result<Vec<_>, _> = builder.entries()?.collect();
    let wal_entries = wal_entries?;
    assert_eq!(1, wal_entries.len());
    assert_eq!(b"somedata".as_ref(), wal_entries[0].as_data());
    assert_eq!(0, wal_entries[0].sequence_number());

    Ok(())
}
