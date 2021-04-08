use wal::{WalBuilder, WritePayload};

#[test]
fn no_concurrency() {
    let dir = test_helpers::tmp_dir().unwrap();
    let builder = WalBuilder::new(dir.as_ref());
    let mut wal = builder.clone().wal().unwrap();

    let data = Vec::from("somedata");
    let payload = WritePayload::new(data).unwrap();
    let sequence_number = wal.append(payload).unwrap();
    wal.sync_all().unwrap();

    assert_eq!(0, sequence_number);

    let wal_entries: Result<Vec<_>, _> = builder.entries().unwrap().collect();
    let wal_entries = wal_entries.unwrap();
    assert_eq!(1, wal_entries.len());
    assert_eq!(b"somedata".as_ref(), wal_entries[0].as_data());
    assert_eq!(0, wal_entries[0].sequence_number());
}
