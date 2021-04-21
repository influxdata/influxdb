use write_buffer::{WriteBufferBuilder, WritePayload};

#[test]
fn no_concurrency() {
    let dir = test_helpers::tmp_dir().unwrap();
    let builder = WriteBufferBuilder::new(dir.as_ref());
    let mut write_buffer = builder.clone().write_buffer().unwrap();

    let data = Vec::from("somedata");
    let payload = WritePayload::new(data).unwrap();
    let sequence_number = write_buffer.append(payload).unwrap();
    write_buffer.sync_all().unwrap();

    assert_eq!(0, sequence_number);

    let write_buffer_entries: Result<Vec<_>, _> = builder.entries().unwrap().collect();
    let write_buffer_entries = write_buffer_entries.unwrap();
    assert_eq!(1, write_buffer_entries.len());
    assert_eq!(b"somedata".as_ref(), write_buffer_entries[0].as_data());
    assert_eq!(0, write_buffer_entries[0].sequence_number());
}
