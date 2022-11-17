use wal::SegmentWal;

#[tokio::test]
async fn crud() {
    let dir = test_helpers::tmp_dir().unwrap();

    let wal = wal::Wal::new(&dir).await.unwrap();

    // Just-created WALs have no closed segments.
    assert!(
        wal.closed_segments().is_empty(),
        "Expected empty closed segments; got {:?}",
        wal.closed_segments()
    );

    // Creating a WAL creates a file in the directory for the open segment.
    let open = wal.open_segment().await;
    let open_segment_id = open.id();
    let files: Vec<_> = dir
        .path()
        .read_dir()
        .unwrap()
        .flatten()
        .map(|dir_entry| dir_entry.path())
        .collect();
    assert_eq!(files.len(), 1);
    assert_eq!(
        files[0].file_name().unwrap().to_str().unwrap(),
        &format!("{open_segment_id}.dat")
    );
}
