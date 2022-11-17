use wal::SegmentWal;

#[tokio::test]
async fn crud() {
    let dir = test_helpers::tmp_dir().unwrap();

    let wal = wal::Wal::new(dir).await.unwrap();

    assert!(
        wal.closed_segments().is_empty(),
        "Expected empty closed segments; got {:?}",
        wal.closed_segments()
    );
}
