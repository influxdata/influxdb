use data_types::{NamespaceId, TableId};
use dml::DmlWrite;
use generated_types::influxdata::pbdata::v1::DatabaseBatch;
use mutable_batch_lp::lines_to_batches;
use wal::{SegmentWal, SequencedWalOp, WalOp};

#[tokio::test]
async fn crud() {
    let dir = test_helpers::tmp_dir().unwrap();

    let wal = wal::Wal::new(dir.path()).await.unwrap();

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

    // Can write an entry to the open segment
    let op = arbitrary_sequenced_wal_op();
    let summary = open.write_op(&op).await.unwrap();
    assert_eq!(summary.total_bytes, 373);
}

fn arbitrary_sequenced_wal_op() -> SequencedWalOp {
    let w = test_data("m1,t=foo v=1i 1");
    SequencedWalOp {
        sequence_number: 42,
        op: WalOp::Write(w),
    }
}

fn test_data(lp: &str) -> DatabaseBatch {
    let batches = lines_to_batches(lp, 0).unwrap();
    let batches = batches
        .into_iter()
        .enumerate()
        .map(|(i, (_table_name, batch))| (TableId::new(i as _), batch))
        .collect();

    let write = DmlWrite::new(
        NamespaceId::new(42),
        batches,
        "bananas".into(),
        Default::default(),
    );

    mutable_batch_pb::encode::encode_write(42, &write)
}
