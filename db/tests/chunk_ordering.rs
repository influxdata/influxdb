use db::test_helpers::write_lp;
use db::utils::make_db_time;
use std::time::Duration;

#[tokio::test]
async fn test_chunk_ordering() {
    let partition_key = "1970-01-01T00";
    let (db, time) = make_db_time().await;
    let late_arrival = db.rules().lifecycle_rules.late_arrive_window_seconds.get() as u64;

    // Write data into chunk order 1
    write_lp(&db, "cpu,tag=a field=23 1");
    time.inc(Duration::from_secs(late_arrival * 2));
    write_lp(&db, "cpu,tag=b field=24 100");

    let chunks = db.filtered_chunk_summaries(None, None);
    assert_eq!(chunks.len(), 1);

    db.persist_partition("cpu", partition_key, false)
        .await
        .unwrap();

    let chunks = db.filtered_chunk_summaries(None, None);

    // One chunk should be persisted one not
    assert!(chunks.iter().any(|x| !x.storage.has_object_store()));
    assert!(chunks.iter().any(|x| x.storage.has_object_store()));

    // Write some new data that overlaps
    write_lp(&db, "cpu,tag=a field=30 1");

    db.persist_partition("cpu", partition_key, true)
        .await
        .unwrap();

    let chunks = db.filtered_chunk_summaries(None, None);

    // Everything should be persisted
    assert_eq!(chunks.len(), 2);
    assert!(chunks.iter().all(|x| x.storage.has_object_store()));

    // They must not have the same order
    assert_ne!(chunks[0].order, chunks[1].order);
}
