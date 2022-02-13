use arrow_util::assert_batches_eq;
use data_types::{
    chunk_metadata::ChunkStorage,
    database_rules::{DatabaseRules, LifecycleRules, PartitionTemplate, TemplatePart},
    sequence::Sequence,
    server_id::ServerId,
    write_buffer::WriteBufferConnection,
    DatabaseName,
};
use db::{
    test_helpers::{run_query, wait_for_tables},
    Db,
};
use futures_util::FutureExt;
use query::QueryDatabase;
use server::{
    rules::ProvidedDatabaseRules,
    test_utils::{make_application, make_initialized_server},
};
use std::{
    num::{NonZeroU32, NonZeroUsize},
    sync::Arc,
    time::{Duration, Instant},
};
use test_helpers::{assert_contains, tracing::TracingCapture};
use write_buffer::mock::MockBufferSharedState;

#[tokio::test]
async fn write_buffer_lifecycle() {
    // Test the interaction between the write buffer and the lifecycle

    let tracing_capture = TracingCapture::new();

    // ==================== setup ====================
    let server_id = ServerId::new(NonZeroU32::new(1).unwrap());
    let db_name = DatabaseName::new("delete_predicate_preservation_test").unwrap();

    let application = make_application();

    let mock_shared_state =
        MockBufferSharedState::empty_with_n_sequencers(NonZeroU32::new(1).unwrap());

    // The writes are split into two groups to allow "pausing replay" by playing back from
    // a MockBufferSharedState with only the first set of writes
    write_group1(&mock_shared_state);
    write_group2(&mock_shared_state);

    application
        .write_buffer_factory()
        .register_mock("my_mock".to_string(), mock_shared_state.clone());

    let server = make_initialized_server(server_id, Arc::clone(&application)).await;

    let partition_template = PartitionTemplate {
        parts: vec![TemplatePart::Column("tag_partition_by".to_string())],
    };

    let write_buffer_connection = WriteBufferConnection {
        type_: "mock".to_string(),
        connection: "my_mock".to_string(),
        ..Default::default()
    };

    //
    // Phase 1: Verify that consuming from a write buffer will wait for compaction in the event
    // the hard limit is exceeded
    //

    // create DB
    let rules = DatabaseRules {
        partition_template: partition_template.clone(),
        lifecycle_rules: LifecycleRules {
            buffer_size_hard: Some(NonZeroUsize::new(10_000).unwrap()),
            mub_row_threshold: NonZeroUsize::new(10).unwrap(),
            ..Default::default()
        },
        write_buffer_connection: Some(write_buffer_connection.clone()),
        ..DatabaseRules::new(db_name.clone())
    };

    let database = server
        .create_database(ProvidedDatabaseRules::new_rules(rules.into()).unwrap())
        .await
        .unwrap();

    let db = database.initialized_db().unwrap();

    // after a while the table should exist
    wait_for_tables(&db, &["table_1", "table_2"]).await;

    // no rows should be dropped
    let batches = run_query(Arc::clone(&db), "select sum(bar) as n from table_1").await;
    let expected = vec!["+----+", "| n  |", "+----+", "| 25 |", "+----+"];
    assert_batches_eq!(expected, &batches);

    // check that hard buffer limit was actually hit (otherwise this test is pointless/outdated)
    assert_contains!(
        tracing_capture.to_string(),
        "Hard limit reached while reading from write buffer, waiting for compaction to catch up"
    );

    // Persist the final write, this will ensure that we have to replay the data for table_1
    db.persist_partition("table_2", "tag_partition_by_a", true)
        .await
        .unwrap();

    // Only table_2 should be persisted
    assert_eq!(count_persisted_chunks(&db), 1);

    // Shutdown server
    server.shutdown();
    server.join().await.unwrap();

    // Drop so they don't contribute to metrics
    std::mem::drop(server);
    std::mem::drop(database);
    std::mem::drop(db);
    std::mem::drop(tracing_capture);

    //
    // Phase 2: Verify that replaying from a write buffer will wait for compaction in the event
    // the hard limit is exceeded
    //

    // Recreate server
    let tracing_capture = TracingCapture::new();

    let server = make_initialized_server(server_id, Arc::clone(&application)).await;
    let databases = server.databases().unwrap();
    assert_eq!(databases.len(), 1);

    let database = databases.into_iter().next().unwrap();
    database.wait_for_init().await.unwrap();
    let database_uuid = database.uuid();

    let db = database.initialized_db().unwrap();
    let batches = run_query(Arc::clone(&db), "select sum(bar) as n from table_1").await;
    let expected = vec!["+----+", "| n  |", "+----+", "| 25 |", "+----+"];
    assert_batches_eq!(expected, &batches);

    assert_contains!(
        tracing_capture.to_string(),
        "Hard limit reached while replaying, waiting for compaction to catch up"
    );

    // Only table_2 should be persisted
    assert_eq!(count_persisted_chunks(&db), 1);

    server.shutdown();
    server.join().await.unwrap();

    //
    // Phase 3: Verify that persistence is disabled during replay
    //

    // Override rules to set persist row threshold lower on restart
    let rules = ProvidedDatabaseRules::new_rules(
        DatabaseRules {
            partition_template,
            lifecycle_rules: LifecycleRules {
                persist: true,
                late_arrive_window_seconds: NonZeroU32::new(1).unwrap(),
                persist_row_threshold: NonZeroUsize::new(5).unwrap(),
                ..Default::default()
            },
            write_buffer_connection: Some(write_buffer_connection),
            ..DatabaseRules::new(db_name.clone())
        }
        .into(),
    )
    .unwrap();

    application
        .config_provider()
        .store_rules(database_uuid, &rules)
        .await
        .unwrap();

    std::mem::drop(server);
    std::mem::drop(database);
    std::mem::drop(db);
    std::mem::drop(tracing_capture);

    // Clear the write buffer and only write in the first group of writes
    mock_shared_state.clear_messages(0);
    write_group1(&mock_shared_state);

    // Restart server - this will load new rules written above
    let server = make_initialized_server(server_id, Arc::clone(&application)).await;
    let databases = server.databases().unwrap();
    assert_eq!(databases.len(), 1);
    let database = databases.into_iter().next().unwrap();

    // Sleep for a bit to allow the lifecycle policy to run a bit
    //
    // During this time replay should still be running as there is insufficient
    // writes within the write buffer to "complete" replay.
    //
    // However, there are sufficient rows to exceed the persist row threshold.
    //
    // Therefore, if persist were not disabled by replay, the lifecycle would try
    // to persist without the full set of writes. This would in turn result
    // in two separate chunks being persisted for table_1
    tokio::time::sleep(Duration::from_secs(1)).await;

    assert!(
        database.wait_for_init().now_or_never().is_none(),
        "replay shouldn't have finished as insufficient data"
    );

    // Write in remainder of data to allow replay to finish
    write_group2(&mock_shared_state);

    database.wait_for_init().await.unwrap();
    let db = database.initialized_db().unwrap();

    let start = Instant::now();
    loop {
        if count_persisted_chunks(&db) > 1 {
            // As soon as replay finishes the lifecycle should have persisted everything in
            // table_1 into a single chunk. We should therefore have two chunks, one for
            // each of table_1 and table_2
            assert_eq!(db.chunk_summaries().len(), 2, "persisted during replay!");
            break;
        }

        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "failed to persist chunk"
        )
    }

    server.shutdown();
    server.join().await.unwrap();
}

/// The first set of writes for the write buffer
fn write_group1(write_buffer_state: &MockBufferSharedState) {
    // setup write buffer
    // these numbers are handtuned to trigger hard buffer limits w/o making the test too big
    let n_entries = 50u64;

    for sequence_number in 0..n_entries {
        let lp = format!(
            "table_1,tag_partition_by=a foo=\"hello\",bar=1 {}",
            sequence_number / 2
        );
        write_buffer_state.push_lp(Sequence::new(0, sequence_number), &lp);
    }
}

/// The second set of writes for the write buffer
fn write_group2(write_buffer_state: &MockBufferSharedState) {
    // Write line with timestamp 0 - this forces persistence to persist all
    // prior writes if the server has read this line
    write_buffer_state.push_lp(
        Sequence::new(0, 100),
        "table_1,tag_partition_by=a foo=\"hello\",bar=1 0",
    );

    write_buffer_state.push_lp(Sequence::new(0, 101), "table_2,tag_partition_by=a foo=1 0");
}

fn count_persisted_chunks(db: &Db) -> usize {
    db.chunk_summaries()
        .into_iter()
        .filter(|x| {
            matches!(
                x.storage,
                ChunkStorage::ObjectStoreOnly | ChunkStorage::ReadBufferAndObjectStore
            )
        })
        .count()
}
