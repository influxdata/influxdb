use std::num::{NonZeroU32, NonZeroUsize};
use std::time::{Duration, Instant};

use arrow_util::assert_batches_eq;
use data_types::database_rules::{
    DatabaseRules, LifecycleRules, PartitionTemplate, TemplatePart, WriteBufferConnection,
    WriteBufferDirection,
};
use data_types::{sequence::Sequence, server_id::ServerId, DatabaseName};
use entry::{test_helpers::lp_to_entry, SequencedEntry};
use query::QueryDatabase;
use server::{
    db::test_helpers::run_query,
    rules::ProvidedDatabaseRules,
    test_utils::{make_application, make_initialized_server},
};
use test_helpers::{assert_contains, tracing::TracingCapture};
use time::Time;
use write_buffer::mock::MockBufferSharedState;

#[tokio::test]
async fn write_buffer_reads_wait_for_compaction() {
    let tracing_capture = TracingCapture::new();

    // ==================== setup ====================
    let server_id = ServerId::new(NonZeroU32::new(1).unwrap());
    let db_name = DatabaseName::new("delete_predicate_preservation_test").unwrap();

    // Test that delete predicates are stored within the preserved catalog

    // setup write buffer
    // these numbers are handtuned to trigger hard buffer limits w/o making the test too big
    let n_entries = 50u64;
    let write_buffer_state =
        MockBufferSharedState::empty_with_n_sequencers(NonZeroU32::new(1).unwrap());

    for sequence_number in 0..n_entries {
        let lp = format!(
            "table_1,tag_partition_by=a foo=\"hello\",bar=1 {}",
            sequence_number / 2
        );
        write_buffer_state.push_entry(SequencedEntry::new_from_sequence(
            Sequence::new(0, sequence_number),
            Time::from_timestamp_nanos(0),
            lp_to_entry(&lp),
        ));
    }

    write_buffer_state.push_entry(SequencedEntry::new_from_sequence(
        Sequence::new(0, n_entries),
        Time::from_timestamp_nanos(0),
        lp_to_entry("table_2,partition_by=a foo=1 0"),
    ));

    let application = make_application();
    application
        .write_buffer_factory()
        .register_mock("my_mock".to_string(), write_buffer_state);

    let server = make_initialized_server(server_id, application).await;

    // create DB
    let rules = DatabaseRules {
        partition_template: PartitionTemplate {
            parts: vec![TemplatePart::Column("tag_partition_by".to_string())],
        },
        lifecycle_rules: LifecycleRules {
            buffer_size_hard: Some(NonZeroUsize::new(10_000).unwrap()),
            mub_row_threshold: NonZeroUsize::new(10).unwrap(),
            ..Default::default()
        },
        write_buffer_connection: Some(WriteBufferConnection {
            direction: WriteBufferDirection::Read,
            type_: "mock".to_string(),
            connection: "my_mock".to_string(),
            ..Default::default()
        }),
        ..DatabaseRules::new(db_name.clone())
    };

    let database = server
        .create_database(ProvidedDatabaseRules::new_rules(rules.clone().into()).unwrap())
        .await
        .unwrap();

    let db = database.initialized_db().unwrap();

    // after a while the table should exist
    let t_0 = Instant::now();
    loop {
        if db.table_schema("table_2").is_some() {
            break;
        }

        assert!(t_0.elapsed() < Duration::from_secs(10));
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // no rows should be dropped
    let batches = run_query(db, "select sum(bar) as n from table_1").await;
    let expected = vec!["+----+", "| n  |", "+----+", "| 25 |", "+----+"];
    assert_batches_eq!(expected, &batches);

    // check that hard buffer limit was actually hit (otherwise this test is pointless/outdated)
    assert_contains!(
        tracing_capture.to_string(),
        "Hard limit reached while reading from write buffer, waiting for compaction to catch up"
    );

    server.shutdown();
    server.join().await.unwrap();
}
