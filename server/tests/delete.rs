use futures::TryStreamExt;
use std::{
    num::{NonZeroU32, NonZeroU64},
    sync::Arc,
    time::{Duration, Instant},
};

use arrow_util::assert_batches_sorted_eq;
use data_types::{
    chunk_metadata::ChunkStorage,
    database_rules::{DatabaseRules, LifecycleRules, PartitionTemplate, TemplatePart},
    server_id::ServerId,
    timestamp::TimestampRange,
    DatabaseName,
};
use iox_object_store::IoxObjectStore;
use predicate::{delete_expr::DeleteExpr, delete_predicate::DeletePredicate};
use query::{QueryChunk, QueryChunkMeta, QueryDatabase};
use server::{
    db::test_helpers::{run_query, write_lp},
    rules::ProvidedDatabaseRules,
    test_utils::{make_application, make_initialized_server},
    Db,
};
use test_helpers::maybe_start_logging;

#[tokio::test]
async fn delete_predicate_preservation() {
    maybe_start_logging();

    // ==================== setup ====================
    let server_id = ServerId::new(NonZeroU32::new(1).unwrap());
    let db_name = DatabaseName::new("delete_predicate_preservation_test").unwrap();

    let application = make_application();
    let server = make_initialized_server(server_id, Arc::clone(&application)).await;

    // Test that delete predicates are stored within the preserved catalog

    // ==================== do: create DB ====================
    // Create a DB given a server id, an object store and a db name

    let rules = DatabaseRules {
        partition_template: PartitionTemplate {
            parts: vec![TemplatePart::Column("part".to_string())],
        },
        lifecycle_rules: LifecycleRules {
            catalog_transactions_until_checkpoint: NonZeroU64::new(1).unwrap(),
            // do not prune transactions files because this tests relies on them
            catalog_transaction_prune_age: Duration::from_secs(1_000),
            late_arrive_window_seconds: NonZeroU32::new(1).unwrap(),
            ..Default::default()
        },
        ..DatabaseRules::new(db_name.clone())
    };

    let database = server
        .create_database(ProvidedDatabaseRules::new_rules(rules.clone().into()).unwrap())
        .await
        .unwrap();
    let db = database.initialized_db().unwrap();

    // ==================== do: create chunks ====================
    let table_name = "cpu";

    // 1: preserved
    let partition_key = "part_a";
    write_lp(&db, "cpu,part=a row=10,selector=0i 10").await;
    write_lp(&db, "cpu,part=a row=11,selector=1i 11").await;
    db.persist_partition(table_name, partition_key, true)
        .await
        .unwrap();

    // 2: RUB
    let partition_key = "part_b";
    write_lp(&db, "cpu,part=b row=20,selector=0i 20").await;
    write_lp(&db, "cpu,part=b row=21,selector=1i 21").await;
    db.compact_partition(table_name, partition_key)
        .await
        .unwrap();

    // 3: MUB
    let _partition_key = "part_c";
    write_lp(&db, "cpu,part=c row=30,selector=0i 30").await;
    write_lp(&db, "cpu,part=c row=31,selector=1i 31").await;

    // 4: preserved and unloaded
    let partition_key = "part_d";
    write_lp(&db, "cpu,part=d row=40,selector=0i 40").await;
    write_lp(&db, "cpu,part=d row=41,selector=1i 41").await;

    let chunk_id = db
        .persist_partition(table_name, partition_key, true)
        .await
        .unwrap()
        .unwrap()
        .id();

    db.unload_read_buffer(table_name, partition_key, chunk_id)
        .unwrap();

    // ==================== do: delete ====================
    let pred = Arc::new(DeletePredicate {
        range: TimestampRange {
            start: 0,
            end: 1_000,
        },
        exprs: vec![DeleteExpr::new(
            "selector".to_string(),
            predicate::delete_expr::Op::Eq,
            predicate::delete_expr::Scalar::I64(1),
        )],
    });
    db.delete("cpu", Arc::clone(&pred)).await.unwrap();

    // ==================== do: preserve another partition ====================
    let partition_key = "part_b";
    db.persist_partition(table_name, partition_key, true)
        .await
        .unwrap();

    // ==================== do: use background worker for a short while ====================
    let iters_start = db.worker_iterations_delete_predicate_preservation();
    // time_provider.inc(rules.lifecycle_rules.late_arrive_window());

    let t_0 = Instant::now();
    loop {
        let did_delete_predicate_preservation =
            db.worker_iterations_delete_predicate_preservation() > iters_start;
        let did_compaction = db.chunk_summaries().unwrap().into_iter().any(|summary| {
            (summary.partition_key.as_ref() == "part_c")
                && (summary.storage == ChunkStorage::ReadBuffer)
        });
        if did_delete_predicate_preservation && did_compaction {
            break;
        }
        assert!(t_0.elapsed() < Duration::from_secs(10));
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // ==================== check: delete predicates ====================

    let closure_check_delete_predicates = |db: &Db| {
        for chunk in db.chunks(&Default::default()) {
            let partition_key = chunk.addr().partition_key.as_ref();
            if partition_key == "part_b" {
                // Strictly speaking not required because the chunk was persisted AFTER the delete predicate was
                // registered so we can get away with materializing it during persistence.
                continue;
            }
            if partition_key == "part_c" {
                // This partition was compacted, so the delete predicates were materialized.
                continue;
            }
            let predicates = chunk.delete_predicates();
            assert_eq!(predicates.len(), 1);
            assert_eq!(predicates[0].as_ref(), pred.as_ref());
        }
    };
    closure_check_delete_predicates(&db);

    // ==================== check: query ====================
    let expected = vec![
        "+------+-----+----------+--------------------------------+",
        "| part | row | selector | time                           |",
        "+------+-----+----------+--------------------------------+",
        "| a    | 10  | 0        | 1970-01-01T00:00:00.000000010Z |",
        "| b    | 20  | 0        | 1970-01-01T00:00:00.000000020Z |",
        "| c    | 30  | 0        | 1970-01-01T00:00:00.000000030Z |",
        "| d    | 40  | 0        | 1970-01-01T00:00:00.000000040Z |",
        "+------+-----+----------+--------------------------------+",
    ];
    let batches = run_query(Arc::clone(&db), "select * from cpu order by time").await;
    assert_batches_sorted_eq!(&expected, &batches);

    // ==================== do: re-load DB ====================
    // Re-create database with same store, serverID, and DB name
    database.restart().await.unwrap();
    let db = database.initialized_db().unwrap();

    // ==================== check: delete predicates ====================
    closure_check_delete_predicates(&db);

    // ==================== check: query ====================
    // NOTE: partition "c" is gone here because it was not written to object store
    let expected = vec![
        "+------+-----+----------+--------------------------------+",
        "| part | row | selector | time                           |",
        "+------+-----+----------+--------------------------------+",
        "| a    | 10  | 0        | 1970-01-01T00:00:00.000000010Z |",
        "| b    | 20  | 0        | 1970-01-01T00:00:00.000000020Z |",
        "| d    | 40  | 0        | 1970-01-01T00:00:00.000000040Z |",
        "+------+-----+----------+--------------------------------+",
    ];

    let batches = run_query(Arc::clone(&db), "select * from cpu order by time").await;
    assert_batches_sorted_eq!(&expected, &batches);

    database.restart().await.unwrap();

    // ==================== do: remove checkpoint files ====================
    let iox_object_store =
        IoxObjectStore::find_existing(Arc::clone(application.object_store()), server_id, &db_name)
            .await
            .unwrap()
            .unwrap();

    let files = iox_object_store
        .catalog_transaction_files()
        .await
        .unwrap()
        .try_concat()
        .await
        .unwrap();

    let mut deleted_one = false;
    for file in files {
        if file.is_checkpoint() {
            iox_object_store
                .delete_catalog_transaction_file(&file)
                .await
                .unwrap();
            deleted_one = true;
        }
    }
    assert!(deleted_one);

    // ==================== do: re-load DB ====================
    // Re-create database with same store, serverID, and DB name
    database.restart().await.unwrap();
    let db = database.initialized_db().unwrap();

    // ==================== check: delete predicates ====================
    closure_check_delete_predicates(&db);

    // ==================== check: query ====================
    // NOTE: partition "c" is gone here because it was not written to object store
    let _expected = vec![
        "+------+-----+----------+--------------------------------+",
        "| part | row | selector | time                           |",
        "+------+-----+----------+--------------------------------+",
        "| a    | 10  | 0        | 1970-01-01T00:00:00.000000010Z |",
        "| b    | 20  | 0        | 1970-01-01T00:00:00.000000020Z |",
        "| d    | 40  | 0        | 1970-01-01T00:00:00.000000040Z |",
        "+------+-----+----------+--------------------------------+",
    ];
    let batches = run_query(Arc::clone(&db), "select * from cpu order by time").await;
    assert_batches_sorted_eq!(&expected, &batches);

    server.shutdown();
    server.join().await.unwrap();
}
