use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use arrow_util::assert_batches_sorted_eq;
use object_store::{ObjectStore, ObjectStoreIntegration};
use query::{
    exec::{ExecutionContextProvider, IOxExecutionContext},
    frontend::sql::SqlQueryPlanner,
    QueryChunk,
};
use server::{db::test_helpers::write_lp, utils::TestDb};

#[tokio::test]
async fn test_query_cancellation_slow_store() {
    let object_store = Arc::new(ObjectStore::new_in_memory_throttled(Default::default()));

    // create test DB
    let test_db = TestDb::builder()
        .object_store(Arc::clone(&object_store))
        .build()
        .await;
    let db = test_db.db;

    let partition_key = "1970-01-01T00";
    let table_name = "cpu";

    // create persisted chunk
    let data = "cpu,region=west user=23.2 100";
    write_lp(&db, data).await;
    db.rollover_partition(table_name, partition_key)
        .await
        .unwrap();
    db.compact_partition(table_name, partition_key)
        .await
        .unwrap();
    let id = db
        .persist_partition("cpu", partition_key, true)
        .await
        .unwrap()
        .unwrap()
        .id();

    // unload read buffer from persisted chunk so that object store access is required
    db.unload_read_buffer(table_name, partition_key, id)
        .unwrap();

    // create in-memory chunk
    let data = "cpu,region=east user=0.1 42";
    write_lp(&db, data).await;

    // make store access really slow
    if let ObjectStoreIntegration::InMemoryThrottled(inner) = &object_store.integration {
        inner.config_mut(|cfg| cfg.wait_get_per_call = Duration::from_secs(1_000));
    } else {
        panic!("wrong store type");
    }

    // setup query context
    let ctx = db.new_query_context(None);
    wait_for_tasks(&ctx, 0).await;

    // query fast part
    let expected_fast = vec![
        "+--------+--------------------------------+------+",
        "| region | time                           | user |",
        "+--------+--------------------------------+------+",
        "| east   | 1970-01-01T00:00:00.000000042Z | 0.1  |",
        "+--------+--------------------------------+------+",
    ];
    let query_fast = "select * from cpu where region='east'";
    let physical_plan = SqlQueryPlanner::default()
        .query(query_fast, &ctx)
        .await
        .unwrap();
    let batches = ctx.collect(physical_plan).await.unwrap();
    assert_batches_sorted_eq!(&expected_fast, &batches);
    wait_for_tasks(&ctx, 0).await;

    // query blocked part
    let query_slow = "select * from cpu where region='west'";
    let physical_plan = SqlQueryPlanner::default()
        .query(query_slow, &ctx)
        .await
        .unwrap();
    let ctx_captured = ctx.child_ctx("slow");
    let passed = Arc::new(AtomicBool::new(false));
    let passed_captured = Arc::clone(&passed);
    let join_handle = tokio::spawn(async move {
        ctx_captured.collect(physical_plan).await.unwrap();
        passed_captured.store(true, Ordering::SeqCst);
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert!(!passed.load(Ordering::SeqCst));
    wait_for_tasks(&ctx, 1).await;

    // querying fast part should not be blocked
    let physical_plan = SqlQueryPlanner::default()
        .query(query_fast, &ctx)
        .await
        .unwrap();
    let batches = ctx.collect(physical_plan).await.unwrap();
    assert_batches_sorted_eq!(&expected_fast, &batches);
    wait_for_tasks(&ctx, 1).await;

    // canceling the blocking query should free resources again
    // cancelation might take a short while
    join_handle.abort();
    wait_for_tasks(&ctx, 0).await;
    assert!(!passed.load(Ordering::SeqCst));
}

/// Wait up to 10s for correct task count.
async fn wait_for_tasks(ctx: &IOxExecutionContext, n: usize) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if dbg!(ctx.tasks()) == n {
                return;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
}
