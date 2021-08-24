#![allow(unused_imports, dead_code, unused_macros)]
use std::sync::Arc;

use arrow_util::assert_batches_sorted_eq;
use datafusion::logical_plan::{col, lit};
use query::{
    exec::{stringset::StringSet, ExecutorType},
    frontend::{influxrpc::InfluxRpcPlanner, sql::SqlQueryPlanner},
    predicate::PredicateBuilder,
    QueryChunk,
};

use server::db::test_helpers::write_lp;
use server::utils::{make_db, TestDb};

async fn setup() -> TestDb {
    // Test that partition pruning is connected up
    let test_db = make_db().await;
    let db = &test_db.db;

    // Chunk 0 has bar:[1-2]
    write_lp(db, "cpu bar=1 10").await;
    write_lp(db, "cpu bar=2 20").await;

    let partition_key = "1970-01-01T00";
    let mb_chunk = db
        .rollover_partition("cpu", partition_key)
        .await
        .unwrap()
        .unwrap();
    db.move_chunk_to_read_buffer("cpu", partition_key, mb_chunk.id())
        .await
        .unwrap();

    // Chunk 1 has bar:[3-3] (going to get pruned)
    write_lp(db, "cpu bar=3 10").await;
    write_lp(db, "cpu bar=3 100").await;
    write_lp(db, "cpu bar=3 1000").await;

    let partition_key = "1970-01-01T00";
    let mb_chunk = db
        .rollover_partition("cpu", partition_key)
        .await
        .unwrap()
        .unwrap();
    db.move_chunk_to_read_buffer("cpu", partition_key, mb_chunk.id())
        .await
        .unwrap();

    test_db
}

#[tokio::test]
async fn chunk_pruning_sql() {
    ::test_helpers::maybe_start_logging();
    // Test that partition pruning is connected up
    let TestDb {
        db,
        metric_registry,
        ..
    } = setup().await;

    let expected = vec![
        "+-----+--------------------------------+",
        "| bar | time                           |",
        "+-----+--------------------------------+",
        "| 1   | 1970-01-01T00:00:00.000000010Z |",
        "| 2   | 1970-01-01T00:00:00.000000020Z |",
        "+-----+--------------------------------+",
    ];
    let query = "select * from cpu where bar < 3.0";

    let ctx = db.new_query_context();
    let physical_plan = SqlQueryPlanner::default().query(query, &ctx).unwrap();
    let batches = ctx.collect(physical_plan).await.unwrap();

    assert_batches_sorted_eq!(&expected, &batches);

    // Validate that the chunk was pruned using the metrics
    metric_registry
        .has_metric_family("query_access_pruned_chunks_total")
        .with_labels(&[
            ("db_name", "placeholder"),
            ("table_name", "cpu"),
            ("svr_id", "1"),
        ])
        .counter()
        .eq(1.0)
        .unwrap();

    // Validate that the chunk was pruned using the metrics
    metric_registry
        .has_metric_family("query_access_pruned_rows_total")
        .with_labels(&[
            ("db_name", "placeholder"),
            ("table_name", "cpu"),
            ("svr_id", "1"),
        ])
        .counter()
        .eq(3.0)
        .unwrap();
}

#[tokio::test]
async fn chunk_pruning_influxrpc() {
    ::test_helpers::maybe_start_logging();
    // Test that partition pruning is connected up
    let TestDb {
        db,
        metric_registry,
        ..
    } = setup().await;

    let predicate = PredicateBuilder::new()
        // bar < 3.0
        .add_expr(col("bar").lt(lit(3.0)))
        .build();

    let mut expected = StringSet::new();
    expected.insert("cpu".into());

    let ctx = db.executor().new_context(query::exec::ExecutorType::Query);

    let plan = InfluxRpcPlanner::new()
        .table_names(db.as_ref(), predicate)
        .unwrap();

    let result = ctx.to_string_set(plan).await.unwrap();

    assert_eq!(&expected, result.as_ref());

    // Validate that the chunk was pruned using the metrics
    metric_registry
        .has_metric_family("query_access_pruned_chunks_total")
        .with_labels(&[
            ("db_name", "placeholder"),
            ("table_name", "cpu"),
            ("svr_id", "1"),
        ])
        .counter()
        .eq(1.0)
        .unwrap();

    // Validate that the chunk was pruned using the metrics
    metric_registry
        .has_metric_family("query_access_pruned_rows_total")
        .with_labels(&[
            ("db_name", "placeholder"),
            ("table_name", "cpu"),
            ("svr_id", "1"),
        ])
        .counter()
        .eq(3.0)
        .unwrap();
}
