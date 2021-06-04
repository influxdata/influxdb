#![allow(unused_imports, dead_code, unused_macros)]
use std::sync::Arc;

use arrow_util::assert_batches_sorted_eq;
use datafusion::logical_plan::{col, lit};
use query::{
    exec::stringset::StringSet,
    frontend::{influxrpc::InfluxRpcPlanner, sql::SqlQueryPlanner},
    predicate::PredicateBuilder,
    PartitionChunk,
};

use crate::{db::test_helpers::write_lp, query_tests::utils::make_db};

use super::utils::TestDb;

async fn setup() -> TestDb {
    // Test that partition pruning is connected up
    let test_db = make_db().await;
    let db = &test_db.db;

    // Chunk 0 has bar:[1-2]
    write_lp(db, "cpu bar=1 10");
    write_lp(db, "cpu bar=2 20");

    let partition_key = "1970-01-01T00";
    let mb_chunk = db
        .rollover_partition(partition_key, "cpu")
        .await
        .unwrap()
        .unwrap();
    db.load_chunk_to_read_buffer(partition_key, "cpu", mb_chunk.id(), &Default::default())
        .await
        .unwrap();

    // Chunk 1 has bar:[3-3] (going to get pruned)
    write_lp(db, "cpu bar=3 10");
    write_lp(db, "cpu bar=3 100");
    write_lp(db, "cpu bar=3 1000");

    let partition_key = "1970-01-01T00";
    let mb_chunk = db
        .rollover_partition(partition_key, "cpu")
        .await
        .unwrap()
        .unwrap();
    db.load_chunk_to_read_buffer(partition_key, "cpu", mb_chunk.id(), &Default::default())
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
    } = setup().await;
    let db = Arc::new(db);

    let expected = vec![
        "+-----+-------------------------------+",
        "| bar | time                          |",
        "+-----+-------------------------------+",
        "| 1   | 1970-01-01 00:00:00.000000010 |",
        "| 2   | 1970-01-01 00:00:00.000000020 |",
        "+-----+-------------------------------+",
    ];
    let query = "select * from cpu where bar < 3.0";

    let executor = db.executor();
    let physical_plan = SqlQueryPlanner::default()
        .query(db, query, &executor)
        .unwrap();
    let batches = executor.collect(physical_plan).await.unwrap();

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
    } = setup().await;
    let db = Arc::new(db);

    let predicate = PredicateBuilder::new()
        // bar < 3.0
        .add_expr(col("bar").lt(lit(3.0)))
        .build();

    let mut expected = StringSet::new();
    expected.insert("cpu".into());

    let plan = InfluxRpcPlanner::new()
        .table_names(db.as_ref(), predicate)
        .unwrap();

    let result = db.executor().to_string_set(plan).await.unwrap();

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
