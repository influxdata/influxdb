#![allow(unused_imports, dead_code, unused_macros)]

use arrow_util::assert_batches_sorted_eq;
use datafusion::logical_plan::{col, lit};
use db::{
    test_helpers::write_lp,
    utils::{make_db, TestDb},
};
use metric::{Attributes, Metric, U64Counter};
use predicate::predicate::PredicateBuilder;
use query::{
    exec::{stringset::StringSet, ExecutionContextProvider, ExecutorType},
    frontend::{influxrpc::InfluxRpcPlanner, sql::SqlQueryPlanner},
    QueryChunk,
};
use std::sync::Arc;

async fn setup() -> TestDb {
    // Test that partition pruning is connected up
    let test_db = make_db().await;
    let db = &test_db.db;

    // Chunk 0 has bar:[1-2]
    write_lp(db, "cpu bar=1 10");
    write_lp(db, "cpu bar=2 20");

    let partition_key = "1970-01-01T00";
    db.compact_open_chunk("cpu", partition_key).await.unwrap();

    // Chunk 1 has bar:[3-3] (going to get pruned)
    write_lp(db, "cpu bar=3 10");
    write_lp(db, "cpu bar=3 100");
    write_lp(db, "cpu bar=3 1000");

    let partition_key = "1970-01-01T00";
    db.compact_open_chunk("cpu", partition_key).await.unwrap();

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

    let ctx = db.new_query_context(None);
    let physical_plan = SqlQueryPlanner::default().query(query, &ctx).await.unwrap();
    let batches = ctx.collect(physical_plan).await.unwrap();

    assert_batches_sorted_eq!(&expected, &batches);

    let database_attributes = Attributes::from(&[("db_name", "placeholder")]);
    let table_attributes = Attributes::from(&[("db_name", "placeholder"), ("table_name", "cpu")]);
    // Validate that the chunk was pruned using the metrics
    let pruned_chunks = metric_registry
        .get_instrument::<Metric<U64Counter>>("query_access_pruned_chunks")
        .unwrap()
        .get_observer(&table_attributes)
        .unwrap()
        .fetch();
    assert_eq!(pruned_chunks, 1);

    // Validate that the chunk was pruned using the metrics
    let pruned_rows = metric_registry
        .get_instrument::<Metric<U64Counter>>("query_access_pruned_rows")
        .unwrap()
        .get_observer(&table_attributes)
        .unwrap()
        .fetch();
    assert_eq!(pruned_rows, 3);

    // Validate that it recorded that pruning took place
    let prune_count = metric_registry
        .get_instrument::<Metric<U64Counter>>("query_access_prune")
        .unwrap()
        .get_observer(&database_attributes)
        .unwrap()
        .fetch();
    assert_eq!(prune_count, 2);

    // Validate that it recorded that the catalog was snapshotted
    let snapshot_count = metric_registry
        .get_instrument::<Metric<U64Counter>>("query_access_catalog_snapshot")
        .unwrap()
        .get_observer(&database_attributes)
        .unwrap()
        .fetch();
    assert_eq!(snapshot_count, 1);
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

    let attributes = Attributes::from(&[("db_name", "placeholder"), ("table_name", "cpu")]);
    // Validate that the chunk was pruned using the metrics
    let pruned_chunks = metric_registry
        .get_instrument::<Metric<U64Counter>>("query_access_pruned_chunks")
        .unwrap()
        .get_observer(&attributes)
        .unwrap()
        .fetch();
    assert_eq!(pruned_chunks, 1);

    // Validate that the chunk was pruned using the metrics
    let pruned_rows = metric_registry
        .get_instrument::<Metric<U64Counter>>("query_access_pruned_rows")
        .unwrap()
        .get_observer(&attributes)
        .unwrap()
        .fetch();
    assert_eq!(pruned_rows, 3);
}
