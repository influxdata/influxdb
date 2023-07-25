use arrow_util::assert_batches_sorted_eq;
use data_types::PartitionKey;
use ingester_query_grpc::influxdata::iox::ingester::v1::IngesterQueryRequest;
use ingester_test_ctx::TestContextBuilder;
use metric::{DurationHistogram, U64Histogram};

// Write data to an ingester through the RPC interface and query the data, validating the contents.
#[tokio::test]
async fn write_query() {
    let namespace_name = "write_query_test_namespace";
    let mut ctx = TestContextBuilder::default().build().await;
    let ns = ctx.ensure_namespace(namespace_name, None, None).await;

    // Initial write
    let partition_key = PartitionKey::from("1970-01-01");
    ctx.write_lp(
        namespace_name,
        "bananas greatness=\"unbounded\" 10",
        partition_key.clone(),
        0,
        None,
    )
    .await;

    // A subsequent write with a non-contiguous sequence number to a different table.
    ctx.write_lp(
        namespace_name,
        "cpu bar=2 20\ncpu bar=3 30",
        partition_key.clone(),
        7,
        None,
    )
    .await;

    // And a third write that appends more data to the table in the initial
    // write.
    ctx.write_lp(
        namespace_name,
        "bananas count=42 200",
        partition_key.clone(),
        42,
        None,
    )
    .await;

    // Perform a query to validate the actual data buffered.
    let data: Vec<_> = ctx
        .query(IngesterQueryRequest {
            namespace_id: ns.id.get(),
            table_id: ctx.table_id(namespace_name, "bananas").await.get(),
            columns: vec![],
            predicate: None,
        })
        .await
        .expect("query request failed");

    let expected = vec![
        "+-------+-----------+--------------------------------+",
        "| count | greatness | time                           |",
        "+-------+-----------+--------------------------------+",
        "|       | unbounded | 1970-01-01T00:00:00.000000010Z |",
        "| 42.0  |           | 1970-01-01T00:00:00.000000200Z |",
        "+-------+-----------+--------------------------------+",
    ];
    assert_batches_sorted_eq!(&expected, &data);

    // Assert various ingest metrics.
    let hist = ctx
        .get_metric::<DurationHistogram, _>(
            "ingester_dml_sink_apply_duration",
            &[("handler", "write_apply"), ("result", "success")],
        )
        .fetch();
    assert_eq!(hist.sample_count(), 3);

    // Read metrics
    let hist = ctx
        .get_metric::<DurationHistogram, _>(
            "ingester_query_stream_duration",
            &[("request", "complete")],
        )
        .fetch();
    assert_eq!(hist.sample_count(), 1);

    let hist = ctx
        .get_metric::<U64Histogram, _>("ingester_query_result_row", &[])
        .fetch();
    assert_eq!(hist.sample_count(), 1);
    assert_eq!(hist.total, 2);
}

// Write data to an ingester through the RPC interface and query the data, validating the contents.
#[tokio::test]
async fn write_query_projection() {
    let namespace_name = "write_query_test_namespace";
    let mut ctx = TestContextBuilder::default().build().await;
    let ns = ctx.ensure_namespace(namespace_name, None, None).await;

    // Initial write
    let partition_key = PartitionKey::from("1970-01-01");
    ctx.write_lp(
        namespace_name,
        "bananas greatness=\"unbounded\",level=42 10",
        partition_key.clone(),
        0,
        None,
    )
    .await;

    // Another write that appends more data to the table in the initial write.
    ctx.write_lp(
        namespace_name,
        "bananas count=42,level=4242 200",
        partition_key.clone(),
        42,
        None,
    )
    .await;

    // Perform a query to validate the actual data buffered.
    let data: Vec<_> = ctx
        .query(IngesterQueryRequest {
            namespace_id: ns.id.get(),
            table_id: ctx.table_id(namespace_name, "bananas").await.get(),
            columns: vec![],
            predicate: None,
        })
        .await
        .expect("query request failed");

    let expected = vec![
        "+-------+-----------+--------+--------------------------------+",
        "| count | greatness | level  | time                           |",
        "+-------+-----------+--------+--------------------------------+",
        "|       | unbounded | 42.0   | 1970-01-01T00:00:00.000000010Z |",
        "| 42.0  |           | 4242.0 | 1970-01-01T00:00:00.000000200Z |",
        "+-------+-----------+--------+--------------------------------+",
    ];
    assert_batches_sorted_eq!(&expected, &data);

    // And perform a query with projection, selecting a column that is entirely
    // non-NULL, a column containing NULLs (in a different order to the above)
    // and a column that does not exist.
    let data: Vec<_> = ctx
        .query(IngesterQueryRequest {
            namespace_id: ns.id.get(),
            table_id: ctx.table_id(namespace_name, "bananas").await.get(),
            columns: vec![
                "level".to_string(),
                "greatness".to_string(),
                "platanos".to_string(),
            ],
            predicate: None,
        })
        .await
        .expect("query request failed");

    let expected = vec![
        "+--------+-----------+",
        "| level  | greatness |",
        "+--------+-----------+",
        "| 42.0   | unbounded |",
        "| 4242.0 |           |",
        "+--------+-----------+",
    ];
    assert_batches_sorted_eq!(&expected, &data);
}
