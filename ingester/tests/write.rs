mod common;

use arrow_util::assert_batches_sorted_eq;
pub use common::*;
use data_types::PartitionKey;
use generated_types::ingester::IngesterQueryRequest;
use iox_time::{SystemProvider, TimeProvider};
use metric::{DurationHistogram, U64Counter, U64Gauge};

// Write data to an ingester through the write buffer interface, utilise the
// progress API to wait for it to become readable, and finally query the data
// and validate the contents.
#[tokio::test]
async fn test_write_query() {
    let mut ctx = TestContext::new().await;

    ctx.ensure_namespace("test_namespace").await;

    // Initial write
    let partition_key = PartitionKey::from("1970-01-01");
    ctx.write_lp(
        "test_namespace",
        "bananas greatness=\"unbounded\" 10",
        partition_key.clone(),
        0,
    )
    .await;

    // A subsequent write with a non-contiguous sequence number to a different table.
    ctx.write_lp(
        "test_namespace",
        "cpu bar=2 20\ncpu bar=3 30",
        partition_key.clone(),
        7,
    )
    .await;

    // And a third write that appends more data to the table in the initial
    // write.
    let offset = ctx
        .write_lp(
            "test_namespace",
            "bananas count=42 200",
            partition_key.clone(),
            42,
        )
        .await;

    ctx.wait_for_readable(offset).await;

    // Perform a query to validate the actual data buffered.
    let data = ctx
        .query(IngesterQueryRequest {
            namespace: "test_namespace".to_string(),
            table: "bananas".to_string(),
            columns: vec![],
            predicate: None,
        })
        .await
        .expect("query should succeed")
        .into_record_batches()
        .await;

    let expected = vec![
        "+-------+-----------+--------------------------------+",
        "| count | greatness | time                           |",
        "+-------+-----------+--------------------------------+",
        "|       | unbounded | 1970-01-01T00:00:00.000000010Z |",
        "| 42    |           | 1970-01-01T00:00:00.000000200Z |",
        "+-------+-----------+--------------------------------+",
    ];
    assert_batches_sorted_eq!(&expected, &data);

    // Assert various ingest metrics.
    let hist = ctx
        .get_metric::<DurationHistogram, _>(
            "ingester_op_apply_duration",
            &[
                ("kafka_topic", TEST_TOPIC_NAME),
                ("kafka_partition", "0"),
                ("result", "success"),
            ],
        )
        .fetch();
    assert_eq!(hist.sample_count(), 3);

    let metric = ctx
        .get_metric::<U64Counter, _>(
            "ingester_write_buffer_read_bytes",
            &[("kafka_topic", TEST_TOPIC_NAME), ("kafka_partition", "0")],
        )
        .fetch();
    assert_eq!(metric, 150);

    let metric = ctx
        .get_metric::<U64Gauge, _>(
            "ingester_write_buffer_last_sequence_number",
            &[("kafka_topic", TEST_TOPIC_NAME), ("kafka_partition", "0")],
        )
        .fetch();
    assert_eq!(metric, 42);

    let metric = ctx
        .get_metric::<U64Gauge, _>(
            "ingester_write_buffer_sequence_number_lag",
            &[("kafka_topic", TEST_TOPIC_NAME), ("kafka_partition", "0")],
        )
        .fetch();
    assert_eq!(metric, 0);

    let metric = ctx
        .get_metric::<U64Gauge, _>(
            "ingester_write_buffer_last_ingest_ts",
            &[("kafka_topic", TEST_TOPIC_NAME), ("kafka_partition", "0")],
        )
        .fetch();
    let now = SystemProvider::new().now();
    assert!(metric < now.timestamp_nanos() as _);
}
