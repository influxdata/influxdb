mod common;

use arrow_util::assert_batches_sorted_eq;
use assert_matches::assert_matches;
pub use common::*;
use data_types::{Partition, PartitionKey, SequenceNumber};
use generated_types::ingester::IngesterQueryRequest;
use iox_time::{SystemProvider, TimeProvider};
use metric::{DurationHistogram, U64Counter, U64Gauge};

// Write data to an ingester through the write buffer interface, utilise the
// progress API to wait for it to become readable, and finally query the data
// and validate the contents.
#[tokio::test]
async fn test_write_query() {
    let mut ctx = TestContext::new().await;

    let ns = ctx.ensure_namespace("test_namespace", None).await;

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
            namespace_id: ns.id,
            table_id: ctx.table_id("test_namespace", "bananas").await,
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

// Ensure an ingester correctly seeks to the offset stored in the catalog at
// startup, skipping any empty offsets.
#[tokio::test]
async fn test_seek_on_init() {
    let mut ctx = TestContext::new().await;

    // Place some writes into the write buffer.

    let partition_key = PartitionKey::from("1970-01-01");

    let ns = ctx.ensure_namespace("test_namespace", None).await;
    ctx.write_lp(
        "test_namespace",
        "bananas greatness=\"unbounded\" 10",
        partition_key.clone(),
        0,
    )
    .await;

    // A subsequent write with a non-contiguous sequence number to a different
    // table.
    //
    // Resuming will be configured against an offset in the middle of the two
    // ranges.
    let w2 = ctx
        .write_lp(
            "test_namespace",
            "bananas greatness=\"amazing\",platanos=42 20",
            partition_key.clone(),
            7,
        )
        .await;

    // Wait for the writes to be processed.
    ctx.wait_for_readable(w2).await;

    // Assert the data in memory.
    let data = ctx
        .query(IngesterQueryRequest {
            namespace_id: ns.id,
            table_id: ctx.table_id("test_namespace", "bananas").await,
            columns: vec![],
            predicate: None,
        })
        .await
        .expect("query should succeed")
        .into_record_batches()
        .await;

    let expected = vec![
        "+-----------+----------+--------------------------------+",
        "| greatness | platanos | time                           |",
        "+-----------+----------+--------------------------------+",
        "| amazing   | 42       | 1970-01-01T00:00:00.000000020Z |",
        "| unbounded |          | 1970-01-01T00:00:00.000000010Z |",
        "+-----------+----------+--------------------------------+",
    ];
    assert_batches_sorted_eq!(&expected, &data);

    // Update the catalog state, causing the next boot of the ingester to seek
    // past the first write, but before the second write.
    ctx.catalog()
        .repositories()
        .await
        .shards()
        .update_min_unpersisted_sequence_number(ctx.shard_id(), SequenceNumber::new(3))
        .await
        .expect("failed to update persisted marker");

    // Restart the ingester.
    ctx.restart().await;

    // Wait for the second write to become readable again.
    ctx.wait_for_readable(w2).await;

    // Assert the data in memory now contains only w2.
    let data = ctx
        .query(IngesterQueryRequest {
            namespace_id: ns.id,
            table_id: ctx.table_id("test_namespace", "bananas").await,
            columns: vec![],
            predicate: None,
        })
        .await
        .expect("query should succeed")
        .into_record_batches()
        .await;

    let expected = vec![
        "+-----------+----------+--------------------------------+",
        "| greatness | platanos | time                           |",
        "+-----------+----------+--------------------------------+",
        "| amazing   | 42       | 1970-01-01T00:00:00.000000020Z |",
        "+-----------+----------+--------------------------------+",
    ];
    assert_batches_sorted_eq!(&expected, &data);
}

// Ensure an ingester respects the per-partition persist watermark, skipping
// already applied ops.
#[tokio::test]
async fn test_skip_previously_applied_partition_ops() {
    let mut ctx = TestContext::new().await;

    // Place some writes into the write buffer.
    let ns = ctx.ensure_namespace("test_namespace", None).await;
    let partition_key = PartitionKey::from("1970-01-01");
    ctx.write_lp(
        "test_namespace",
        "bananas greatness=\"unbounded\" 10",
        partition_key.clone(),
        5,
    )
    .await;
    let w2 = ctx
        .write_lp(
            "test_namespace",
            "bananas greatness=\"amazing\",platanos=42 20",
            partition_key.clone(),
            10,
        )
        .await;

    // Wait for the writes to be processed.
    ctx.wait_for_readable(w2).await;

    // Assert the data in memory.
    let data = ctx
        .query(IngesterQueryRequest {
            namespace_id: ns.id,
            table_id: ctx.table_id("test_namespace", "bananas").await,
            columns: vec![],
            predicate: None,
        })
        .await
        .expect("query should succeed")
        .into_record_batches()
        .await;

    let expected = vec![
        "+-----------+----------+--------------------------------+",
        "| greatness | platanos | time                           |",
        "+-----------+----------+--------------------------------+",
        "| amazing   | 42       | 1970-01-01T00:00:00.000000020Z |",
        "| unbounded |          | 1970-01-01T00:00:00.000000010Z |",
        "+-----------+----------+--------------------------------+",
    ];
    assert_batches_sorted_eq!(&expected, &data);

    // Read the partition ID of the writes above.
    let partitions = ctx
        .catalog()
        .repositories()
        .await
        .partitions()
        .list_by_namespace(ns.id)
        .await
        .unwrap();
    assert_matches!(&*partitions, &[Partition { .. }]);

    // And set the per-partition persist marker after the first write, but
    // before the second.
    ctx.catalog()
        .repositories()
        .await
        .partitions()
        .update_persisted_sequence_number(partitions[0].id, SequenceNumber::new(6))
        .await
        .expect("failed to update persisted marker");

    // Restart the ingester, which shall seek to the shard offset of 0, and
    // begin replaying ops.
    ctx.restart().await;

    // Wait for the second write to become readable again.
    ctx.wait_for_readable(w2).await;

    // Assert the partition replay skipped the first write.
    let data = ctx
        .query(IngesterQueryRequest {
            namespace_id: ns.id,
            table_id: ctx.table_id("test_namespace", "bananas").await,
            columns: vec![],
            predicate: None,
        })
        .await
        .expect("query should succeed")
        .into_record_batches()
        .await;

    let expected = vec![
        "+-----------+----------+--------------------------------+",
        "| greatness | platanos | time                           |",
        "+-----------+----------+--------------------------------+",
        "| amazing   | 42       | 1970-01-01T00:00:00.000000020Z |",
        "+-----------+----------+--------------------------------+",
    ];
    assert_batches_sorted_eq!(&expected, &data);
}

// Ensure a seek beyond the actual data available (i.e. into the future) causes
// a panic to bring about a human response.
#[tokio::test]
#[should_panic = "attempted to seek to offset 42, but current high watermark for partition 0 is 0"]
async fn test_seek_beyond_available_data() {
    let mut ctx = TestContext::new().await;

    // Place a write into the write buffer so it is not empty.
    ctx.ensure_namespace("test_namespace", None).await;
    ctx.write_lp(
        "test_namespace",
        "bananas greatness=\"unbounded\" 10",
        PartitionKey::from("1970-01-01"),
        0,
    )
    .await;

    // Update the catalog state, causing the next boot of the ingester to seek
    // past the write, beyond valid data offsets.
    ctx.catalog()
        .repositories()
        .await
        .shards()
        .update_min_unpersisted_sequence_number(ctx.shard_id(), SequenceNumber::new(42))
        .await
        .expect("failed to update persisted marker");

    // Restart the ingester.
    ctx.restart().await;
}

// Ensure an ingester configured to resume from offset 1 correctly seeks to the
// oldest available data when that offset no longer exists.
#[tokio::test]
async fn test_seek_dropped_offset() {
    let mut ctx = TestContext::new().await;

    // Place a write into the write buffer so it is not empty.
    let ns = ctx.ensure_namespace("test_namespace", None).await;

    // A write at offset 42
    let w1 = ctx
        .write_lp(
            "test_namespace",
            "bananas greatness=\"unbounded\" 10",
            PartitionKey::from("1970-01-01"),
            42,
        )
        .await;

    // Configure the ingester to seek to offset 1, which does not exist.
    ctx.catalog()
        .repositories()
        .await
        .shards()
        .update_min_unpersisted_sequence_number(ctx.shard_id(), SequenceNumber::new(1))
        .await
        .expect("failed to update persisted marker");

    // Restart the ingester.
    ctx.restart().await;

    // Wait for the op to be applied
    ctx.wait_for_readable(w1).await;

    // Assert the data in memory now contains only w2.
    let data = ctx
        .query(IngesterQueryRequest {
            namespace_id: ns.id,
            table_id: ctx.table_id("test_namespace", "bananas").await,
            columns: vec![],
            predicate: None,
        })
        .await
        .expect("query should succeed")
        .into_record_batches()
        .await;

    let expected = vec![
        "+-----------+--------------------------------+",
        "| greatness | time                           |",
        "+-----------+--------------------------------+",
        "| unbounded | 1970-01-01T00:00:00.000000010Z |",
        "+-----------+--------------------------------+",
    ];
    assert_batches_sorted_eq!(&expected, &data);

    // Ensure the metric was set to cause an alert for potential data loss.
    let metric = ctx
        .get_metric::<U64Counter, _>(
            "shard_reset_count",
            &[
                ("kafka_topic", TEST_TOPIC_NAME),
                ("kafka_partition", "0"),
                ("potential_data_loss", "true"),
            ],
        )
        .fetch();
    assert!(metric > 0);
}
