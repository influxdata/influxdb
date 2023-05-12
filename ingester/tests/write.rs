use arrow_util::assert_batches_sorted_eq;
use assert_matches::assert_matches;
use data_types::PartitionKey;
use ingester_query_grpc::influxdata::iox::ingester::v1::IngesterQueryRequest;
use ingester_test_ctx::TestContextBuilder;
use iox_catalog::interface::Catalog;
use metric::{DurationHistogram, U64Histogram};
use std::sync::Arc;

// Write data to an ingester through the RPC interface and query the data, validating the contents.
#[tokio::test]
async fn write_query() {
    let namespace_name = "write_query_test_namespace";
    let mut ctx = TestContextBuilder::default().build().await;
    let ns = ctx.ensure_namespace(namespace_name, None).await;

    // Initial write
    let partition_key = PartitionKey::from("1970-01-01");
    ctx.write_lp(
        namespace_name,
        "bananas greatness=\"unbounded\" 10",
        partition_key.clone(),
        0,
    )
    .await;

    // A subsequent write with a non-contiguous sequence number to a different table.
    ctx.write_lp(
        namespace_name,
        "cpu bar=2 20\ncpu bar=3 30",
        partition_key.clone(),
        7,
    )
    .await;

    // And a third write that appends more data to the table in the initial
    // write.
    ctx.write_lp(
        namespace_name,
        "bananas count=42 200",
        partition_key.clone(),
        42,
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
            &[("request", "complete"), ("has_error", "false")],
        )
        .fetch();
    assert_eq!(hist.sample_count(), 1);

    let hist = ctx
        .get_metric::<U64Histogram, _>("ingester_query_result_row", &[])
        .fetch();
    assert_eq!(hist.sample_count(), 1);
    assert_eq!(hist.total, 2);
}

// Write data to the ingester, which writes it to the WAL, then drop and recreate the WAL and
// validate the data is replayed from the WAL into memory.
#[tokio::test]
async fn wal_replay() {
    let wal_dir = Arc::new(test_helpers::tmp_dir().unwrap());
    let metrics: Arc<metric::Registry> = Default::default();
    let catalog: Arc<dyn Catalog> =
        Arc::new(iox_catalog::mem::MemCatalog::new(Arc::clone(&metrics)));
    let namespace_name = "wal_replay_test_namespace";

    {
        let mut ctx = TestContextBuilder::default()
            .with_wal_dir(Arc::clone(&wal_dir))
            .with_catalog(Arc::clone(&catalog))
            .build()
            .await;

        let ns = ctx.ensure_namespace(namespace_name, None).await;

        // Initial write
        let partition_key = PartitionKey::from("1970-01-01");
        ctx.write_lp(
            namespace_name,
            "bananas greatness=\"unbounded\" 10",
            partition_key.clone(),
            0,
        )
        .await;

        // A subsequent write with a non-contiguous sequence number to a different table.
        ctx.write_lp(
            namespace_name,
            "cpu bar=2 20\ncpu bar=3 30",
            partition_key.clone(),
            7,
        )
        .await;

        // And a third write that appends more data to the table in the initial
        // write.
        ctx.write_lp(
            namespace_name,
            "bananas count=42 200",
            partition_key.clone(),
            42,
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
    } // Drop the first ingester instance

    // Restart the ingester and perform replay by creating another ingester using the same WAL
    // directory and catalog
    let ctx = TestContextBuilder::default()
        .with_wal_dir(wal_dir)
        .with_catalog(catalog)
        .build()
        .await;

    // Validate the data has been replayed and is now in object storage (it won't be in the
    // ingester's memory because replaying the WAL also persists).
    let parquet_files = ctx.catalog_parquet_file_records(namespace_name).await;
    assert_eq!(parquet_files.len(), 2);
    let mut expected_table_ids = vec![
        ctx.table_id(namespace_name, "bananas").await,
        ctx.table_id(namespace_name, "cpu").await,
    ];
    expected_table_ids.sort();
    let mut actual_table_ids: Vec<_> = parquet_files.iter().map(|pf| pf.table_id).collect();
    actual_table_ids.sort();
    assert_eq!(actual_table_ids, expected_table_ids);
}

// Ensure that data applied to an ingester is persisted at shutdown, and the WAL
// files are cleared.
#[tokio::test]
async fn graceful_shutdown() {
    let wal_dir = Arc::new(test_helpers::tmp_dir().unwrap());
    let metrics: Arc<metric::Registry> = Default::default();
    let catalog: Arc<dyn Catalog> =
        Arc::new(iox_catalog::mem::MemCatalog::new(Arc::clone(&metrics)));
    let namespace_name = "wal_replay_test_namespace";

    let mut ctx = TestContextBuilder::default()
        .with_wal_dir(Arc::clone(&wal_dir))
        .with_catalog(Arc::clone(&catalog))
        .build()
        .await;

    let ns = ctx.ensure_namespace(namespace_name, None).await;
    let namespace_id = ns.id;

    // Initial write
    let partition_key = PartitionKey::from("1970-01-01");
    ctx.write_lp(
        namespace_name,
        "bananas greatness=\"unbounded\" 10",
        partition_key.clone(),
        0,
    )
    .await;

    // Persist the data
    ctx.persist(namespace_name).await;

    // A subsequent write with a non-contiguous sequence number to a different table.
    ctx.write_lp(
        namespace_name,
        "cpu bar=2 20\ncpu bar=3 30",
        partition_key.clone(),
        7,
    )
    .await;

    // And a third write that appends more data to the table in the initial
    // write.
    ctx.write_lp(
        namespace_name,
        "bananas count=42 200",
        partition_key.clone(),
        42,
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
        "+-------+--------------------------------+",
        "| count | time                           |",
        "+-------+--------------------------------+",
        "| 42.0  | 1970-01-01T00:00:00.000000200Z |",
        "+-------+--------------------------------+",
    ];
    assert_batches_sorted_eq!(&expected, &data);

    // Gracefully stop the ingester.
    ctx.shutdown().await;

    // Inspect the WAL files.
    //
    // There should be one WAL file, containing no operations.
    let wal = wal::Wal::new(wal_dir.path())
        .await
        .expect("failed to reinitialise WAL");

    let wal_files = wal.closed_segments();
    assert_eq!(wal_files.len(), 1);

    let mut reader = wal
        .reader_for_segment(wal_files[0].id())
        .expect("failed to open wal segment");

    // Assert the file contains no operations
    assert_matches!(
        reader
            .next_batch()
            .expect("failed to read wal segment contents"),
        None
    );

    // Validate the parquet files were added to the catalog during shutdown.
    let parquet_files = catalog
        .repositories()
        .await
        .parquet_files()
        .list_by_namespace_not_to_delete(namespace_id)
        .await
        .unwrap();
    assert_eq!(parquet_files.len(), 3);
}
