use std::ffi::OsString;
use std::fs::read_dir;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use arrow_util::assert_batches_sorted_eq;
use assert_matches::assert_matches;
use data_types::{PartitionKey, TableId, Timestamp};
use ingester_query_grpc::influxdata::iox::ingester::v1::IngesterQueryRequest;
use ingester_test_ctx::{TestContextBuilder, DEFAULT_MAX_PERSIST_QUEUE_DEPTH};
use iox_catalog::interface::Catalog;
use itertools::Itertools;
use metric::{
    assert_counter, assert_histogram, DurationHistogram, U64Counter, U64Gauge, U64Histogram,
};
use parquet_file::ParquetFilePath;
use test_helpers::timeout::FutureTimeout;
use trace::{ctx::SpanContext, RingBufferTraceCollector};

// Write data to an ingester through the RPC interface and persist the data.
#[tokio::test]
async fn write_persist() {
    let namespace_name = "write_query_test_namespace";
    let mut ctx = TestContextBuilder::default().build().await;
    let ns = ctx.ensure_namespace(namespace_name, None, None).await;

    let partition_key = PartitionKey::from("1970-01-01");
    ctx.write_lp(
        namespace_name,
        r#"bananas count=42,greatness="inf" 200"#,
        partition_key.clone(),
        42,
        None,
    )
    .await;

    // Perform a query to validate the actual data buffered.
    let table_id = ctx.table_id(namespace_name, "bananas").await.get();
    let data: Vec<_> = ctx
        .query(IngesterQueryRequest {
            namespace_id: ns.id.get(),
            table_id,
            columns: vec![],
            predicate: None,
        })
        .await
        .expect("query request failed");

    let expected = vec![
        "+-------+-----------+--------------------------------+",
        "| count | greatness | time                           |",
        "+-------+-----------+--------------------------------+",
        "| 42.0  | inf       | 1970-01-01T00:00:00.000000200Z |",
        "+-------+-----------+--------------------------------+",
    ];
    assert_batches_sorted_eq!(&expected, &data);

    // Persist the data.
    ctx.persist(namespace_name).await;

    // Ensure the data is no longer buffered.
    let data: Vec<_> = ctx
        .query(IngesterQueryRequest {
            namespace_id: ns.id.get(),
            table_id,
            columns: vec![],
            predicate: None,
        })
        .await
        .expect("query request failed");
    assert!(data.is_empty());

    // Validate the parquet file was added to the catalog
    let parquet_files = ctx.catalog_parquet_file_records(namespace_name).await;
    let (path, want_file_size) = assert_matches!(parquet_files.as_slice(), [f] => {
        assert_eq!(f.namespace_id, ns.id);
        assert_eq!(f.table_id, TableId::new(table_id));
        assert_eq!(f.min_time, Timestamp::new(200));
        assert_eq!(f.max_time, Timestamp::new(200));
        assert_eq!(f.to_delete, None);
        assert_eq!(f.row_count, 1);
        assert_eq!(f.column_set.len(), 3);
        assert_eq!(f.max_l0_created_at, f.created_at);

        (ParquetFilePath::from(f), f.file_size_bytes)
    });

    // Validate the file exists at the expected object store path.
    let file_size = ctx
        .object_store()
        .get(&path.object_store_path())
        .await
        .expect("parquet file must exist in object store")
        .bytes()
        .await
        .expect("failed to read parquet file bytes")
        .len();
    assert_eq!(file_size, want_file_size as usize);

    // And that the persist metrics were recorded.
    let metrics = ctx.metrics();

    ////////////////////////////////////////////////////////////////////////////
    // Config reflection metrics
    assert_counter!(
        metrics,
        U64Gauge,
        "ingester_persist_max_parallelism",
        value = 5,
    );

    assert_counter!(
        metrics,
        U64Gauge,
        "ingester_persist_max_queue_depth",
        value = DEFAULT_MAX_PERSIST_QUEUE_DEPTH as u64,
    );

    ////////////////////////////////////////////////////////////////////////////
    // Persist worker metrics
    assert_histogram!(
        metrics,
        DurationHistogram,
        "ingester_persist_active_duration",
        samples = 1,
    );

    assert_histogram!(
        metrics,
        DurationHistogram,
        "ingester_persist_enqueue_duration",
        samples = 1,
    );

    assert_counter!(
        metrics,
        U64Counter,
        "ingester_persist_enqueued_jobs",
        value = 1,
    );

    ////////////////////////////////////////////////////////////////////////////
    // Parquet file metrics
    assert_histogram!(
        metrics,
        DurationHistogram,
        "ingester_persist_parquet_file_time_range",
        samples = 1,
        sum = Duration::from_secs(0),
    );

    assert_histogram!(
        metrics,
        U64Histogram,
        "ingester_persist_parquet_file_size_bytes",
        samples = 1,
    );

    assert_histogram!(
        metrics,
        U64Histogram,
        "ingester_persist_parquet_file_row_count",
        samples = 1,
        sum = 1,
    );

    assert_histogram!(
        metrics,
        U64Histogram,
        "ingester_persist_parquet_file_column_count",
        samples = 1,
        sum = 3,
    );
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

    let ns = ctx.ensure_namespace(namespace_name, None, None).await;
    let namespace_id = ns.id;

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

    // Persist the data
    ctx.persist(namespace_name).await;

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
    assert_matches!(reader.next(), None);

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

#[tokio::test]
async fn wal_reference_dropping() {
    let wal_dir = Arc::new(test_helpers::tmp_dir().unwrap());
    let metrics = Arc::new(metric::Registry::default());
    let catalog: Arc<dyn Catalog> =
        Arc::new(iox_catalog::mem::MemCatalog::new(Arc::clone(&metrics)));

    // Test-local namespace name
    const TEST_NAMESPACE_NAME: &str = "wal_reference_dropping_test_namespace";
    // Create an ingester using a fairly low write-ahead log rotation interval
    const WAL_ROTATION_PERIOD: Duration = Duration::from_secs(15);

    // Create an ingester with a low
    let mut ctx = TestContextBuilder::default()
        .with_wal_dir(Arc::clone(&wal_dir))
        .with_catalog(Arc::clone(&catalog))
        .with_wal_rotation_period(WAL_ROTATION_PERIOD)
        .build()
        .await;

    let ns = ctx.ensure_namespace(TEST_NAMESPACE_NAME, None, None).await;

    // Initial write
    let partition_key = PartitionKey::from("1970-01-01");
    ctx.write_lp(
        TEST_NAMESPACE_NAME,
        "bananas greatness=\"unbounded\" 10",
        partition_key.clone(),
        0,
        None,
    )
    .await;

    // A subsequent write with a non-contiguous sequence number to a different table.
    ctx.write_lp(
        TEST_NAMESPACE_NAME,
        "cpu bar=2 20\ncpu bar=3 30",
        partition_key.clone(),
        7,
        None,
    )
    .await;

    // And a third write that appends more data to the table in the initial
    // write.
    ctx.write_lp(
        TEST_NAMESPACE_NAME,
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
            table_id: ctx.table_id(TEST_NAMESPACE_NAME, "bananas").await.get(),
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

    let initial_segment_names =
        get_file_names_in_dir(wal_dir.path()).expect("should be able to get file names");
    assert_eq!(initial_segment_names.len(), 1); // Ensure a single (open) segment is present

    tokio::time::pause();
    tokio::time::advance(WAL_ROTATION_PERIOD).await;
    tokio::time::resume();

    // Wait for the rotation to result in the initial segment no longer being present in
    // the write-ahead log directory
    async {
        loop {
            let segments =
                get_file_names_in_dir(wal_dir.path()).expect("should be able to get file names");
            if !segments
                .iter()
                .any(|name| initial_segment_names.contains(name))
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    }
    .with_timeout_panic(Duration::from_secs(5))
    .await;

    let final_segment_names =
        get_file_names_in_dir(wal_dir.path()).expect("should be able to get file names");
    assert_eq!(final_segment_names.len(), 1); // Ensure a single (open) segment is present after the old one has been dropped
}

fn get_file_names_in_dir(dir: &Path) -> Result<Vec<OsString>, std::io::Error> {
    read_dir(dir)?
        .filter_map_ok(|f| {
            if let Ok(file_type) = f.file_type() {
                if file_type.is_file() {
                    return Some(f.file_name());
                }
            }
            None
        })
        .collect::<Result<Vec<_>, std::io::Error>>()
}

#[tokio::test]
async fn write_tracing() {
    let namespace_name = "write_tracing_test_namespace";
    let mut ctx = TestContextBuilder::default().build().await;
    let ns = ctx.ensure_namespace(namespace_name, None, None).await;

    let trace_collector = Arc::new(RingBufferTraceCollector::new(5));
    let span_ctx = SpanContext::new(Arc::new(Arc::clone(&trace_collector)));
    let request_span = span_ctx.child("write request span");

    let partition_key = PartitionKey::from("1970-01-01");
    ctx.write_lp(
        namespace_name,
        r#"bananas count=42,greatness="inf" 200"#,
        partition_key.clone(),
        42,
        Some(request_span.ctx.clone()),
    )
    .await;

    // Perform a query to validate the actual data buffered.
    let table_id = ctx.table_id(namespace_name, "bananas").await.get();
    let data: Vec<_> = ctx
        .query(IngesterQueryRequest {
            namespace_id: ns.id.get(),
            table_id,
            columns: vec![],
            predicate: None,
        })
        .await
        .expect("query request failed");

    let expected = vec![
        "+-------+-----------+--------------------------------+",
        "| count | greatness | time                           |",
        "+-------+-----------+--------------------------------+",
        "| 42.0  | inf       | 1970-01-01T00:00:00.000000200Z |",
        "+-------+-----------+--------------------------------+",
    ];
    assert_batches_sorted_eq!(&expected, &data);

    // Check the spans emitted for the write request align capture what is expected
    let spans = trace_collector.spans();
    assert_matches!(spans.as_slice(), [span3, span2, span1, handler_span] => {
        // Check that the DML handlers are hit, and that they inherit from the
        // handler span, which in turn inherits from the request
        assert_eq!(handler_span.name, "ingester write");
        assert_eq!(span1.name, "write_apply");
        assert_eq!(span2.name, "wal");
        assert_eq!(span3.name, "buffer");

        assert_eq!(handler_span.ctx.parent_span_id, Some(request_span.ctx.span_id));
        assert_eq!(span1.ctx.parent_span_id, Some(handler_span.ctx.span_id));
        assert_eq!(span2.ctx.parent_span_id, Some(handler_span.ctx.span_id));
        assert_eq!(span3.ctx.parent_span_id, Some(handler_span.ctx.span_id));
    })
}
