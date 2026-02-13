//! End-to-end tests for Gen1 file cleanup after compaction.
//!
//! These tests verify that Gen1 parquet files are correctly deleted after
//! the compactor has processed them into Gen2+ generations.

use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::server::enterprise::tmp_dir;
use crate::server::{ConfigProvider, TestServer};
use futures::TryStreamExt;
use influxdb3_enterprise_clap_blocks::serve::BufferMode;
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use reqwest::StatusCode;

/// Timestamp for write `i` distributed across Gen1 windows of `gen1_duration_s` seconds.
fn write_timestamp_ns_multi(i: i64, writes_per_window: i64, gen1_duration_s: i64) -> i64 {
    let window = (i - 1) / writes_per_window;
    let offset = (i - 1) % writes_per_window + 1;
    (window * gen1_duration_s + offset) * 1_000_000_000
}

/// Default 3-window layout: 12 writes, 4 per window, 600s apart.
/// Creates 2 Gen1 blocks in one Gen2 window + a "newer" block for the compaction planner.
fn write_timestamp_ns(i: i64) -> i64 {
    write_timestamp_ns_multi(i, 4, 600)
}

/// List parquet files in the object store for a given node.
async fn list_parquet_files(tmp_dir: &str, node_id: &str) -> Vec<String> {
    let object_store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(tmp_dir).expect("create local fs"));

    let prefix = object_store::path::Path::from(format!("{}/dbs", node_id));
    let files: Vec<_> = object_store
        .list(Some(&prefix))
        .try_collect()
        .await
        .unwrap_or_default();

    files
        .into_iter()
        .map(|m| m.location.to_string())
        .filter(|p| p.ends_with(".parquet"))
        .collect()
}

async fn spawn_compactor(
    cluster_id: &str,
    obj_store_path: &str,
    check_interval: &str,
) -> TestServer {
    TestServer::configure_enterprise()
        .with_cluster_id(cluster_id)
        .with_node_id("compactor")
        .with_mode(vec![BufferMode::Compact])
        .with_object_store(obj_store_path)
        .with_catalog_sync_interval("100ms")
        .with_compaction_check_interval(check_interval)
        .spawn()
        .await
}

async fn spawn_ingester(
    cluster_id: &str,
    obj_store_path: &str,
    modes: Vec<BufferMode>,
) -> TestServer {
    TestServer::configure_enterprise()
        .with_cluster_id(cluster_id)
        .with_node_id("ingester")
        .with_mode(modes)
        .with_object_store(obj_store_path)
        .with_catalog_sync_interval("100ms")
        .with_compaction_check_interval("1s")
        .spawn()
        .await
}

async fn spawn_querier(cluster_id: &str, obj_store_path: &str) -> TestServer {
    TestServer::configure_enterprise()
        .with_cluster_id(cluster_id)
        .with_node_id("querier")
        .with_mode(vec![BufferMode::Query])
        .with_object_store(obj_store_path)
        .with_catalog_sync_interval("100ms")
        .with_compaction_check_interval("1s")
        .spawn()
        .await
}

/// Write rows across tables, then sleep 2s for snapshots to persist.
async fn write_rows(
    server: &TestServer,
    db_name: &str,
    tables: &[&str],
    count: i64,
    ts_fn: impl Fn(i64) -> i64,
) {
    for i in 1..=count {
        let ts = ts_fn(i);
        for table in tables {
            server
                .write_lp_to_db(
                    db_name,
                    &format!("{table},host=server{i} value={i}.0 {ts}"),
                    influxdb3_client::Precision::Nanosecond,
                )
                .await
                .expect("write to db");
        }
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
}

/// Query a table and assert it has exactly `expected` rows.
async fn assert_row_count(server: &TestServer, db_name: &str, table: &str, expected: i64) {
    let resp = server
        .api_v3_query_sql(&[
            ("db", db_name),
            ("q", &format!("SELECT COUNT(*) as cnt FROM {table}")),
            ("format", "json"),
        ])
        .await;
    assert_eq!(StatusCode::OK, resp.status());
    let body = resp.text().await.unwrap();
    assert!(
        body.contains(&format!("\"cnt\":{expected}")),
        "Expected {expected} rows in {table} but got: {body}",
    );
}

/// Retry cleanup until file count drops below `initial_count`.
async fn wait_for_gen1_cleanup(
    server: &TestServer,
    obj_store_path: &str,
    node_id: &str,
    initial_count: usize,
    timeout: Duration,
) -> (usize, usize) {
    let start = Instant::now();
    loop {
        let resp = server.trigger_gen1_cleanup().await;
        let status = resp.status();
        if !status.is_success() && status != StatusCode::METHOD_NOT_ALLOWED {
            panic!("Unexpected cleanup status: {:?}", status);
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
        let current = list_parquet_files(obj_store_path, node_id).await;
        if current.len() < initial_count {
            return (initial_count, current.len());
        }

        if start.elapsed() > timeout {
            panic!(
                "Timed out waiting for Gen1 cleanup: files still at {} (started at {})",
                current.len(),
                initial_count
            );
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// Verify Gen1 files are deleted after compaction and cleanup,
/// and that data remains queryable from gen2 compacted files.
#[test_log::test(tokio::test)]
async fn test_gen1_cleanup_basic_flow() {
    let obj_store_path = tmp_dir();
    let _compactor = spawn_compactor("cleanup-test", &obj_store_path, "10s").await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    let ingester = spawn_ingester(
        "cleanup-test",
        &obj_store_path,
        vec![BufferMode::Ingest, BufferMode::Query],
    )
    .await;
    write_rows(&ingester, "testdb", &["cpu"], 12, write_timestamp_ns).await;

    let gen1_before = list_parquet_files(&obj_store_path, "ingester").await;
    assert!(
        gen1_before.len() >= 3,
        "Should have at least 3 Gen1 parquet files after snapshots, got {}",
        gen1_before.len()
    );

    // Compaction runs on its own schedule (10s interval). The cleanup loop
    // retries until the compactor has processed files and cleanup succeeds.
    wait_for_gen1_cleanup(
        &ingester,
        &obj_store_path,
        "ingester",
        gen1_before.len(),
        Duration::from_secs(45),
    )
    .await;

    assert_row_count(&ingester, "testdb", "cpu", 12).await;
}

/// Cleanup returns 405 and files are NOT deleted when no compacted data is available.
#[test_log::test(tokio::test)]
async fn test_gen1_cleanup_no_files_eligible() {
    let obj_store_path = tmp_dir();
    let db_name = "testdb";

    // No compactor — cleanup handler not created.
    let ingester = TestServer::configure_enterprise()
        .with_cluster_id("cleanup-test-no-compact")
        .with_node_id("ingester")
        .with_mode(vec![BufferMode::Ingest, BufferMode::Query])
        .with_object_store(&obj_store_path)
        .with_catalog_sync_interval("10ms")
        .spawn()
        .await;

    for i in 1i64..=5 {
        ingester
            .write_lp_to_db(
                db_name,
                &format!(
                    "cpu,host=server{} value={}.0 {}",
                    i,
                    i,
                    i * 1_000_000_000i64
                ),
                influxdb3_client::Precision::Nanosecond,
            )
            .await
            .expect("write to db");
    }

    tokio::time::sleep(Duration::from_millis(500)).await;

    let gen1_before = list_parquet_files(&obj_store_path, "ingester").await;

    let resp = ingester.trigger_gen1_cleanup().await;
    assert_eq!(
        StatusCode::METHOD_NOT_ALLOWED,
        resp.status(),
        "Cleanup should not be available without compactor"
    );

    let gen1_after = list_parquet_files(&obj_store_path, "ingester").await;
    assert_eq!(
        gen1_before.len(),
        gen1_after.len(),
        "Gen1 files should not be deleted when cleanup is not available"
    );
}

/// Large backlog: 60 writes across 10 Gen1 windows.
#[test_log::test(tokio::test)]
async fn test_gen1_cleanup_large_backlog() {
    let obj_store_path = tmp_dir();
    // 15s compaction interval so 60 writes complete before first cycle.
    let _compactor = spawn_compactor("backlog-test", &obj_store_path, "15s").await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    let ingester = spawn_ingester(
        "backlog-test",
        &obj_store_path,
        vec![BufferMode::Ingest, BufferMode::Query],
    )
    .await;
    write_rows(&ingester, "testdb", &["cpu"], 60, |i| {
        write_timestamp_ns_multi(i, 6, 600)
    })
    .await;

    let gen1_before = list_parquet_files(&obj_store_path, "ingester").await;
    assert!(
        gen1_before.len() >= 10,
        "Expected >= 10 Gen1 files, got {}",
        gen1_before.len()
    );

    wait_for_gen1_cleanup(
        &ingester,
        &obj_store_path,
        "ingester",
        gen1_before.len(),
        Duration::from_secs(50),
    )
    .await;

    assert_row_count(&ingester, "testdb", "cpu", 60).await;
}

/// A separate query-only node can read data after gen1 files are deleted.
/// Validates that gen2 compacted files are sufficient for a fresh querier.
#[test_log::test(tokio::test)]
async fn test_gen1_cleanup_querier_reads_after_cleanup() {
    let obj_store_path = tmp_dir();
    let _compactor = spawn_compactor("cleanup-querier-test", &obj_store_path, "10s").await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    // Ingest-only — no Query mode so the assertion below runs exclusively on the querier.
    let ingester = spawn_ingester(
        "cleanup-querier-test",
        &obj_store_path,
        vec![BufferMode::Ingest],
    )
    .await;
    write_rows(&ingester, "testdb", &["cpu"], 12, write_timestamp_ns).await;

    let gen1_before = list_parquet_files(&obj_store_path, "ingester").await;
    assert!(
        gen1_before.len() >= 3,
        "Should have at least 3 Gen1 parquet files, got {}",
        gen1_before.len()
    );

    wait_for_gen1_cleanup(
        &ingester,
        &obj_store_path,
        "ingester",
        gen1_before.len(),
        Duration::from_secs(45),
    )
    .await;

    // Spawn a fresh query-only node AFTER gen1 files are deleted.
    let querier = spawn_querier("cleanup-querier-test", &obj_store_path).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_row_count(&querier, "testdb", "cpu", 12).await;
}

/// Cleanup across 3 tables (cpu, mem, disk) — all remain queryable afterwards.
#[test_log::test(tokio::test)]
async fn test_gen1_cleanup_multiple_tables() {
    let obj_store_path = tmp_dir();
    let _compactor = spawn_compactor("multi-table-test", &obj_store_path, "10s").await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    let ingester = spawn_ingester(
        "multi-table-test",
        &obj_store_path,
        vec![BufferMode::Ingest, BufferMode::Query],
    )
    .await;
    write_rows(
        &ingester,
        "testdb",
        &["cpu", "mem", "disk"],
        12,
        write_timestamp_ns,
    )
    .await;

    let gen1_before = list_parquet_files(&obj_store_path, "ingester").await;
    assert!(
        gen1_before.len() >= 6,
        "Expected >= 6 Gen1 files across 3 tables, got {}",
        gen1_before.len()
    );

    wait_for_gen1_cleanup(
        &ingester,
        &obj_store_path,
        "ingester",
        gen1_before.len(),
        Duration::from_secs(45),
    )
    .await;

    for table in &["cpu", "mem", "disk"] {
        assert_row_count(&ingester, "testdb", table, 12).await;
    }
}
