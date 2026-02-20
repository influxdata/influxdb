use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime};

use influxdb3_client::Precision;
use reqwest::StatusCode;
use tempfile::TempDir;
use tokio::time::sleep;

use crate::server::{ConfigProvider, TestServer};

fn list_snapshot_files(object_store_root: &str, node_id: &str) -> Vec<PathBuf> {
    let dir = Path::new(object_store_root).join(node_id).join("snapshots");
    read_dir_sorted(&dir)
}

fn read_dir_sorted(dir: &Path) -> Vec<PathBuf> {
    if !dir.exists() {
        return vec![];
    }
    let mut paths = fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("failed to read dir {dir:?}: {e}"))
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.is_file())
        .collect::<Vec<_>>();
    paths.sort();
    paths
}

fn get_file_mtimes(object_store_root: &str, node_id: &str) -> Vec<(PathBuf, SystemTime)> {
    let root = Path::new(object_store_root).join(node_id);
    let mut results = vec![];
    if root.exists() {
        collect_parquet_json_files(&root, &mut results);
    }
    results.sort_by(|a, b| a.0.cmp(&b.0));
    results
}

fn collect_parquet_json_files(dir: &Path, results: &mut Vec<(PathBuf, SystemTime)>) {
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.filter_map(|e| e.ok()) {
            let path = entry.path();
            if path.is_dir() {
                collect_parquet_json_files(&path, results);
            } else if path.is_file() {
                let is_parquet_or_json = path
                    .extension()
                    .map(|ext| ext == "parquet" || ext == "json")
                    .unwrap_or(false);
                if !is_parquet_or_json {
                    continue;
                }
                if let Ok(meta) = fs::metadata(&path)
                    && let Ok(mtime) = meta.modified()
                {
                    results.push((path, mtime));
                }
            }
        }
    }
}

fn list_wal_files(object_store_root: &str, node_id: &str) -> Vec<PathBuf> {
    let wal_dir = Path::new(object_store_root).join(node_id).join("wal");
    read_dir_sorted(&wal_dir)
}

async fn wait_for_snapshot_count(
    object_store_root: &str,
    node_id: &str,
    min_count: usize,
    timeout: Duration,
) -> Vec<PathBuf> {
    let start = Instant::now();
    loop {
        let files = list_snapshot_files(object_store_root, node_id);
        if files.len() >= min_count {
            return files;
        }
        if start.elapsed() > timeout {
            panic!(
                "timed out waiting for {} snapshots, only found {} in {}/{}/snapshots/",
                min_count,
                files.len(),
                object_store_root,
                node_id,
            );
        }
        sleep(Duration::from_millis(100)).await;
    }
}

async fn kill_and_wait(server: &mut TestServer) {
    server.kill();
    let start = Instant::now();
    while !server.is_stopped() {
        if start.elapsed() > Duration::from_secs(5) {
            panic!("timed out waiting for server process to exit after kill");
        }
        sleep(Duration::from_millis(100)).await;
    }
}

const SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(30);
const TOTAL_WRITES: i64 = 10;
const WAL_FILES_TO_KEEP: &str = "1";

#[test_log::test(tokio::test)]
async fn test_lookback_lt_gen1_data_loss_on_restart() {
    let tmp_dir = TempDir::new().unwrap();
    let obj_store_path = tmp_dir.path().to_str().unwrap().to_string();
    let node_id = "lookback-dataloss";

    let mut server = TestServer::configure()
        .with_node_id(node_id)
        .with_object_store_dir(&obj_store_path)
        .with_gen1_duration("10m")
        .with_gen1_lookback_duration("5m")
        .with_snapshotted_wal_files_to_keep(WAL_FILES_TO_KEEP)
        .spawn()
        .await;

    for i in 1_i64..=TOTAL_WRITES {
        server
            .write_lp_to_db(
                "testdb",
                &format!("cpu,host=server{i} usage={i}.0 {}", i * 1_000_000_000),
                Precision::Nanosecond,
            )
            .await
            .expect("write lp");
        sleep(Duration::from_millis(50)).await;
    }

    wait_for_snapshot_count(&obj_store_path, node_id, 1, SNAPSHOT_TIMEOUT).await;
    sleep(Duration::from_secs(3)).await;

    let resp = server
        .api_v3_query_sql(&[
            ("db", "testdb"),
            ("q", "SELECT COUNT(*) as cnt FROM cpu"),
            ("format", "json"),
        ])
        .await;
    assert_eq!(StatusCode::OK, resp.status());
    let body = resp.text().await.unwrap();
    assert!(
        body.contains(&format!("\"cnt\":{TOTAL_WRITES}")),
        "expected {TOTAL_WRITES} rows before restart, got: {body}"
    );

    let wal_files = list_wal_files(&obj_store_path, node_id);
    let snap_files = list_snapshot_files(&obj_store_path, node_id);
    println!(
        "before restart: {} WAL files, {} snapshot files",
        wal_files.len(),
        snap_files.len()
    );
    assert!(
        wal_files.len() <= WAL_FILES_TO_KEEP.parse::<usize>().unwrap() + 2,
        "expected WAL files to be cleaned up to ~{WAL_FILES_TO_KEEP}, got {}",
        wal_files.len()
    );

    let files_before_restart = get_file_mtimes(&obj_store_path, node_id);

    kill_and_wait(&mut server).await;

    let mut server = TestServer::configure()
        .with_node_id(node_id)
        .with_object_store_dir(&obj_store_path)
        .with_gen1_duration("10m")
        .with_gen1_lookback_duration("5m")
        .with_snapshotted_wal_files_to_keep(WAL_FILES_TO_KEEP)
        .spawn()
        .await;

    wait_for_snapshot_count(&obj_store_path, node_id, 1, SNAPSHOT_TIMEOUT).await;
    sleep(Duration::from_secs(3)).await;

    let files_after_restart = get_file_mtimes(&obj_store_path, node_id);
    for (path, mtime_before) in &files_before_restart {
        if let Some((_, mtime_after)) = files_after_restart.iter().find(|(p, _)| p == path) {
            assert_eq!(
                mtime_before, mtime_after,
                "file was modified after restart: {path:?}"
            );
        }
    }

    kill_and_wait(&mut server).await;
}
