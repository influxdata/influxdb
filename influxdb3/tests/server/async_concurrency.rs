//! Tests for `--async-trigger-concurrency-limit`.

use std::num::NonZeroUsize;

use serde_json::Value;
use tempfile::TempDir;
use test_helpers::assert_contains;

use crate::server::{ConfigProvider, TestServer};

/// Request plugin that writes a marker row and polls for its peer's marker.
const LIMIT_MARKER_PLUGIN: &str = r#"
import time

def process_request(influxdb3_local, query_parameters, request_headers, request_body, args=None):
    my_id = query_parameters.get("id")
    peer_id = query_parameters.get("peer")

    line = LineBuilder("limit_markers").tag("request_id", my_id).int64_field("v", 1)
    influxdb3_local.write_sync(line, False)

    for _ in range(20):
        result = influxdb3_local.query(f"SELECT * FROM limit_markers WHERE request_id = '{peer_id}'")
        if len(result) > 0:
            return {"status": "ok", "saw_peer": True}
        time.sleep(0.1)

    return {"status": "timeout", "saw_peer": False}
"#;

fn plugin_in_temp_dir(code: &str) -> (TempDir, &'static str) {
    let temp_dir = TempDir::new().unwrap();
    std::fs::write(temp_dir.path().join("plugin.py"), code).unwrap();
    (temp_dir, "plugin.py")
}

/// With `--async-trigger-concurrency-limit 1`, an async request trigger runs
/// one invocation at a time.
///
/// Two concurrent requests each write a marker and poll for the other's
/// marker. Serialized execution means the first invocation times out without
/// seeing its peer (the peer is queued behind it), while the second sees the
/// first's marker immediately — so exactly one of the two observes its peer.
/// If the limit were not applied, both would run concurrently and both would
/// see each other.
#[test_log::test(tokio::test)]
async fn test_async_trigger_concurrency_limit_serializes_requests() {
    let (temp_dir, plugin_filename) = plugin_in_temp_dir(LIMIT_MARKER_PLUGIN);

    let server = TestServer::configure()
        .with_plugin_dir(temp_dir.path().to_string_lossy().into_owned())
        .with_async_trigger_concurrency_limit(NonZeroUsize::MIN)
        .spawn()
        .await;

    let db_name = "limit_test_db";
    server.create_database(db_name).run().unwrap();

    let result = server
        .create_trigger(db_name, "limit_trigger", plugin_filename, "request:limited")
        .run_asynchronous(true)
        .run()
        .unwrap();
    assert_contains!(&result, "Trigger limit_trigger created successfully");

    let client = server.http_client();
    let base_url = server.client_addr();

    let (r1, r2) = tokio::join!(
        client
            .get(format!("{base_url}/api/v3/engine/limited"))
            .query(&[("id", "1"), ("peer", "2")])
            .send(),
        client
            .get(format!("{base_url}/api/v3/engine/limited"))
            .query(&[("id", "2"), ("peer", "1")])
            .send(),
    );

    let body1: Value = r1.unwrap().json().await.unwrap();
    let body2: Value = r2.unwrap().json().await.unwrap();

    let saw_peer_count = [&body1, &body2]
        .iter()
        .filter(|body| body["saw_peer"] == true)
        .count();
    assert_eq!(
        saw_peer_count, 1,
        "limit 1: invocations must serialize, so exactly one request sees its \
         peer's marker. Got: {body1} and {body2}"
    );
}

/// Without a configured limit, an async request trigger runs more than eight
/// invocations concurrently (the previous hard-coded cap).
///
/// Ten concurrent requests each write a marker and poll until all ten markers
/// are visible. That only completes for every request if all ten invocations
/// are in flight at once.
#[test_log::test(tokio::test)]
async fn test_async_trigger_unlimited_exceeds_previous_cap() {
    let plugin_code = r#"
import time

def process_request(influxdb3_local, query_parameters, request_headers, request_body, args=None):
    my_id = query_parameters.get("id")

    line = LineBuilder("unlimited_markers").tag("request_id", my_id).int64_field("v", 1)
    influxdb3_local.write_sync(line, False)

    for _ in range(50):
        result = influxdb3_local.query("SELECT request_id FROM unlimited_markers")
        if len(result) >= 10:
            return {"status": "ok", "saw_all": True}
        time.sleep(0.2)

    return {"status": "timeout", "saw_all": False}
"#;

    let (temp_dir, plugin_filename) = plugin_in_temp_dir(plugin_code);

    let server = TestServer::configure()
        .with_plugin_dir(temp_dir.path().to_string_lossy().into_owned())
        .spawn()
        .await;

    let db_name = "unlimited_test_db";
    server.create_database(db_name).run().unwrap();

    let result = server
        .create_trigger(
            db_name,
            "unlimited_trigger",
            plugin_filename,
            "request:unlimited",
        )
        .run_asynchronous(true)
        .run()
        .unwrap();
    assert_contains!(&result, "Trigger unlimited_trigger created successfully");

    let client = server.http_client();
    let base_url = server.client_addr();

    let mut handles = Vec::new();
    for id in 1..=10 {
        let client = client.clone();
        let url = format!("{base_url}/api/v3/engine/unlimited");
        handles.push(tokio::spawn(async move {
            let resp = client
                .get(&url)
                .query(&[("id", id.to_string())])
                .send()
                .await
                .unwrap();
            let body: Value = resp.json().await.unwrap();
            body
        }));
    }

    for handle in handles {
        let body = handle.await.unwrap();
        assert_eq!(
            body["saw_all"], true,
            "unlimited default: all ten invocations must run concurrently \
             (more than the previous cap of eight). Got: {body}"
        );
    }
}
