#![cfg(unix)]

use std::time::{Duration, Instant};

use tempfile::TempDir;

use crate::server::{ConfigProvider, TestServer};

/// Request plugin that blocks its thread. The marker file tells the test the
/// plugin is running.
const SLEEPING_PLUGIN: &str = r#"
import time

def process_request(influxdb3_local, query_parameters, request_headers, request_body, args=None):
    open(args["marker"], "w").close()
    time.sleep(60)
    return {"status": "done"}
"#;

/// A plugin stuck on a blocking thread must not keep the process alive after
/// SIGTERM.
#[test_log::test(tokio::test)]
async fn sigterm_exits_while_plugin_sleeps() {
    let plugin_dir = TempDir::new().unwrap();
    std::fs::write(plugin_dir.path().join("plugin.py"), SLEEPING_PLUGIN).unwrap();
    let marker_dir = TempDir::new().unwrap();
    let marker = marker_dir.path().join("started");

    let mut server = TestServer::configure()
        .with_plugin_dir(plugin_dir.path().to_string_lossy().into_owned())
        .with_capture_logs()
        // Nothing cancels the plugin at shutdown (#6828), so the HTTP drain waits for the in-flight
        // request until this timeout.
        .with_env_var("INFLUXDB3_SHUTDOWN_TIMEOUT", "1s")
        .spawn()
        .await;

    server.create_database("shutdown_db").run().unwrap();
    server
        .create_trigger("shutdown_db", "sleeper", "plugin.py", "request:sleeper")
        .add_trigger_argument(format!("marker={}", marker.display()))
        .run()
        .unwrap();

    let request = tokio::spawn(
        server
            .http_client()
            .get(format!("{}/api/v3/engine/sleeper", server.client_addr()))
            .send(),
    );

    let started_by = Instant::now() + Duration::from_secs(10);
    while !marker.exists() {
        assert!(
            Instant::now() < started_by,
            "plugin did not start; last log lines:\n{}",
            server.get_logs(Some(80)).unwrap_or_default()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let signalled = Instant::now();
    server.send_sigterm();
    let status = server.wait_until_stopped().await;
    let elapsed = signalled.elapsed();
    assert!(
        elapsed < Duration::from_secs(15),
        "server took {elapsed:?} to exit after SIGTERM"
    );

    assert!(
        status.success(),
        "server exited with {status} after SIGTERM"
    );

    request.abort();
}
