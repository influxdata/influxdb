use super::*;
use anyhow::Result;
use std::{fs, time::Duration};
use tempfile::TempDir;

fn write_plugin(plugin_dir: &TempDir, filename: &str, code: &str) -> Result<()> {
    fs::write(plugin_dir.path().join(filename), code)?;
    Ok(())
}

async fn assert_plugin_trigger_invocation_telemetry(
    server: &TestServer,
    database_name: &str,
    trigger_name: &str,
    plugin_name: &str,
    trigger_type: &str,
    min_invocation_count: u64,
) {
    let mut check_count = 0;
    loop {
        let snapshot = server.telemetry_snapshot().await;
        let invocations = snapshot["plugin_trigger_invocations"]
            .as_array()
            .expect("plugin_trigger_invocations should be an array");

        if invocations.iter().any(|invocation| {
            invocation["database_name"] == database_name
                && invocation["trigger_name"] == trigger_name
                && invocation["plugin_name"] == plugin_name
                && invocation["trigger_type"] == trigger_type
                && invocation["invocation_count"].as_u64().unwrap_or_default()
                    >= min_invocation_count
        }) {
            return;
        }

        check_count += 1;
        if check_count > 100 {
            panic!(
                "missing plugin trigger invocation telemetry for \
                 database_name={database_name}, trigger_name={trigger_name}, \
                 plugin_name={plugin_name}, trigger_type={trigger_type}, \
                 min_invocation_count={min_invocation_count}; \
                 snapshot={snapshot:#}",
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn telemetry_snapshot_returns_current_payload_in_test_mode() {
    let server = TestServer::configure().with_test_mode().spawn().await;

    let snapshot = server.telemetry_snapshot().await;

    assert_eq!(snapshot["product_type"], "Core");
    assert_eq!(snapshot["storage_engine_type"], "parquet");
    assert!(snapshot["installed_packages"].as_array().is_some());
    assert!(snapshot["plugin_trigger_invocations"].as_array().is_some());
}

#[tokio::test]
async fn telemetry_snapshot_is_unavailable_without_test_mode() {
    let server = TestServer::spawn().await;

    let response = server.telemetry_snapshot_response().await;

    assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);
}

#[test_log::test(tokio::test)]
async fn telemetry_records_wal_trigger_invocations() -> Result<()> {
    let plugin_dir = TempDir::new()?;
    write_plugin(
        &plugin_dir,
        "plugin.py",
        r#"
def process_writes(influxdb3_local, table_batches, args=None):
    influxdb3_local.info("wal telemetry")
"#,
    )?;

    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir.path().to_string_lossy())
        .with_test_mode()
        .spawn()
        .await;
    let db_name = "telemetry_wal_invocation";
    let trigger_name = "telemetry_wal_trigger";

    server.create_database(db_name).run()?;
    let result = server
        .create_trigger(db_name, trigger_name, "plugin.py", "all_tables")
        .run()?;
    assert!(result.contains("Trigger telemetry_wal_trigger created successfully"));

    server
        .write_lp_to_db(
            db_name,
            "cpu,host=a usage=0.9",
            influxdb3_client::Precision::Second,
        )
        .await?;

    assert_plugin_trigger_invocation_telemetry(
        &server,
        db_name,
        trigger_name,
        "plugin",
        "process_writes",
        1,
    )
    .await;

    Ok(())
}

#[test_log::test(tokio::test)]
async fn telemetry_records_request_trigger_invocations() -> Result<()> {
    let plugin_dir = TempDir::new()?;
    write_plugin(
        &plugin_dir,
        "telemetry_plugin.py",
        r#"
def process_request(influxdb3_local, query_parameters, request_headers, request_body, args=None):
    return {"status": "ok"}
"#,
    )?;

    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir.path().to_string_lossy())
        .with_test_mode()
        .spawn()
        .await;
    let db_name = "telemetry_request_invocation";
    let trigger_name = "telemetry_request_trigger";

    server.create_database(db_name).run()?;
    let result = server
        .create_trigger(
            db_name,
            trigger_name,
            "telemetry_plugin.py",
            "request:telemetry",
        )
        .run()?;
    assert!(result.contains("Trigger telemetry_request_trigger created successfully"));

    for _ in 0..2 {
        let response = server
            .http_client()
            .post(format!("{}/api/v3/engine/telemetry", server.client_addr()))
            .body("{}")
            .send()
            .await?;
        assert_eq!(response.status(), reqwest::StatusCode::OK);
    }

    assert_plugin_trigger_invocation_telemetry(
        &server,
        db_name,
        trigger_name,
        "telemetry_plugin",
        "process_request",
        2,
    )
    .await;

    Ok(())
}

#[test_log::test(tokio::test)]
async fn telemetry_records_schedule_trigger_invocations() -> Result<()> {
    let plugin_dir = TempDir::new()?;
    write_plugin(
        &plugin_dir,
        "plugin.py",
        r#"
def process_scheduled_call(influxdb3_local, schedule_time, args=None):
    influxdb3_local.info("schedule telemetry")
"#,
    )?;

    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir.path().to_string_lossy())
        .with_test_mode()
        .spawn()
        .await;
    let db_name = "telemetry_schedule_invocation";
    let trigger_name = "telemetry_schedule_trigger";

    server.create_database(db_name).run()?;
    let result = server
        .create_trigger(db_name, trigger_name, "plugin.py", "cron:* * * * * *")
        .run()?;
    assert!(result.contains("Trigger telemetry_schedule_trigger created successfully"));

    assert_plugin_trigger_invocation_telemetry(
        &server,
        db_name,
        trigger_name,
        "plugin",
        "process_scheduled_call",
        1,
    )
    .await;

    Ok(())
}
