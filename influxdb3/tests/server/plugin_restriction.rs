use crate::server::{ConfigProvider, TestServer};
use anyhow::Result;
use tempfile::TempDir;

struct PluginTest {
    plugin_dir: TempDir,
    plugin_filename: &'static str,
}

impl PluginTest {
    fn new() -> Result<Self> {
        let plugin_dir = TempDir::new()?;
        let plugin_path = plugin_dir.path().join("test_plugin.py");
        std::fs::write(
            &plugin_path,
            b"def process_scheduled_call(influxdb3_local, schedule_time, args=None): pass\n",
        )?;
        Ok(Self {
            plugin_dir,
            plugin_filename: "test_plugin.py",
        })
    }

    fn plugin_dir_str(&self) -> String {
        self.plugin_dir.path().to_string_lossy().into_owned()
    }
}

#[test_log::test(tokio::test)]
async fn test_restrict_plugin_triggers_to_schedule_rejects_other_types() -> Result<()> {
    let setup = PluginTest::new()?;

    let server = TestServer::configure()
        .with_plugin_dir(setup.plugin_dir_str())
        .with_restrict_plugin_triggers_to(["schedule"])
        .spawn()
        .await;

    server.create_database("testdb").run()?;

    // WAL trigger (all_tables) should be rejected
    let err = server
        .create_trigger("testdb", "wal_trigger", setup.plugin_filename, "all_tables")
        .run()
        .expect_err("WAL trigger should be rejected when only schedule is allowed");
    assert!(
        err.to_string()
            .contains("server is configured to reject WalRows"),
        "Expected restriction error, got: {err}"
    );

    // WAL trigger (table:X) should also be rejected
    let err = server
        .create_trigger(
            "testdb",
            "wal_table_trigger",
            setup.plugin_filename,
            "table:cpu",
        )
        .run()
        .expect_err("Table WAL trigger should be rejected when only schedule is allowed");
    assert!(
        err.to_string()
            .contains("server is configured to reject WalRows"),
        "Expected restriction error, got: {err}"
    );

    // Request trigger should be rejected
    let err = server
        .create_trigger(
            "testdb",
            "request_trigger",
            setup.plugin_filename,
            "request:mypath",
        )
        .run()
        .expect_err("Request trigger should be rejected when only schedule is allowed");
    assert!(
        err.to_string()
            .contains("server is configured to reject Request"),
        "Expected restriction error, got: {err}"
    );

    // Schedule trigger (every:) should be allowed
    server
        .create_trigger(
            "testdb",
            "every_trigger",
            setup.plugin_filename,
            "every:10s",
        )
        .disabled(true)
        .run()
        .expect("every:10s schedule trigger should be allowed");

    // Schedule trigger (cron:) should also be allowed
    server
        .create_trigger(
            "testdb",
            "cron_trigger",
            setup.plugin_filename,
            "cron:* * * * * *",
        )
        .disabled(true)
        .run()
        .expect("cron schedule trigger should be allowed");

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_restrict_plugin_triggers_to_wal_rejects_other_types() -> Result<()> {
    let setup = PluginTest::new()?;

    let server = TestServer::configure()
        .with_plugin_dir(setup.plugin_dir_str())
        .with_restrict_plugin_triggers_to(["wal"])
        .spawn()
        .await;

    server.create_database("testdb").run()?;

    // Schedule trigger should be rejected
    let err = server
        .create_trigger(
            "testdb",
            "schedule_trigger",
            setup.plugin_filename,
            "every:10s",
        )
        .run()
        .expect_err("Schedule trigger should be rejected when only WAL is allowed");
    assert!(
        err.to_string()
            .contains("server is configured to reject Schedule"),
        "Expected restriction error, got: {err}"
    );

    // Request trigger should be rejected
    let err = server
        .create_trigger(
            "testdb",
            "request_trigger",
            setup.plugin_filename,
            "request:mypath",
        )
        .run()
        .expect_err("Request trigger should be rejected when only WAL is allowed");
    assert!(
        err.to_string()
            .contains("server is configured to reject Request"),
        "Expected restriction error, got: {err}"
    );

    // WAL trigger should be allowed
    server
        .create_trigger("testdb", "wal_trigger", setup.plugin_filename, "all_tables")
        .disabled(true)
        .run()
        .expect("WAL trigger should be allowed");

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_no_plugin_trigger_restriction_allows_all_types() -> Result<()> {
    let setup = PluginTest::new()?;

    let server = TestServer::configure()
        .with_plugin_dir(setup.plugin_dir_str())
        .spawn()
        .await;

    server.create_database("testdb").run()?;

    // All trigger types should be allowed with no restriction
    server
        .create_trigger("testdb", "wal_trigger", setup.plugin_filename, "all_tables")
        .disabled(true)
        .run()
        .expect("WAL trigger should be allowed with no restriction");

    server
        .create_trigger(
            "testdb",
            "schedule_trigger",
            setup.plugin_filename,
            "every:10s",
        )
        .disabled(true)
        .run()
        .expect("Schedule trigger should be allowed with no restriction");

    server
        .create_trigger(
            "testdb",
            "request_trigger",
            setup.plugin_filename,
            "request:mypath",
        )
        .disabled(true)
        .run()
        .expect("Request trigger should be allowed with no restriction");

    Ok(())
}
