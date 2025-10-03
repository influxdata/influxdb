use crate::server::{ConfigProvider, TestServer};
use anyhow::Result;
use std::fs;
use tempfile::TempDir;

const PLUGIN_ALPHA: &str = r#"
def process_writes(influxdb3_local, table_batches, args=None):
    influxdb3_local.info("Plugin Alpha processing")
"#;

const PLUGIN_BETA: &str = r#"
def process_writes(influxdb3_local, table_batches, args=None):
    influxdb3_local.info("Plugin Beta processing")
"#;

const PLUGIN_GAMMA: &str = r#"
def process_writes(influxdb3_local, table_batches, args=None):
    influxdb3_local.info("Plugin Gamma processing")
"#;

#[test_log::test(tokio::test)]
async fn test_system_plugins_table() -> Result<()> {
    // Setup: Create temp directory with plugin files
    let plugin_dir = TempDir::new()?;
    let plugin_dir_path = plugin_dir.path();

    fs::write(plugin_dir_path.join("plugin_alpha.py"), PLUGIN_ALPHA)?;
    fs::write(plugin_dir_path.join("plugin_beta.py"), PLUGIN_BETA)?;
    fs::write(plugin_dir_path.join("plugin_gamma.py"), PLUGIN_GAMMA)?;

    // Start server with plugin directory
    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir_path.to_str().unwrap())
        .spawn()
        .await;

    // Create database
    server.create_database("test_db").run()?;

    // Create table
    server
        .create_table("test_db", "cpu")
        .with_fields([("usage", "float64")])
        .run()?;

    // Create triggers for each plugin
    server
        .create_trigger("test_db", "trigger_alpha", "plugin_alpha.py", "all_tables")
        .run()?;

    server
        .create_trigger("test_db", "trigger_beta", "plugin_beta.py", "table:cpu")
        .run()?;

    server
        .create_trigger("test_db", "trigger_gamma", "plugin_gamma.py", "all_tables")
        .run()?;

    // Query system.plugin_files table
    let result = server
        .query_sql("_internal")
        .with_sql("SELECT * FROM system.plugin_files ORDER BY plugin_name")
        .run()?;

    let parsed = result.as_array().expect("Expected array result");

    // Verify we have exactly 3 plugins
    assert_eq!(parsed.len(), 3, "Should have three plugins");

    // Verify first plugin (trigger_alpha)
    assert_eq!(parsed[0]["plugin_name"], "trigger_alpha");
    assert_eq!(parsed[0]["file_name"], "plugin_alpha.py");
    assert!(
        parsed[0]["file_path"]
            .as_str()
            .unwrap()
            .contains("plugin_alpha.py")
    );
    assert!(parsed[0]["size_bytes"].as_i64().unwrap() > 0);
    assert!(parsed[0]["last_modified"].as_i64().unwrap() > 0);

    // Verify second plugin (trigger_beta)
    assert_eq!(parsed[1]["plugin_name"], "trigger_beta");
    assert_eq!(parsed[1]["file_name"], "plugin_beta.py");
    assert!(
        parsed[1]["file_path"]
            .as_str()
            .unwrap()
            .contains("plugin_beta.py")
    );
    assert!(parsed[1]["size_bytes"].as_i64().unwrap() > 0);
    assert!(parsed[1]["last_modified"].as_i64().unwrap() > 0);

    // Verify third plugin (trigger_gamma)
    assert_eq!(parsed[2]["plugin_name"], "trigger_gamma");
    assert_eq!(parsed[2]["file_name"], "plugin_gamma.py");
    assert!(
        parsed[2]["file_path"]
            .as_str()
            .unwrap()
            .contains("plugin_gamma.py")
    );
    assert!(parsed[2]["size_bytes"].as_i64().unwrap() > 0);
    assert!(parsed[2]["last_modified"].as_i64().unwrap() > 0);

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_system_plugins_table_empty() -> Result<()> {
    // Start server without any plugins
    let server = TestServer::spawn().await;

    // Create database
    server.create_database("test_db").run()?;

    // Query system.plugin_files table when no triggers exist
    let result = server
        .query_sql("_internal")
        .with_sql("SELECT * FROM system.plugin_files")
        .run()?;

    let parsed = result.as_array().expect("Expected array result");

    // Should be empty
    assert_eq!(
        parsed.len(),
        0,
        "Should have no plugins when no triggers exist"
    );

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_system_plugins_table_after_delete() -> Result<()> {
    // Setup: Create temp directory with a plugin file
    let plugin_dir = TempDir::new()?;
    let plugin_dir_path = plugin_dir.path();

    fs::write(plugin_dir_path.join("test_plugin.py"), PLUGIN_ALPHA)?;

    // Start server with plugin directory
    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir_path.to_str().unwrap())
        .spawn()
        .await;

    // Create database and table
    server.create_database("test_db").run()?;
    server
        .create_table("test_db", "cpu")
        .with_fields([("usage", "float64")])
        .run()?;

    // Create a trigger
    server
        .create_trigger("test_db", "test_trigger", "test_plugin.py", "all_tables")
        .run()?;

    // Verify plugin appears in system table
    let result = server
        .query_sql("_internal")
        .with_sql("SELECT * FROM system.plugin_files")
        .run()?;

    let parsed = result.as_array().expect("Expected array result");
    assert_eq!(parsed.len(), 1, "Should have one plugin");
    assert_eq!(parsed[0]["plugin_name"], "test_trigger");

    // Delete the trigger
    server
        .delete_trigger("test_db", "test_trigger")
        .force(true)
        .run()?;

    // Verify plugin is removed from system table
    let result_after = server
        .query_sql("_internal")
        .with_sql("SELECT * FROM system.plugin_files")
        .run()?;

    let parsed_after = result_after.as_array().expect("Expected array result");
    assert_eq!(
        parsed_after.len(),
        0,
        "Should have no plugins after trigger deletion"
    );

    Ok(())
}
