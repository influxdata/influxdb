use crate::server::{ConfigProvider, TestServer};
use anyhow::Result;
use std::fs;
use std::path::Path;
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

    server.create_database("test_db").run()?;

    server
        .create_table("test_db", "cpu")
        .with_fields([("usage", "float64")])
        .run()?;

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
    // Create temp directory with a plugin file
    let plugin_dir = TempDir::new()?;
    let plugin_dir_path = plugin_dir.path();

    fs::write(plugin_dir_path.join("test_plugin.py"), PLUGIN_ALPHA)?;

    // Start server with plugin directory
    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir_path.to_str().unwrap())
        .spawn()
        .await;

    server.create_database("test_db").run()?;
    server
        .create_table("test_db", "cpu")
        .with_fields([("usage", "float64")])
        .run()?;

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

#[test_log::test(tokio::test)]
async fn test_show_plugins_single_file() -> Result<()> {
    let plugin_dir = TempDir::new()?;
    let plugin_dir_path = plugin_dir.path();

    fs::write(plugin_dir_path.join("plugin_alpha.py"), PLUGIN_ALPHA)?;
    fs::write(plugin_dir_path.join("plugin_beta.py"), PLUGIN_BETA)?;

    // Start server with plugin directory
    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir_path.to_str().unwrap())
        .spawn()
        .await;

    server.create_database("test_db").run()?;

    // Create triggers with different trigger specifications
    server
        .create_trigger("test_db", "trigger_alpha", "plugin_alpha.py", "all_tables")
        .run()?;

    server
        .create_trigger("test_db", "trigger_beta", "plugin_beta.py", "every:5s")
        .run()?;

    // Run show plugins command with JSON format
    let output = server.show_plugins().with_format("json").run()?;

    let parsed: serde_json::Value = serde_json::from_str(&output)?;
    let triggers = parsed.as_array().expect("Expected array result");

    // Verify we have exactly 2 plugins
    assert_eq!(triggers.len(), 2, "Should have two plugins");

    // Verify first plugin (trigger_alpha)
    assert_eq!(triggers[0]["plugin_name"], "trigger_alpha");
    assert_eq!(triggers[0]["file_name"], "plugin_alpha.py");
    assert!(
        triggers[0]["file_path"]
            .as_str()
            .unwrap()
            .contains("plugin_alpha.py"),
        "file_path should contain plugin_alpha.py"
    );
    assert!(triggers[0]["size_bytes"].as_i64().unwrap() > 0);
    assert!(triggers[0]["last_modified"].as_i64().unwrap() > 0);

    // Verify second plugin (trigger_beta)
    assert_eq!(triggers[1]["plugin_name"], "trigger_beta");
    assert_eq!(triggers[1]["file_name"], "plugin_beta.py");
    assert!(
        triggers[1]["file_path"]
            .as_str()
            .unwrap()
            .contains("plugin_beta.py"),
        "file_path should contain plugin_beta.py"
    );
    assert!(triggers[1]["size_bytes"].as_i64().unwrap() > 0);
    assert!(triggers[1]["last_modified"].as_i64().unwrap() > 0);

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_show_plugins_multiple_databases() -> Result<()> {
    let plugin_dir = TempDir::new()?;
    let plugin_dir_path = plugin_dir.path();

    fs::write(plugin_dir_path.join("plugin_alpha.py"), PLUGIN_ALPHA)?;
    fs::write(plugin_dir_path.join("plugin_beta.py"), PLUGIN_BETA)?;

    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir_path.to_str().unwrap())
        .spawn()
        .await;

    server.create_database("test_db").run()?;
    server.create_database("test_db2").run()?;

    server
        .create_trigger("test_db", "trigger_alpha", "plugin_alpha.py", "all_tables")
        .run()?;

    server
        .create_trigger(
            "test_db2",
            "trigger_beta",
            "plugin_beta.py",
            "table:measurements",
        )
        .run()?;

    // Show plugins - should see all plugins from all databases
    let output = server.show_plugins().with_format("json").run()?;

    let parsed: serde_json::Value = serde_json::from_str(&output)?;
    let plugins = parsed.as_array().expect("Expected array result");

    assert_eq!(plugins.len(), 2, "Should have two plugins total");

    // Verify both plugins are present
    let plugin_names: Vec<&str> = plugins
        .iter()
        .map(|p| p["plugin_name"].as_str().unwrap())
        .collect();
    assert!(
        plugin_names.contains(&"trigger_alpha"),
        "Should contain trigger_alpha"
    );
    assert!(
        plugin_names.contains(&"trigger_beta"),
        "Should contain trigger_beta"
    );

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_show_plugins_empty() -> Result<()> {
    // Start server without any plugins
    let server = TestServer::spawn().await;

    server.create_database("test_db").run()?;

    // Run show plugins command when no triggers exist
    let output = server.show_plugins().with_format("json").run()?;

    let parsed: serde_json::Value = serde_json::from_str(&output)?;
    let triggers = parsed.as_array().expect("Expected array result");

    assert_eq!(
        triggers.len(),
        0,
        "Should have no triggers when no triggers exist"
    );

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_show_plugins_pretty_format() -> Result<()> {
    let plugin_dir = TempDir::new()?;
    let plugin_dir_path = plugin_dir.path();

    fs::write(plugin_dir_path.join("plugin_alpha.py"), PLUGIN_ALPHA)?;
    fs::write(plugin_dir_path.join("plugin_beta.py"), PLUGIN_BETA)?;

    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir_path.to_str().unwrap())
        .spawn()
        .await;

    server.create_database("test_db").run()?;

    // Create triggers with different trigger specifications
    server
        .create_trigger("test_db", "trigger_alpha", "plugin_alpha.py", "all_tables")
        .run()?;

    server
        .create_trigger("test_db", "trigger_beta", "plugin_beta.py", "every:5s")
        .run()?;

    // Run show plugins command with pretty format
    let output = server.show_plugins().run()?;

    assert!(
        output.contains("plugin_name"),
        "Should have plugin_name column"
    );
    assert!(output.contains("file_name"), "Should have file_name column");
    assert!(output.contains("file_path"), "Should have file_path column");
    assert!(
        output.contains("size_bytes"),
        "Should have size_bytes column"
    );
    assert!(
        output.contains("last_modified"),
        "Should have last_modified column"
    );

    // Verify the data rows
    assert!(
        output.contains("trigger_alpha"),
        "Should contain trigger_alpha"
    );
    assert!(
        output.contains("trigger_beta"),
        "Should contain trigger_beta"
    );
    assert!(
        output.contains("plugin_alpha.py"),
        "Should contain plugin_alpha.py"
    );
    assert!(
        output.contains("plugin_beta.py"),
        "Should contain plugin_beta.py"
    );

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_show_plugins_csv_format() -> Result<()> {
    let plugin_dir = TempDir::new()?;
    let plugin_dir_path = plugin_dir.path();

    fs::write(plugin_dir_path.join("plugin_alpha.py"), PLUGIN_ALPHA)?;
    fs::write(plugin_dir_path.join("plugin_beta.py"), PLUGIN_BETA)?;

    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir_path.to_str().unwrap())
        .spawn()
        .await;

    server.create_database("test_db").run()?;

    // Create triggers with different trigger specifications
    server
        .create_trigger("test_db", "trigger_alpha", "plugin_alpha.py", "all_tables")
        .run()?;

    server
        .create_trigger("test_db", "trigger_beta", "plugin_beta.py", "every:5s")
        .run()?;

    let output = server.show_plugins().with_format("csv").run()?;

    // Parse CSV output
    let lines: Vec<&str> = output.lines().collect();

    // Verify CSV header
    assert!(lines.len() >= 3, "Should have header + 2 data rows");
    assert_eq!(
        lines[0], "plugin_name,file_name,file_path,size_bytes,last_modified",
        "CSV header should match expected columns"
    );

    // Verify trigger_alpha is in the output
    let alpha_line = lines.iter().find(|line| line.contains("trigger_alpha"));
    assert!(alpha_line.is_some(), "Should contain trigger_alpha in CSV");
    let alpha_line = alpha_line.unwrap();
    assert!(
        alpha_line.contains("plugin_alpha.py"),
        "Should contain plugin_alpha.py"
    );

    // Verify trigger_beta is in the output
    let beta_line = lines.iter().find(|line| line.contains("trigger_beta"));
    assert!(beta_line.is_some(), "Should contain trigger_beta in CSV");
    let beta_line = beta_line.unwrap();
    assert!(
        beta_line.contains("plugin_beta.py"),
        "Should contain plugin_beta.py"
    );

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_show_plugins_pretty_format_empty() -> Result<()> {
    // Start server without any plugins
    let server = TestServer::spawn().await;

    server.create_database("test_db").run()?;

    // Run show plugins command with pretty format when no triggers exist
    let output = server.show_plugins().run()?;

    assert!(
        !output.contains("trigger_alpha"),
        "Should not contain any trigger data"
    );
    assert!(
        !output.contains("trigger_beta"),
        "Should not contain any trigger data"
    );

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_show_plugins_csv_format_empty() -> Result<()> {
    // Start server without any plugins
    let server = TestServer::spawn().await;

    server.create_database("test_db").run()?;

    // Run show plugins command with CSV format when no triggers exist
    let output = server.show_plugins().with_format("csv").run()?;

    assert!(
        !output.contains("trigger_alpha"),
        "Should not contain any trigger data"
    );
    assert!(
        !output.contains("trigger_beta"),
        "Should not contain any trigger data"
    );

    // If output is present, verify it's valid CSV format
    if !output.trim().is_empty() {
        let lines: Vec<&str> = output.lines().collect();
        // If there are lines, the first should be the header
        if !lines.is_empty() {
            assert_eq!(
                lines[0], "plugin_name,file_name,file_path,size_bytes,last_modified",
                "First line should be CSV header"
            );
            // And there should be no data rows when empty
            assert_eq!(lines.len(), 1, "Should only have header line, no data rows");
        }
    }

    Ok(())
}

fn create_multifile_plugin(plugin_dir: &Path, plugin_name: &str) -> Result<()> {
    let plugin_path = plugin_dir.join(plugin_name);
    fs::create_dir(&plugin_path)?;

    let init_code = r#"
from .utils import process_table

def process_writes(influxdb3_local, table_batches, args=None):
    for table_batch in table_batches:
        count = process_table(table_batch)
        influxdb3_local.info(f"Processed {count} rows from {table_batch['table_name']}")
"#;
    fs::write(plugin_path.join("__init__.py"), init_code)?;

    let utils_code = r#"
def process_table(table_batch):
    return len(table_batch["rows"])
"#;
    fs::write(plugin_path.join("utils.py"), utils_code)?;

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_multifile_plugin_wal_trigger() -> Result<()> {
    let plugin_dir = TempDir::new()?;
    let plugin_dir_path = plugin_dir.path();

    create_multifile_plugin(plugin_dir_path, "my_multifile_plugin")?;

    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir_path.to_str().unwrap())
        .spawn()
        .await;

    server.create_database("test_db").run()?;

    server
        .create_trigger(
            "test_db",
            "multifile_trigger",
            "my_multifile_plugin",
            "all_tables",
        )
        .run()?;

    server
        .write_lp_to_db(
            "test_db",
            "cpu,host=server01 value=42.0",
            influxdb3_client::Precision::Second,
        )
        .await?;

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let result = server
        .query_sql("_internal")
        .with_sql("SELECT * FROM system.plugin_files WHERE plugin_name = 'multifile_trigger' ORDER BY file_name")
        .run()?;

    let parsed = result.as_array().expect("Expected array result");

    assert_eq!(
        parsed.len(),
        2,
        "Should have 2 files (__init__.py and utils.py)"
    );

    let file_names: Vec<&str> = parsed
        .iter()
        .map(|f| f["file_name"].as_str().unwrap())
        .collect();

    assert!(
        file_names.contains(&"__init__.py"),
        "Should contain __init__.py"
    );
    assert!(file_names.contains(&"utils.py"), "Should contain utils.py");

    for file in parsed {
        assert_eq!(file["plugin_name"], "multifile_trigger");
        assert!(file["size_bytes"].as_i64().unwrap() > 0);
        assert!(file["last_modified"].as_i64().unwrap() > 0);
    }

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_multifile_plugin_nested_structure() -> Result<()> {
    let plugin_dir = TempDir::new()?;
    let plugin_dir_path = plugin_dir.path();

    let plugin_path = plugin_dir_path.join("nested_plugin");
    fs::create_dir(&plugin_path)?;

    let models_dir = plugin_path.join("models");
    fs::create_dir(&models_dir)?;

    let init_code = r#"
from .models.processor import process_data

def process_writes(influxdb3_local, table_batches, args=None):
    result = process_data(table_batches)
    influxdb3_local.info(f"Processed {result} batches")
"#;
    fs::write(plugin_path.join("__init__.py"), init_code)?;

    let processor_code = r#"
def process_data(batches):
    return len(batches)
"#;
    fs::write(models_dir.join("processor.py"), processor_code)?;
    fs::write(models_dir.join("__init__.py"), "")?;

    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir_path.to_str().unwrap())
        .spawn()
        .await;

    server.create_database("test_db").run()?;

    server
        .create_trigger("test_db", "nested_trigger", "nested_plugin", "all_tables")
        .run()?;

    let result = server
        .query_sql("_internal")
        .with_sql("SELECT file_name FROM system.plugin_files WHERE plugin_name = 'nested_trigger' ORDER BY file_name")
        .run()?;

    let parsed = result.as_array().expect("Expected array result");

    assert_eq!(
        parsed.len(),
        3,
        "Should have 3 files (__init__.py at root and models/__init__.py and models/processor.py)"
    );

    let file_names: Vec<&str> = parsed
        .iter()
        .map(|f| f["file_name"].as_str().unwrap())
        .collect();

    assert!(
        file_names.contains(&"__init__.py"),
        "Should contain root __init__.py"
    );
    assert!(
        file_names.contains(&"models/__init__.py"),
        "Should contain models/__init__.py"
    );
    assert!(
        file_names.contains(&"models/processor.py"),
        "Should contain models/processor.py"
    );

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_multifile_plugin_missing_init_py() -> Result<()> {
    let plugin_dir = TempDir::new()?;
    let plugin_dir_path = plugin_dir.path();

    let plugin_path = plugin_dir_path.join("bad_plugin");
    fs::create_dir(&plugin_path)?;
    fs::write(plugin_path.join("utils.py"), "def helper(): pass")?;

    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir_path.to_str().unwrap())
        .spawn()
        .await;

    server.create_database("test_db").run()?;

    let result = server
        .create_trigger("test_db", "bad_trigger", "bad_plugin", "all_tables")
        .run();

    assert!(result.is_err(), "Should fail when __init__.py is missing");

    Ok(())
}
