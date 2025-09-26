use std::fs;
use tempfile::TempDir;
use test_helpers::assert_contains;

use crate::server::{ConfigProvider, TestServer};

#[test_log::test(tokio::test)]
async fn test_show_system_plugin_tables() -> Result<(), Box<dyn std::error::Error>> {
    let plugin_dir = TempDir::new()?;
    let plugin_dir_path = plugin_dir.path();

    // Create a simple plugin
    let plugin_content = r#"
def process_writes(influxdb3_local, table_batches, args=None):
    influxdb3_local.info("Processing writes")
"#;
    fs::write(plugin_dir_path.join("test_plugin.py"), plugin_content)?;

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

    // Create a trigger
    server
        .create_trigger("test_db", "test_trigger", "test_plugin.py", "table:cpu")
        .with_trigger_arguments(vec!["arg1=val1", "arg2=val2"])
        .run()?;

    // First verify the trigger was created
    let trigger_result = server
        .query_sql("test_db")
        .with_sql("SELECT * FROM system.processing_engine_triggers")
        .run()?;

    let trigger_parsed = trigger_result
        .as_array()
        .expect("Expected array result for triggers");
    assert_eq!(trigger_parsed.len(), 1, "Should have one trigger");

    // Test show system table for plugin_files (in _internal database)
    let result = server
        .query_sql("_internal")
        .with_sql("SELECT * FROM system.plugin_files")
        .run()?;

    let parsed = result.as_array().expect("Expected array result");
    assert_eq!(parsed.len(), 1, "Should have one plugin file");
    assert_eq!(parsed[0]["plugin_name"], "test_trigger");
    assert_eq!(parsed[0]["file_name"], "test_plugin.py");

    // Test show system table for processing_engine_triggers
    let result = server
        .query_sql("test_db")
        .with_sql("SELECT * FROM system.processing_engine_triggers")
        .run()?;

    let parsed = result.as_array().expect("Expected array result");
    assert_eq!(parsed.len(), 1, "Should have one trigger");
    assert_eq!(parsed[0]["trigger_name"], "test_trigger");
    assert_eq!(parsed[0]["plugin_filename"], "test_plugin.py");
    assert_eq!(parsed[0]["disabled"], false);

    // Test show system table for processing_engine_trigger_arguments
    let result = server
        .query_sql("test_db")
        .with_sql("SELECT * FROM system.processing_engine_trigger_arguments")
        .run()?;

    let parsed = result.as_array().expect("Expected array result");
    assert_eq!(parsed.len(), 2, "Should have two arguments");

    // Check arguments (they may come in any order)
    let args: Vec<(String, String)> = parsed
        .iter()
        .map(|v| {
            (
                v["argument_key"].as_str().unwrap().to_string(),
                v["argument_value"].as_str().unwrap().to_string(),
            )
        })
        .collect();
    assert!(args.contains(&("arg1".to_string(), "val1".to_string())));
    assert!(args.contains(&("arg2".to_string(), "val2".to_string())));

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_show_system_table_list_includes_plugin_tables()
-> Result<(), Box<dyn std::error::Error>> {
    let server = TestServer::spawn().await;

    // Create database
    server.create_database("test_db").run()?;

    // Get list of system tables from test_db
    let result = server
        .query_sql("test_db")
        .with_sql("SELECT table_name FROM information_schema.columns WHERE table_schema = 'system' GROUP BY table_name")
        .run()?;

    let parsed = result.as_array().expect("Expected array result");

    // Extract table names from test_db
    let table_names: Vec<String> = parsed
        .iter()
        .map(|v| v["table_name"].as_str().unwrap().to_string())
        .collect();

    // Check that processing engine tables are included in test_db
    assert!(
        table_names.contains(&"processing_engine_triggers".to_string()),
        "Should contain processing_engine_triggers table"
    );
    assert!(
        table_names.contains(&"processing_engine_trigger_arguments".to_string()),
        "Should contain processing_engine_trigger_arguments table"
    );

    // Get list of system tables from _internal database (where plugin_files lives)
    let result = server
        .query_sql("_internal")
        .with_sql("SELECT table_name FROM information_schema.columns WHERE table_schema = 'system' GROUP BY table_name")
        .run()?;

    let parsed = result.as_array().expect("Expected array result");

    // Extract table names from _internal
    let internal_table_names: Vec<String> = parsed
        .iter()
        .map(|v| v["table_name"].as_str().unwrap().to_string())
        .collect();

    // Check that plugin_files is in _internal database
    assert!(
        internal_table_names.contains(&"plugin_files".to_string()),
        "Should contain plugin_files table in _internal database"
    );

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_show_system_plugin_tables_with_limits() -> Result<(), Box<dyn std::error::Error>> {
    let plugin_dir = TempDir::new()?;
    let plugin_dir_path = plugin_dir.path();

    // Create multiple plugins
    for i in 0..5 {
        let plugin_content = format!(
            r#"
def process_writes(influxdb3_local, table_batches, args=None):
    influxdb3_local.info("Plugin {i}")
"#
        );
        fs::write(
            plugin_dir_path.join(format!("plugin_{i}.py")),
            plugin_content,
        )?;
    }

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

    // Create multiple triggers
    for i in 0..5 {
        server
            .create_trigger(
                "test_db",
                format!("trigger_{i}"),
                format!("plugin_{i}.py"),
                "all_tables",
            )
            .run()?;
    }

    // Test with limit
    let result = server
        .query_sql("test_db")
        .with_sql("SELECT * FROM system.processing_engine_triggers LIMIT 3")
        .run()?;

    let parsed = result.as_array().expect("Expected array result");
    assert_eq!(parsed.len(), 3, "Should respect limit of 3");

    // Test with ordering
    let result = server
        .query_sql("test_db")
        .with_sql("SELECT * FROM system.processing_engine_triggers ORDER BY trigger_name")
        .run()?;

    let parsed = result.as_array().expect("Expected array result");
    let names: Vec<String> = parsed
        .iter()
        .map(|v| v["trigger_name"].as_str().unwrap().to_string())
        .collect();

    // Check ordering
    for i in 0..names.len() - 1 {
        assert!(
            names[i] <= names[i + 1],
            "Results should be ordered by trigger_name"
        );
    }

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_show_system_plugin_tables_empty() -> Result<(), Box<dyn std::error::Error>> {
    let server = TestServer::spawn().await;

    // Create database
    server.create_database("test_db").run()?;

    // Check empty plugin tables (plugin_files is in _internal database)
    let result = server
        .query_sql("_internal")
        .with_sql("SELECT * FROM system.plugin_files")
        .run()?;

    let parsed = result.as_array().expect("Expected array result");
    assert_eq!(parsed.len(), 0, "plugin_files should be empty initially");

    let result = server
        .query_sql("test_db")
        .with_sql("SELECT * FROM system.processing_engine_triggers")
        .run()?;

    let parsed = result.as_array().expect("Expected array result");
    assert_eq!(
        parsed.len(),
        0,
        "processing_engine_triggers should be empty initially"
    );

    let result = server
        .query_sql("test_db")
        .with_sql("SELECT * FROM system.processing_engine_trigger_arguments")
        .run()?;

    let parsed = result.as_array().expect("Expected array result");
    assert_eq!(
        parsed.len(),
        0,
        "processing_engine_trigger_arguments should be empty initially"
    );

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_update_trigger_single_file() -> Result<(), Box<dyn std::error::Error>> {
    let plugin_dir = TempDir::new()?;
    let plugin_dir_path = plugin_dir.path();

    // Create initial plugin
    let initial_content = r#"
def process_writes(influxdb3_local, table_batches, args=None):
    influxdb3_local.info("Initial version")
"#;
    fs::write(plugin_dir_path.join("test_plugin.py"), initial_content)?;

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

    // Create trigger
    server
        .create_trigger("test_db", "test_trigger", "test_plugin.py", "all_tables")
        .run()?;

    // Create updated plugin content
    let updated_content = r#"
def process_writes(influxdb3_local, table_batches, args=None):
    influxdb3_local.info("Updated version")
"#;
    let update_file = plugin_dir_path.join("updated_plugin.py");
    fs::write(&update_file, updated_content)?;

    // Update the trigger using CLI
    let result = server
        .update_trigger("test_db", "test_trigger")
        .with_path(update_file.to_str().unwrap())
        .run()?;

    assert_contains!(&result, "Plugin 'test_trigger' updated successfully");

    // Verify plugin still works by writing some data
    server
        .write("test_db")
        .with_line_protocol("cpu,host=server1 usage=0.5")
        .run()?;

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_update_trigger_multi_file() -> Result<(), Box<dyn std::error::Error>> {
    let plugin_dir = TempDir::new()?;
    let plugin_dir_path = plugin_dir.path();

    // Create initial multi-file plugin directory
    let initial_plugin_dir = plugin_dir_path.join("multi_plugin");
    fs::create_dir(&initial_plugin_dir)?;

    let main_content = r#"
from helper import process_data

def process_writes(influxdb3_local, table_batches, args=None):
    process_data(influxdb3_local, "v1")
"#;
    fs::write(initial_plugin_dir.join("__main__.py"), main_content)?;

    let helper_content = r#"
def process_data(influxdb3_local, version):
    influxdb3_local.info(f"Processing with {version}")
"#;
    fs::write(initial_plugin_dir.join("helper.py"), helper_content)?;

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

    // Create trigger with multi-file plugin
    server
        .create_trigger("test_db", "multi_trigger", "multi_plugin", "all_tables")
        .run()?;

    // Update just the helper file
    let updated_helper_content = r#"
def process_data(influxdb3_local, version):
    influxdb3_local.info(f"Updated processing with {version}")
"#;
    let update_helper = plugin_dir_path.join("helper.py");
    fs::write(&update_helper, updated_helper_content)?;

    // Update only the helper.py file in the multi-file plugin
    let result = server
        .update_trigger("test_db", "multi_trigger")
        .with_path(update_helper.to_str().unwrap())
        .run()?;

    assert_contains!(&result, "Plugin 'multi_trigger' updated successfully");

    // Verify plugin still works
    server
        .write("test_db")
        .with_line_protocol("cpu,host=server1 usage=0.5")
        .run()?;

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_update_trigger_entire_directory() -> Result<(), Box<dyn std::error::Error>> {
    let plugin_dir = TempDir::new()?;
    let plugin_dir_path = plugin_dir.path();

    // Create initial multi-file plugin
    let initial_plugin_dir = plugin_dir_path.join("initial_plugin");
    fs::create_dir(&initial_plugin_dir)?;

    let main_content = r#"
def process_writes(influxdb3_local, table_batches, args=None):
    influxdb3_local.info("Initial version")
"#;
    fs::write(initial_plugin_dir.join("__main__.py"), main_content)?;

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

    // Create trigger
    server
        .create_trigger("test_db", "dir_trigger", "initial_plugin", "all_tables")
        .run()?;

    // Create updated plugin directory with additional files
    let update_plugin_dir = plugin_dir_path.join("update_plugin");
    fs::create_dir(&update_plugin_dir)?;

    let new_main_content = r#"
from utilities import log_info

def process_writes(influxdb3_local, table_batches, args=None):
    log_info(influxdb3_local, "Updated version")
"#;
    fs::write(update_plugin_dir.join("__main__.py"), new_main_content)?;

    let utilities_content = r#"
def log_info(influxdb3_local, msg):
    influxdb3_local.info(f"LOG: {msg}")
"#;
    fs::write(update_plugin_dir.join("utilities.py"), utilities_content)?;

    // Update entire plugin from directory
    let result = server
        .update_trigger("test_db", "dir_trigger")
        .with_path(update_plugin_dir.to_str().unwrap())
        .run()?;

    assert_contains!(&result, "Plugin 'dir_trigger' updated successfully");

    // Verify plugin still works
    server
        .write("test_db")
        .with_line_protocol("cpu,host=server1 usage=0.5")
        .run()?;

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_client_plugin_api_methods() -> Result<(), Box<dyn std::error::Error>> {
    let plugin_dir = TempDir::new()?;
    let plugin_dir_path = plugin_dir.path();

    // Create a multi-file plugin
    let plugin_path = plugin_dir_path.join("test_plugin");
    fs::create_dir(&plugin_path)?;

    let main_content = r#"
from helper import process_data

def process_writes(influxdb3_local, table_batches, args=None):
    process_data(influxdb3_local, "test")
"#;
    fs::write(plugin_path.join("__main__.py"), main_content)?;

    let helper_content = r#"
def process_data(influxdb3_local, version):
    influxdb3_local.info(f"Processing data with {version}")
"#;
    fs::write(plugin_path.join("helper.py"), helper_content)?;

    // Start server with plugin directory
    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir_path.to_str().unwrap())
        .spawn()
        .await;

    // Create database and trigger
    server.create_database("test_db").run()?;
    server
        .create_table("test_db", "cpu")
        .with_fields([("usage", "float64")])
        .run()?;

    server
        .create_trigger("test_db", "test_trigger", "test_plugin", "all_tables")
        .run()?;

    // Create a client to test the new API methods
    let client = influxdb3_client::Client::new(
        server.client_addr(),
        Some("../testing-certs/rootCA.pem".into()),
    )?;

    // Test api_v3_list_all_plugins
    let all_plugins = client.api_v3_list_all_plugins().await?;
    assert_eq!(all_plugins.len(), 1, "Should have one plugin");
    assert_eq!(all_plugins[0].plugin_name, "test_trigger");
    assert_eq!(all_plugins[0].files.len(), 2, "Should have two files");

    // Test api_v3_get_plugin_files
    let plugin_files = client
        .api_v3_get_plugin_files("test_db", "test_trigger")
        .await?;
    assert_eq!(plugin_files.len(), 2, "Should have two files");
    let file_names: Vec<String> = plugin_files.iter().map(|f| f.file_name.clone()).collect();
    assert!(file_names.contains(&"__main__.py".to_string()));
    assert!(file_names.contains(&"helper.py".to_string()));

    // Test api_v3_get_plugin_file_content
    let main_content_retrieved = client
        .api_v3_get_plugin_file_content("test_db", "test_trigger", "__main__.py")
        .await?;
    assert_contains!(&main_content_retrieved, "from helper import process_data");
    assert_contains!(&main_content_retrieved, "def process_writes");

    let helper_content_retrieved = client
        .api_v3_get_plugin_file_content("test_db", "test_trigger", "helper.py")
        .await?;
    assert_contains!(&helper_content_retrieved, "def process_data");
    assert_contains!(&helper_content_retrieved, "Processing data with");

    Ok(())
}
