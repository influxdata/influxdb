use crate::server::{ConfigProvider, TestServer};
use anyhow::Result;
use std::fs;
use std::path::PathBuf;
use tempfile::TempDir;

/// Helper struct to build multi-file plugin directories
struct PluginDirBuilder {
    temp_dir: TempDir,
    plugin_name: String,
    files: Vec<(String, String)>,
}

impl PluginDirBuilder {
    fn new(plugin_name: impl Into<String>) -> Result<Self> {
        Ok(Self {
            temp_dir: TempDir::new()?,
            plugin_name: plugin_name.into(),
            files: Vec::new(),
        })
    }

    fn add_file(mut self, name: impl Into<String>, content: impl Into<String>) -> Self {
        self.files.push((name.into(), content.into()));
        self
    }

    fn build(self) -> Result<(PathBuf, TempDir)> {
        let plugin_dir = self.temp_dir.path().join(&self.plugin_name);
        fs::create_dir(&plugin_dir)?;

        for (filename, content) in self.files {
            fs::write(plugin_dir.join(filename), content)?;
        }

        Ok((plugin_dir, self.temp_dir))
    }
}

#[test_log::test(tokio::test)]
async fn test_multi_file_wal_plugin() -> Result<()> {
    // Create a multi-file plugin
    let (plugin_dir, _temp_dir) = PluginDirBuilder::new("test_plugin")?
        .add_file(
            "helper.py",
            r#"
def process_value(value):
    """Helper function to process values"""
    return value * 2

def create_processed_line(table_name, value):
    """Create a LineBuilder for processed data"""
    line = LineBuilder(f"{table_name}_processed")
    line.float64_field("value", value)
    return line
"#,
        )
        .add_file(
            "utils.py",
            r#"
import datetime

def get_timestamp():
    """Get current timestamp"""
    return datetime.datetime.now().isoformat()

def log_message(influxdb3_local, message):
    """Log a message with timestamp"""
    timestamp = get_timestamp()
    influxdb3_local.info(f"[{timestamp}] {message}")
"#,
        )
        .add_file(
            "main.py",
            r#"
def process_writes(influxdb3_local, table_batches, args=None):
    """Main plugin function that processes write batches"""
    # Functions from other files are available in global namespace
    log_message(influxdb3_local, "Starting processing")
    
    for batch in table_batches:
        table_name = batch['table_name']
        rows = batch['rows']
        
        for row in rows:
            # Process temperature values - fields are directly in the row dict
            if 'temperature' in row:
                temperature = row['temperature']
                processed_value = process_value(temperature)
                
                # Write processed data using LineBuilder
                line = create_processed_line(table_name, processed_value)
                influxdb3_local.write(line)
                
                log_message(influxdb3_local, f"Processed: {temperature} -> {processed_value}")
    
    log_message(influxdb3_local, "Processing complete")
"#,
        )
        .build()?;

    // Start test server with plugin directory
    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir.parent().unwrap().to_string_lossy())
        .spawn()
        .await;

    // Create database and table
    server.create_database("test_db").run()?;
    server
        .create_table("test_db", "sensor_data")
        .with_fields([("temperature", "float64")])
        .run()?;

    // Create trigger with multi-file plugin
    server
        .create_trigger_with_dir(
            "test_db",
            "multi_file_trigger",
            plugin_dir.file_name().unwrap().to_str().unwrap(),
            "main.py",
            "table:sensor_data",
        )
        .run()?;

    // Write test data
    server
        .write("test_db")
        .with_line_protocol("sensor_data temperature=25.5")
        .run()?;

    // Give the plugin time to process
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Query to verify the plugin processed the data
    let result = server
        .query_sql("test_db")
        .with_sql("SELECT * FROM sensor_data_processed")
        .run()?;

    // Verify the processed value (25.5 * 2 = 51.0)
    let result_str = result.to_string();
    assert!(result_str.contains("51"));

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_multi_file_scheduled_plugin() -> Result<()> {
    // Create a scheduled multi-file plugin
    let (plugin_dir, _temp_dir) = PluginDirBuilder::new("scheduled_plugin")?
        .add_file(
            "config.py",
            r#"
# Configuration constants
METRIC_NAME = "system_status"
CHECK_INTERVAL = 5
"#,
        )
        .add_file(
            "monitoring.py",
            r#"
import random

def get_system_metrics():
    """Simulate getting system metrics"""
    return {
        'cpu': random.uniform(10, 90),
        'memory': random.uniform(20, 80),
        'disk': random.uniform(30, 70)
    }

def create_metric_lines(metrics):
    """Create LineBuilder objects for metrics"""
    lines = []
    for metric, value in metrics.items():
        line = LineBuilder(METRIC_NAME)  # METRIC_NAME will be available from config.py
        line.tag("type", metric)
        line.float64_field("value", value)
        lines.append(line)
    return lines
"#,
        )
        .add_file(
            "scheduler.py",
            r#"
def process_scheduled_call(influxdb3_local, schedule_time, args=None):
    """Scheduled plugin to collect and write system metrics"""
    influxdb3_local.info(f"Scheduled check at {schedule_time}")
    
    # Get system metrics - functions from other files are available in global namespace
    metrics = get_system_metrics()
    
    # Format and write metrics  
    for line in create_metric_lines(metrics):
        influxdb3_local.write(line)
    
    influxdb3_local.info(f"Wrote {len(metrics)} metrics")
"#,
        )
        .build()?;

    // Start test server with plugin directory
    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir.parent().unwrap().to_string_lossy())
        .spawn()
        .await;

    // Create database
    server.create_database("metrics_db").run()?;

    // Test the scheduled plugin with directory format
    let result = server
        .test_schedule_plugin(
            "metrics_db",
            &format!(
                "dir:{}:scheduler.py",
                plugin_dir.file_name().unwrap().to_str().unwrap()
            ),
            "* * * * * *", // Every second
        )
        .run()?;

    // Check for errors first
    if let Some(errors) = result["errors"].as_array() {
        if !errors.is_empty() {
            panic!("Plugin execution failed with errors: {:?}", errors);
        }
    }

    // Verify the plugin executed and wrote metrics
    let logs = result["log_lines"].as_array().unwrap();
    assert!(
        logs.iter()
            .any(|line| line.as_str().unwrap().contains("Scheduled check"))
    );
    assert!(
        logs.iter()
            .any(|line| line.as_str().unwrap().contains("Wrote 3 metrics"))
    );

    // Verify data was written - it's under database_writes for scheduled plugins
    let database_writes = result["database_writes"].as_object().unwrap();
    let metrics_writes = database_writes["metrics_db"].as_array().unwrap();
    assert_eq!(metrics_writes.len(), 3); // cpu, memory, disk
    assert!(
        metrics_writes
            .iter()
            .any(|line| line.as_str().unwrap().contains("type=cpu"))
    );
    assert!(
        metrics_writes
            .iter()
            .any(|line| line.as_str().unwrap().contains("type=memory"))
    );
    assert!(
        metrics_writes
            .iter()
            .any(|line| line.as_str().unwrap().contains("type=disk"))
    );

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_multi_file_plugin_with_import_error() -> Result<()> {
    // Create a plugin with a broken import (importing a non-existent third-party module)
    let (plugin_dir, _temp_dir) = PluginDirBuilder::new("broken_plugin")?
        .add_file(
            "helper.py",
            r#"
def helper_function():
    return "help"
"#,
        )
        .add_file(
            "main.py",
            r#"
# This import should fail because 'nonexistent_module' doesn't exist as a third-party module
import nonexistent_module

def process_writes(influxdb3_local, table_batches, args=None):
    # Try to use the nonexistent module
    nonexistent_module.do_something()
    # helper_function is available from global namespace
    helper_function()
"#,
        )
        .build()?;

    // Start test server with plugin directory
    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir.parent().unwrap().to_string_lossy())
        .spawn()
        .await;

    // Create database
    server.create_database("test_db").run()?;

    // Test that the plugin with import error fails appropriately
    let result = server
        .test_wal_plugin(
            "test_db",
            &format!(
                "dir:{}:main.py",
                plugin_dir.file_name().unwrap().to_str().unwrap()
            ),
        )
        .with_line_protocol("test foo=1")
        .run();

    // Should get an error about the missing module
    // The test returns errors in the JSON response, not as a Result error
    assert!(result.is_ok());
    let response = result.unwrap();
    let errors = response["errors"].as_array().unwrap();
    assert!(!errors.is_empty(), "Expected errors but got none");
    let error_msg = errors[0].as_str().unwrap();
    assert!(error_msg.contains("nonexistent_module") || error_msg.contains("No module named"));

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_multi_file_plugin_missing_entrypoint() -> Result<()> {
    // Create a plugin directory without the specified entrypoint
    let (plugin_dir, _temp_dir) = PluginDirBuilder::new("missing_entrypoint")?
        .add_file(
            "helper.py",
            r#"
def helper_function():
    return "help"
"#,
        )
        .add_file(
            "other.py",
            r#"
def process_writes(influxdb3_local, table_batches, args=None):
    pass
"#,
        )
        // Note: No main.py file
        .build()?;

    // Start test server with plugin directory
    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir.parent().unwrap().to_string_lossy())
        .spawn()
        .await;

    // Create database
    server.create_database("test_db").run()?;

    // Try to create trigger with missing entrypoint - should fail
    let result = server
        .create_trigger_with_dir(
            "test_db",
            "bad_trigger",
            plugin_dir.file_name().unwrap().to_str().unwrap(),
            "main.py", // This file doesn't exist
            "all_tables",
        )
        .run();

    assert!(result.is_err());

    Ok(())
}
