mod api;

use crate::server::{ConfigProvider, TestServer, parse_token};
use assert_cmd::Command as AssertCmd;
use observability_deps::tracing::debug;
use pretty_assertions::assert_eq;
use serde_json::{Value, json};
use std::fs::File;
use std::path::PathBuf;
use std::time::Duration;
use std::{fs, io::Write};
use test_helpers::tempfile::NamedTempFile;
use test_helpers::tempfile::TempDir;
use test_helpers::{assert_contains, assert_not_contains};

pub const WRITE_REPORTS_PLUGIN_CODE: &str = r#"
def process_writes(influxdb3_local, table_batches, args=None):
    for table_batch in table_batches:
        # Skip if table_name is write_reports
        if table_batch["table_name"] == "write_reports":
            continue

        row_count = len(table_batch["rows"])

        # Double row count if table name matches args table_name
        if args and "double_count_table" in args and table_batch["table_name"] == args["double_count_table"]:
            row_count *= 2

        line = LineBuilder("write_reports")\
            .tag("table_name", table_batch["table_name"])\
            .int64_field("row_count", row_count)
        influxdb3_local.write(line)
"#;

fn create_plugin_in_temp_dir(code: &str) -> (TempDir, PathBuf) {
    // Create a temporary directory that will exist until it's dropped
    let temp_dir = tempfile::tempdir().unwrap();

    let file_path = write_plugin_to_temp_dir(&temp_dir, "plugin.py", code);

    // Return both the directory (which must be kept alive) and the file path
    (temp_dir, file_path)
}

fn write_plugin_to_temp_dir(temp_dir: &TempDir, name: &str, code: &str) -> PathBuf {
    // Create a path for the plugin file within this directory
    let file_path = temp_dir.path().join(name);

    // Write the code to the file
    let mut file = File::create(&file_path).unwrap();
    writeln!(file, "{}", code).unwrap();
    file_path
}

#[test_log::test]
fn test_telemetry_disabled_with_debug_msg() {
    let serve_args = &[
        "serve",
        "--node-id",
        "the-best-node",
        "--object-store",
        "memory",
        "--http-bind",
        "0.0.0.0:0",
    ];

    let expected_disabled: &str = "Initializing TelemetryStore with upload disabled.";

    // validate we get a debug message indicating upload disabled
    let output = AssertCmd::cargo_bin("influxdb3")
        .unwrap()
        .args(serve_args)
        .arg("-vv")
        .arg("--disable-telemetry-upload")
        .timeout(std::time::Duration::from_millis(5000))
        .assert()
        .failure()
        .get_output()
        .stdout
        .clone();
    let output = String::from_utf8(output).expect("must be able to convert output to String");
    assert_contains!(output, expected_disabled);
}

#[test_log::test]
fn test_telemetry_disabled() {
    let serve_args = &[
        "serve",
        "--node-id",
        "the-best-node",
        "--object-store",
        "memory",
        "--http-bind",
        "0.0.0.0:0",
    ];

    let expected_disabled: &str = "Initializing TelemetryStore with upload disabled.";
    // validate no message when debug output disabled
    let output = AssertCmd::cargo_bin("influxdb3")
        .unwrap()
        .args(serve_args)
        .arg("-v")
        .arg("--disable-telemetry-upload")
        .timeout(std::time::Duration::from_millis(5000))
        .assert()
        .failure()
        .get_output()
        .stdout
        .clone();
    let output = String::from_utf8(output).expect("must be able to convert output to String");
    assert_not_contains!(output, expected_disabled);
}

#[test_log::test]
fn test_telemetry_enabled_with_debug_msg() {
    let serve_args = &[
        "serve",
        "--node-id",
        "the-best-node",
        "--object-store",
        "memory",
        "--http-bind",
        "0.0.0.0:0",
    ];

    let expected_enabled: &str =
        "Initializing TelemetryStore with upload enabled for http://localhost:9999.";

    // validate debug output shows which endpoint we are hitting when telemetry enabled
    let output = AssertCmd::cargo_bin("influxdb3")
        .unwrap()
        .args(serve_args)
        .arg("-vv")
        .arg("--telemetry-endpoint")
        .arg("http://localhost:9999")
        .timeout(std::time::Duration::from_millis(5000))
        .assert()
        .failure()
        .get_output()
        .stdout
        .clone();
    let output = String::from_utf8(output).expect("must be able to convert output to String");
    assert_contains!(output, expected_enabled);
}

#[test_log::test]
fn test_telementry_enabled() {
    let serve_args = &[
        "serve",
        "--node-id",
        "the-best-node",
        "--object-store",
        "memory",
        "--http-bind",
        "0.0.0.0:0",
    ];

    let expected_enabled: &str =
        "Initializing TelemetryStore with upload enabled for http://localhost:9999.";

    // validate no telemetry endpoint reported when debug output not enabled
    let output = AssertCmd::cargo_bin("influxdb3")
        .unwrap()
        .args(serve_args)
        .arg("-v")
        .arg("--telemetry-endpoint")
        .arg("http://localhost:9999")
        .timeout(std::time::Duration::from_millis(500))
        .assert()
        .failure()
        .get_output()
        .stdout
        .clone();
    let output = String::from_utf8(output).expect("must be able to convert output to String");
    assert_not_contains!(output, expected_enabled);
}
#[test_log::test(tokio::test)]
async fn test_show_databases() {
    let server = TestServer::spawn().await;

    // Create two databases
    server.create_database("foo").run().unwrap();
    server.create_database("bar").run().unwrap();

    // Show databases with default format (pretty)
    let output = server.show_databases().run().unwrap();
    assert_eq!(
        "\
        +---------------+\n\
        | iox::database |\n\
        +---------------+\n\
        | _internal     |\n\
        | bar           |\n\
        | foo           |\n\
        +---------------+\
        ",
        output
    );

    // Show databases with JSON format
    let output = server.show_databases().with_format("json").run().unwrap();
    assert_eq!(
        r#"[{"iox::database":"_internal"},{"iox::database":"bar"},{"iox::database":"foo"}]"#,
        output
    );

    // Show databases with CSV format
    let output = server.show_databases().with_format("csv").run().unwrap();
    assert_eq!(
        "\
        iox::database\n\
        _internal\n\
        bar\n\
        foo\
        ",
        output
    );

    // Show databases with JSONL format
    let output = server.show_databases().with_format("jsonl").run().unwrap();
    assert_eq!(
        "\
        {\"iox::database\":\"_internal\"}\n\
        {\"iox::database\":\"bar\"}\n\
        {\"iox::database\":\"foo\"}\
        ",
        output
    );

    // Delete a database
    server.delete_database("foo").run().unwrap();

    // Show databases after deletion
    let output = server.show_databases().run().unwrap();
    assert_eq!(
        "\
        +---------------+\n\
        | iox::database |\n\
        +---------------+\n\
        | _internal     |\n\
        | bar           |\n\
        +---------------+",
        output
    );

    // Show databases including deleted ones
    let output = server.show_databases().show_deleted(true).run().unwrap();
    // don't assert on actual output since it contains a time stamp which would be flaky
    assert_contains!(output, "foo-");
}

#[test_log::test(tokio::test)]
async fn test_create_database() {
    let server = TestServer::spawn().await;
    let db_name = "foo";

    let result = server.create_database(db_name).run().unwrap();
    debug!(result = ?result, "create database");
    assert_contains!(&result, "Database \"foo\" created successfully");
}

#[test_log::test(tokio::test)]
async fn test_create_database_limit() {
    let server = TestServer::spawn().await;
    let db_name = "foo";

    // Create 5 databases successfully
    for i in 0..5 {
        let name = format!("{db_name}{i}");
        let result = server.create_database(&name).run().unwrap();
        debug!(result = ?result, "create database");
        assert_contains!(&result, format!("Database \"{name}\" created successfully"));
    }

    // Try to create a 6th database, which should fail
    let result = server.create_database("foo5").run();
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    debug!(error = ?err, "create database error");
    assert_contains!(
        &err,
        "Adding a new database would exceed limit of 5 databases"
    );
}

#[test_log::test(tokio::test)]
async fn test_delete_database() {
    let server = TestServer::spawn().await;
    let db_name = "foo";

    // Write data to the database first
    server
        .write_lp_to_db(
            db_name,
            "cpu,t1=a,t2=b,t3=c f1=true,f2=\"hello\",f3=4i,f4=4u,f5=5 1000",
            influxdb3_client::Precision::Second,
        )
        .await
        .expect("write to db");

    // Delete the database using our new API
    let result = server
        .delete_database(db_name)
        .run()
        .expect("delete database");
    debug!(result = ?result, "delete database");

    // We need to modify the DeleteDatabaseQuery to return the output string
    assert_contains!(&result, "Database \"foo\" deleted successfully");
}

#[test_log::test(tokio::test)]
async fn test_delete_missing_database() {
    let server = TestServer::spawn().await;
    let db_name = "foo";

    // Try to delete a non-existent database
    let result = server.delete_database(db_name).run();

    // Check that we got the expected error
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    debug!(err = ?err, "delete missing database");
    assert_contains!(&err, "404");
}
#[test_log::test(tokio::test)]
async fn test_create_table() {
    let server = TestServer::spawn().await;
    let db_name = "foo";
    let table_name = "bar";

    // Create database using the new query API
    let result = server.create_database(db_name).run().unwrap();
    debug!(result = ?result, "create database");
    assert_contains!(&result, "Database \"foo\" created successfully");

    // Create table using the new query API
    let result = server
        .create_table(db_name, table_name)
        .with_tags(["one", "two", "three"])
        .with_fields([
            ("four", "utf8".to_string()),
            ("five", "uint64".to_string()),
            ("six", "float64".to_string()),
            ("seven", "int64".to_string()),
            ("eight", "bool".to_string()),
        ])
        .run()
        .unwrap();

    debug!(result = ?result, "create table");
    assert_contains!(&result, "Table \"foo\".\"bar\" created successfully");

    // Check that we can query the table and that it has no values
    let result = server
        .query_sql(db_name)
        .with_sql("SELECT * FROM bar")
        .run()
        .unwrap();

    assert_eq!(result, json!([]));

    // Write data to the table
    server
        .write_lp_to_db(
            db_name,
            format!("{table_name},one=1,two=2,three=3 four=\"4\",five=5u,six=6,seven=7i,eight=true 2998574937"),
            influxdb3_client::Precision::Second,
        )
        .await
        .expect("write to db");

    // Check that we can get data from the table
    let result = server
        .query_sql(db_name)
        .with_sql("SELECT * FROM bar")
        .run()
        .unwrap();

    debug!(result = ?result, "data written");
    assert_eq!(
        result,
        json!([{
            "one": "1",
            "two": "2",
            "three": "3",
            "four": "4",
            "five": 5,
            "six": 6.0,
            "seven": 7,
            "eight": true,
            "time": "2065-01-07T17:28:57"
        }])
    );
}
#[test_log::test(tokio::test)]
async fn test_create_table_fail_existing() {
    let server = TestServer::spawn().await;
    let db_name = "foo";
    let table_name = "bar";

    // Create database
    let result = server.create_database(db_name).run().unwrap();
    debug!(result = ?result, "create database");
    assert_contains!(&result, "Database \"foo\" created successfully");

    // Create table successfully
    let result = server
        .create_table(db_name, table_name)
        .add_tag("one")
        .add_field("four", "utf8")
        .run()
        .unwrap();
    debug!(result = ?result, "create table");
    assert_contains!(&result, "Table \"foo\".\"bar\" created successfully");

    // Try creating table again, expect failure
    let err = server
        .create_table(db_name, table_name)
        .add_tag("one")
        .add_field("four", "utf8")
        .run()
        .unwrap_err();

    insta::assert_snapshot!("test_create_table_fail_existing", err.to_string());
}

#[test_log::test(tokio::test)]
async fn test_create_table_tags_not_required() {
    let server = TestServer::spawn().await;
    let db_name = "foo";
    let table_name = "bar";

    // Create database
    let result = server.create_database(db_name).run().unwrap();
    debug!(result = ?result, "create database");
    assert_contains!(&result, "Database \"foo\" created successfully");

    // Create table successfully
    let result = server.create_table(db_name, table_name).run().unwrap();

    assert_contains!(&result, "Table \"foo\".\"bar\" created successfully");
}

#[test_log::test(tokio::test)]
async fn test_create_table_fail_empty_tags() {
    let server = TestServer::spawn().await;
    let db_name = "foo";
    let table_name = "bar";

    // Create database
    let result = server.create_database(db_name).run().unwrap();
    debug!(result = ?result, "create database");
    assert_contains!(&result, "Database \"foo\" created successfully");

    // Create table successfully
    let result = server
        .run(
            vec!["create", "table"],
            &["--database", db_name, table_name, "--tags"],
        )
        .unwrap_err();

    assert_contains!(
        result.to_string(),
        "error: a value is required for '--tags <TAGS>...' but none was supplied"
    );
}

#[test_log::test(tokio::test)]
async fn test_delete_table() {
    let server = TestServer::spawn().await;
    let db_name = "foo";
    let table_name = "cpu";

    server
        .write_lp_to_db(
            db_name,
            format!("{table_name},t1=a,t2=b,t3=c f1=true,f2=\"hello\",f3=4i,f4=4u,f5=5 1000"),
            influxdb3_client::Precision::Second,
        )
        .await
        .expect("write to db");

    let result = server.delete_table(db_name, table_name).run().unwrap();

    debug!(result = ?result, "delete table");
    assert_contains!(&result, "Table \"foo\".\"cpu\" deleted successfully");
}

#[test_log::test(tokio::test)]
async fn test_delete_missing_table() {
    let server = TestServer::spawn().await;
    let db_name = "foo";
    let table_name = "mem";

    // Write data to the "mem" table
    server
        .write_lp_to_db(
            db_name,
            format!("{table_name},t1=a,t2=b,t3=c f1=true,f2=\"hello\",f3=4i,f4=4u,f5=5 1000"),
            influxdb3_client::Precision::Second,
        )
        .await
        .expect("write to db");

    // Try to delete a non-existent "cpu" table
    let err = server.delete_table(db_name, "cpu").run().unwrap_err();

    debug!(result = ?err, "delete missing table");
    assert_contains!(err.to_string(), "404");
}

#[tokio::test]
async fn test_create_delete_distinct_cache() {
    let server = TestServer::spawn().await;
    let db_name = "foo";
    let table_name = "bar";

    server
        .write_lp_to_db(
            db_name,
            format!("{table_name},t1=a,t2=aa,t3=aaa f1=true,f2=\"hello\",f3=42"),
            influxdb3_client::Precision::Second,
        )
        .await
        .unwrap();

    let cache_name = "baz";

    // First create the cache
    let result = server
        .create_distinct_cache(db_name, table_name, cache_name)
        .with_columns(["t1", "t2"])
        .run()
        .unwrap();

    assert_contains!(&result, "new cache created");

    // Doing the same thing over again will be a no-op and return an error
    let err = server
        .create_distinct_cache(db_name, table_name, cache_name)
        .with_columns(["t1", "t2"])
        .run()
        .unwrap_err();

    assert_contains!(&err.to_string(), "[409 Conflict]");

    // Now delete the cache
    let result = server
        .delete_distinct_cache(db_name, table_name, cache_name)
        .run()
        .unwrap();

    assert_contains!(&result, "distinct cache deleted successfully");

    // Trying to delete again should result in an error as the cache no longer exists
    let err = server
        .delete_distinct_cache(db_name, table_name, cache_name)
        .run()
        .unwrap_err();

    assert_contains!(&err.to_string(), "[404 Not Found]");
}

#[test_log::test(tokio::test)]
async fn test_create_trigger_and_run() {
    // create a plugin and trigger and write data in, verifying that the trigger is enabled
    // and sent data

    let (temp_dir, plugin_path) = create_plugin_in_temp_dir(WRITE_REPORTS_PLUGIN_CODE);

    let plugin_dir = temp_dir.path().to_str().unwrap();
    let plugin_filename = plugin_path.file_name().unwrap().to_str().unwrap();

    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir)
        .spawn()
        .await;
    let db_name = "foo";
    let trigger_name = "test_trigger";

    // Setup: create database
    server.create_database(db_name).run().unwrap();

    // Creating the trigger should enable it
    let result = server
        .create_trigger(db_name, trigger_name, plugin_filename, "all_tables")
        .add_trigger_argument("double_count_table=cpu")
        .run()
        .unwrap();

    debug!(result = ?result, "create trigger");
    assert_contains!(&result, "Trigger test_trigger created successfully");

    // Now let's write data and see if it gets processed
    server
        .write_lp_to_db(
            db_name,
            "cpu,host=a f1=1.0\ncpu,host=b f1=2.0\nmem,host=a usage=234",
            influxdb3_client::Precision::Second,
        )
        .await
        .expect("write to db");

    let expected = json!(
        [
            {"table_name": "cpu", "row_count": 4},
            {"table_name": "mem", "row_count": 1}
        ]
    );

    // Query to see if the processed data is there. We loop because it could take a bit to write
    // back the data. There's also a condition where the table may have been created, but the
    // write hasn't happened yet, which returns empty results. This ensures we don't hit that race.
    let mut check_count = 0;
    loop {
        match server
            .query_sql(db_name)
            .with_sql("SELECT table_name, row_count FROM write_reports")
            .run()
        {
            Ok(value) => {
                if value == expected {
                    return;
                }
                check_count += 1;
                if check_count > 10 {
                    panic!(
                        "Unexpected query result, got: {:#?}, expected {:#?}",
                        value, expected
                    );
                }
            }
            Err(e) => {
                check_count += 1;
                if check_count > 10 {
                    panic!("Failed to query processed data: {}", e);
                }
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            }
        };
    }
}

#[test_log::test(tokio::test)]
async fn test_triggers_are_started() {
    // Create a plugin and trigger and write data in, verifying that the trigger is enabled
    // and sent data

    let (temp_dir, plugin_path) = create_plugin_in_temp_dir(WRITE_REPORTS_PLUGIN_CODE);

    let plugin_dir = temp_dir.path().to_str().unwrap();
    let plugin_filename = plugin_path.file_name().unwrap().to_str().unwrap();

    // Create tmp dir for object store
    let tmp_file = TempDir::new().unwrap();
    let tmp_dir = tmp_file.path().to_str().unwrap();

    let mut server = TestServer::configure()
        .with_plugin_dir(plugin_dir)
        .with_object_store_dir(tmp_dir)
        .spawn()
        .await;
    let db_name = "foo";
    let trigger_name = "test_trigger";

    // Setup: create database
    server.create_database(db_name).run().unwrap();

    // Creating the trigger should enable it
    let result = server
        .create_trigger(db_name, trigger_name, plugin_filename, "all_tables")
        .add_trigger_argument("double_count_table=cpu")
        .run()
        .unwrap();

    debug!(result = ?result, "create trigger");
    assert_contains!(&result, "Trigger test_trigger created successfully");

    // Restart the server
    server.kill();

    server = TestServer::configure()
        .with_plugin_dir(plugin_dir)
        .with_object_store_dir(tmp_dir)
        .spawn()
        .await;

    // Now let's write data and see if it gets processed
    server
        .write_lp_to_db(
            db_name,
            "cpu,host=a f1=1.0\ncpu,host=b f1=2.0\nmem,host=a usage=234",
            influxdb3_client::Precision::Second,
        )
        .await
        .expect("write to db");

    let expected = json!(
        [
            {"table_name": "cpu", "row_count": 4},
            {"table_name": "mem", "row_count": 1}
        ]
    );

    // Query to see if the processed data is there. We loop because it could take a bit to write
    // back the data. There's also a condition where the table may have been created, but the
    // write hasn't happened yet, which returns empty results. This ensures we don't hit that race.
    let mut check_count = 0;
    loop {
        match server
            .query_sql(db_name)
            .with_sql("SELECT table_name, row_count FROM write_reports")
            .run()
        {
            Ok(value) => {
                if value == expected {
                    return;
                }
                check_count += 1;
                if check_count > 10 {
                    panic!(
                        "Unexpected query result, got: {:#?}, expected {:#?}",
                        value, expected
                    );
                }
            }
            Err(e) => {
                check_count += 1;
                if check_count > 10 {
                    panic!("Failed to query processed data: {}", e);
                }
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            }
        };
    }
}
#[test_log::test(tokio::test)]
async fn test_database_create_persists() {
    // create tmp dir for object store
    let tmp_file = TempDir::new().unwrap();
    let tmp_dir = tmp_file.path().to_str().unwrap();

    let mut server = TestServer::configure()
        .with_object_store_dir(tmp_dir)
        .spawn()
        .await;

    let db_name = "foo";
    let result = server.create_database(db_name).run().unwrap();
    debug!(result = ?result, "create database");
    assert_contains!(&result, "Database \"foo\" created successfully");

    // restart the server
    server.kill();

    server = TestServer::configure()
        .with_object_store_dir(tmp_dir)
        .spawn()
        .await;

    let result = server.show_databases().run().unwrap();
    assert_eq!(
        r#"+---------------+
| iox::database |
+---------------+
| _internal     |
| foo           |
+---------------+"#,
        result
    );
}
#[test_log::test(tokio::test)]
async fn test_trigger_enable() {
    let (temp_dir, plugin_path) = create_plugin_in_temp_dir(
        r#"
def process_writes(influxdb3_local, table_batches, args=None):
    influxdb3_local.info("done")
"#,
    );

    let plugin_dir = temp_dir.path().to_str().unwrap();
    let plugin_filename = plugin_path.file_name().unwrap().to_str().unwrap();

    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir)
        .spawn()
        .await;
    let db_name = "foo";
    let trigger_name = "test_trigger";

    // Setup: create database and trigger
    server.create_database(db_name).run().unwrap();

    server
        .create_trigger(db_name, trigger_name, plugin_filename, "all_tables")
        .run()
        .unwrap();

    // Test enabling
    let result = server.enable_trigger(db_name, trigger_name).run().unwrap();

    debug!(result = ?result, "enable trigger");
    assert_contains!(&result, "Trigger test_trigger enabled successfully");

    // Test disable
    let result = server.disable_trigger(db_name, trigger_name).run().unwrap();

    debug!(result = ?result, "disable trigger");
    assert_contains!(&result, "Trigger test_trigger disabled successfully");
}
#[test_log::test(tokio::test)]
async fn test_delete_enabled_trigger() {
    let (temp_dir, plugin_path) = create_plugin_in_temp_dir(
        r#"
def process_writes(influxdb3_local, table_batches, args=None):
    influxdb3_local.info("done")
"#,
    );

    let plugin_dir = temp_dir.path().to_str().unwrap();
    let plugin_filename = plugin_path.file_name().unwrap().to_str().unwrap();

    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir)
        .spawn()
        .await;
    let db_name = "foo";
    let trigger_name = "test_trigger";

    // Setup: create database, plugin, and enable trigger
    server.create_database(db_name).run().unwrap();

    server
        .create_trigger(db_name, trigger_name, plugin_filename, "all_tables")
        .run()
        .unwrap();

    // Try to delete the enabled trigger without force flag
    let err = server
        .delete_trigger(db_name, trigger_name)
        .run()
        .unwrap_err();

    debug!(result = ?err, "delete enabled trigger without force");
    assert_contains!(&err.to_string(), "command failed");

    // Delete active trigger with force flag
    let result = server
        .delete_trigger(db_name, trigger_name)
        .force(true)
        .run()
        .unwrap();

    debug!(result = ?result, "delete enabled trigger with force");
    assert_contains!(&result, "Trigger test_trigger deleted successfully");
}
#[test_log::test(tokio::test)]
async fn test_table_specific_trigger() {
    let (temp_dir, plugin_path) = create_plugin_in_temp_dir(
        r#"
def process_writes(influxdb3_local, table_batches, args=None):
    influxdb3_local.info("done")
"#,
    );

    let plugin_dir = temp_dir.path().to_str().unwrap();
    let plugin_filename = plugin_path.file_name().unwrap().to_str().unwrap();

    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir)
        .spawn()
        .await;
    let db_name = "foo";
    let table_name = "bar";
    let trigger_name = "test_trigger";

    // Setup: create database, table, and plugin
    server.create_database(db_name).run().unwrap();

    server
        .create_table(db_name, table_name)
        .add_tag("tag1")
        .add_field("field1", "float64")
        .run()
        .unwrap();

    // Create table-specific trigger
    let result = server
        .create_trigger(
            db_name,
            trigger_name,
            plugin_filename,
            format!("table:{}", table_name),
        )
        .run()
        .unwrap();

    debug!(result = ?result, "create table-specific trigger");
    assert_contains!(&result, "Trigger test_trigger created successfully");
}

#[test_log::test(tokio::test)]
async fn test_show_system() {
    let server = TestServer::configure().spawn().await;
    let db_name = "foo";

    server
        .write_lp_to_db(
            db_name,
            "cpu,t1=a,t2=b,t3=c f1=true,f2=\"hello\",f3=4i,f4=4u,f5=5 1000",
            influxdb3_client::Precision::Second,
        )
        .await
        .expect("write to db");

    // Test successful cases
    // 1. Summary command
    let summary_output = server.show_system(db_name).summary().run().unwrap();
    insta::assert_snapshot!(
        "summary_should_show_up_to_ten_entries_from_each_table",
        summary_output
    );

    // 2. Table command for "queries"
    let table_output = server.show_system(db_name).table("queries").run().unwrap();
    insta::assert_snapshot!(
        "table_NAME_should_show_queries_table_without_information_or_system_schema_queries",
        table_output
    );

    // 3. Table-list command
    let table_list_output = server.show_system(db_name).table_list().run().unwrap();
    insta::assert_snapshot!(
        "table-list_should_list_system_schema_tables_only",
        table_list_output
    );

    // Test failure cases
    // 1. Missing database (this can't be tested with the fluent API since we always require a db name)
    let output = server
        .run(
            vec!["show", "system"],
            &["table-list", "--tls-ca", "../testing-certs/rootCA.pem"],
        )
        .unwrap_err()
        .to_string();
    insta::assert_snapshot!("fail_without_database_name", output);

    // 2. Non-existent table name
    let result = server.show_system(db_name).table("meow").run();
    assert!(result.is_err());
    insta::assert_snapshot!(
        "random_table_name_doesn't_exist,_should_error",
        format!("{}", result.unwrap_err())
    );

    // 3. IOx schema table (not a system table)
    let result = server.show_system(db_name).table("cpu").run();
    assert!(result.is_err());
    insta::assert_snapshot!(
        "iox_schema_table_name_exists,_but_should_error_because_we're_concerned_here_with_system_tables",
        format!("{}", result.unwrap_err())
    );
}

#[tokio::test]
async fn distinct_cache_create_and_delete() {
    let server = TestServer::spawn().await;
    let db_name = "foo";

    server
        .write_lp_to_db(
            db_name,
            "cpu,t1=a,t2=b,t3=c f1=true,f2=\"hello\",f3=4i,f4=4u,f5=5 1000",
            influxdb3_client::Precision::Second,
        )
        .await
        .expect("write to db");

    // Create distinct cache
    let result = server
        .create_distinct_cache(db_name, "cpu", "cache_money")
        .with_columns(["t1", "t2"])
        .with_max_cardinality(20000)
        .with_max_age("200s")
        .run()
        .unwrap();

    assert_contains!(result, "new cache created");

    // Try to create again, should not work
    let err = server
        .create_distinct_cache(db_name, "cpu", "cache_money")
        .with_columns(["t1", "t2"])
        .with_max_cardinality(20000)
        .with_max_age("200s")
        .run()
        .unwrap_err();

    assert_contains!(
        err.to_string(),
        "attempted to create a resource that already exists"
    );

    // Delete the cache
    let result = server
        .delete_distinct_cache(db_name, "cpu", "cache_money")
        .run()
        .unwrap();

    assert_contains!(result, "distinct cache deleted successfully");
}
#[test_log::test(tokio::test)]
async fn test_linebuilder_escaping() {
    let (temp_dir, plugin_path) = create_plugin_in_temp_dir(
        r#"
def process_writes(influxdb3_local, table_batches, args=None):
    # Basic test with all field types
    basic_line = LineBuilder("metrics")\
        .tag("host", "server01")\
        .tag("region", "us-west")\
        .int64_field("cpu", 42)\
        .uint64_field("memory_bytes", 8589934592)\
        .float64_field("load", 86.5)\
        .string_field("status", "online")\
        .bool_field("healthy", True)\
        .time_ns(1609459200000000000)
    influxdb3_local.write(basic_line)

    # Test escaping spaces in tag values
    spaces_line = LineBuilder("system_metrics")\
        .tag("server", "app server 1")\
        .tag("datacenter", "us west")\
        .int64_field("count", 1)
    influxdb3_local.write(spaces_line)

    # Test escaping commas in tag values
    commas_line = LineBuilder("network")\
        .tag("servers", "web,app,db")\
        .tag("location", "floor1,rack3")\
        .int64_field("connections", 256)
    influxdb3_local.write(commas_line)

    # Test escaping equals signs in tag values
    equals_line = LineBuilder("formulas")\
        .tag("equation", "y=mx+b")\
        .tag("result", "a=b=c")\
        .float64_field("value", 3.14159)
    influxdb3_local.write(equals_line)

    # Test escaping backslashes in tag values
    backslash_line = LineBuilder("paths")\
        .tag("windows_path", "C:\\Program Files\\App")\
        .tag("regex", "\\d+\\w+")\
        .string_field("description", "Windows\\Unix paths")\
        .int64_field("count", 42)
    influxdb3_local.write(backslash_line)

    # Test escaping quotes in string fields
    quotes_line = LineBuilder("messages")\
        .tag("type", "notification")\
        .string_field("content", "User said \"Hello World\"")\
        .string_field("json", "{\"key\": \"value\"}")\
        .int64_field("priority", 1)
    influxdb3_local.write(quotes_line)

    # Test a complex case with multiple escape characters
    complex_line = LineBuilder("complex,measurement")\
        .tag("location", "New York, USA")\
        .tag("details", "floor=5, room=3")\
        .tag("path", "C:\\Users\\Admin\\Documents")\
        .string_field("message", "Error in line: \"x = y + z\"")\
        .string_field("query", "SELECT * FROM table WHERE id=\"abc\"")\
        .float64_field("value", 123.456)\
        .time_ns(1609459200000000000)
    influxdb3_local.write(complex_line)

    # Test writing to a specific database
    specific_db_line = LineBuilder("memory_stats")\
        .tag("host", "server1")\
        .int64_field("usage", 75)
    influxdb3_local.write_to_db("metrics_db", specific_db_line)

    # Test multiple chained methods
    chained_line = LineBuilder("sensor_data")\
        .tag("device", "thermostat").tag("room", "living room").tag("floor", "1")\
        .float64_field("temperature", 72.5).int64_field("humidity", 45).string_field("mode", "auto")
    influxdb3_local.write(chained_line)

    influxdb3_local.info("All LineBuilder tests completed")"#,
    );

    let plugin_dir = temp_dir.path().to_str().unwrap();
    let plugin_filename = plugin_path.file_name().unwrap().to_str().unwrap();

    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir)
        .spawn()
        .await;

    let db_name = "test_db";

    // Run the test using the fluent API
    let result = server
        .test_wal_plugin(db_name, plugin_filename)
        .with_line_protocol("test_input,tag1=tag1_value field1=1i 500")
        .add_input_argument("arg1=test")
        .run()
        .expect("Failed to run wal plugin test");

    let expected_result = serde_json::json!({
        "log_lines": [
            "INFO: All LineBuilder tests completed"
        ],
        "database_writes": {
            "test_db": [
                "metrics,host=server01,region=us-west cpu=42i,memory_bytes=8589934592u,load=86.5,status=\"online\",healthy=t 1609459200000000000",
                "system_metrics,server=app\\ server\\ 1,datacenter=us\\ west count=1i",
                "network,servers=web\\,app\\,db,location=floor1\\,rack3 connections=256i",
                "formulas,equation=y\\=mx+b,result=a\\=b\\=c value=3.14159",
                "paths,windows_path=C:\\\\Program\\ Files\\\\App,regex=\\\\d+\\\\w+ description=\"Windows\\\\Unix paths\",count=42i",
                "messages,type=notification content=\"User said \\\"Hello World\\\"\",json=\"{\\\"key\\\": \\\"value\\\"}\",priority=1i",
                "complex\\,measurement,location=New\\ York\\,\\ USA,details=floor\\=5\\,\\ room\\=3,path=C:\\\\Users\\\\Admin\\\\Documents message=\"Error in line: \\\"x = y + z\\\"\",query=\"SELECT * FROM table WHERE id=\\\"abc\\\"\",value=123.456 1609459200000000000",
                "sensor_data,device=thermostat,room=living\\ room,floor=1 temperature=72.5,humidity=45i,mode=\"auto\""
            ],
            "metrics_db": [
                "memory_stats,host=server1 usage=75i"
            ]
        },
        "errors": []
    });

    assert_eq!(result, expected_result);
}
#[test_log::test(tokio::test)]
async fn test_wal_plugin_test() {
    use crate::server::ConfigProvider;
    use influxdb3_client::Precision;

    let (temp_dir, plugin_path) = create_plugin_in_temp_dir(
        r#"
def process_writes(influxdb3_local, table_batches, args=None):
    influxdb3_local.info("arg1: " + args["arg1"])

    query_params = {"host": args["host"]}
    query_result = influxdb3_local.query("SELECT host, region, usage FROM cpu where host = $host", query_params)
    influxdb3_local.info("query result: " + str(query_result))
    influxdb3_local.info("i", query_result, args["host"])
    influxdb3_local.warn("w:", query_result)
    influxdb3_local.error("err", query_result)

    for table_batch in table_batches:
        influxdb3_local.info("table: " + table_batch["table_name"])

        for row in table_batch["rows"]:
            influxdb3_local.info("row: " + str(row))

    line = LineBuilder("some_table")\
        .tag("tag1", "tag1_value")\
        .tag("tag2", "tag2_value")\
        .int64_field("field1", 1)\
        .float64_field("field2", 2.0)\
        .string_field("field3", "number three")
    influxdb3_local.write(line)

    other_line = LineBuilder("other_table")
    other_line.int64_field("other_field", 1)
    other_line.float64_field("other_field2", 3.14)
    other_line.time_ns(1302)

    influxdb3_local.write_to_db("mytestdb", other_line)

    influxdb3_local.info("done")"#,
    );

    let plugin_dir = temp_dir.path().to_str().unwrap();
    let plugin_filename = plugin_path.file_name().unwrap().to_str().unwrap();

    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir)
        .spawn()
        .await;

    server
        .write_lp_to_db(
            "foo",
            "cpu,host=s1,region=us-east usage=0.9\n\
            cpu,host=s2,region=us-east usage=0.89\n\
            cpu,host=s1,region=us-east usage=0.85",
            Precision::Nanosecond,
        )
        .await
        .unwrap();

    let db_name = "foo";

    // Run the test using the fluent API
    let result = server
        .test_wal_plugin(db_name, plugin_filename)
        .with_line_protocol("test_input,tag1=tag1_value,tag2=tag2_value field1=1i 500")
        .with_input_arguments(["arg1=arg1_value", "host=s2"])
        .run()
        .expect("Failed to run test_wal_plugin");

    debug!(result = ?result, "test wal plugin");

    let expected_result = r#"{
  "log_lines": [
    "INFO: arg1: arg1_value",
    "INFO: query result: [{'host': 's2', 'region': 'us-east', 'usage': 0.89}]",
    "INFO: i [{'host': 's2', 'region': 'us-east', 'usage': 0.89}] s2",
    "WARN: w: [{'host': 's2', 'region': 'us-east', 'usage': 0.89}]",
    "ERROR: err [{'host': 's2', 'region': 'us-east', 'usage': 0.89}]",
    "INFO: table: test_input",
    "INFO: row: {'tag1': 'tag1_value', 'tag2': 'tag2_value', 'field1': 1, 'time': 500}",
    "INFO: done"
  ],
  "database_writes": {
    "mytestdb": [
      "other_table other_field=1i,other_field2=3.14 1302"
    ],
    "foo": [
      "some_table,tag1=tag1_value,tag2=tag2_value field1=1i,field2=2.0,field3=\"number three\""
    ]
  },
  "errors": []
}"#;
    let expected_result = serde_json::from_str::<serde_json::Value>(expected_result).unwrap();
    assert_eq!(result, expected_result);
}
#[test_log::test(tokio::test)]
async fn test_schedule_plugin_test() {
    use crate::server::ConfigProvider;
    use influxdb3_client::Precision;

    // Create plugin file with a scheduled task
    let (temp_dir, plugin_path) = create_plugin_in_temp_dir(
        r#"
def process_scheduled_call(influxdb3_local, schedule_time, args=None):
    influxdb3_local.info(f"args are {args}")
    influxdb3_local.info("Successfully called")"#,
    );

    let plugin_dir = temp_dir.path().to_str().unwrap();
    let plugin_name = plugin_path.file_name().unwrap().to_str().unwrap();

    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir)
        .spawn()
        .await;

    // Write some test data
    server
        .write_lp_to_db(
            "foo",
            "cpu,host=host1,region=us-east usage=0.75\n\
             cpu,host=host2,region=us-west usage=0.82\n\
             cpu,host=host3,region=us-east usage=0.91",
            Precision::Nanosecond,
        )
        .await
        .unwrap();

    let db_name = "foo";

    // Run the schedule plugin test using the new fluent API
    let result = server
        .test_schedule_plugin(db_name, plugin_name, "*/5 * * * * *") // Run every 5 seconds
        .add_input_argument("region=us-east")
        .run()
        .expect("Failed to run schedule plugin test");

    debug!(result = ?result, "test schedule plugin");

    // The trigger_time will be dynamic, so we'll just verify it exists and is in the right format
    let trigger_time = result["trigger_time"].as_str().unwrap();
    assert!(trigger_time.contains('T')); // Basic RFC3339 format check

    // Check the rest of the response structure
    let expected_result = serde_json::json!({
        "log_lines": [
            "INFO: args are {'region': 'us-east'}",
            "INFO: Successfully called"
        ],
        "database_writes": {
        },
        "errors": []
    });
    assert_eq!(result["log_lines"], expected_result["log_lines"]);
    assert_eq!(
        result["database_writes"],
        expected_result["database_writes"]
    );
    assert_eq!(result["errors"], expected_result["errors"]);
}

#[test_log::test(tokio::test)]
async fn test_tag_query_behavior() {
    use crate::server::ConfigProvider;
    use influxdb3_client::Precision;

    // Create plugin file with a scheduled task
    let (temp_dir, plugin_path) = create_plugin_in_temp_dir(
        r#"
def process_scheduled_call(influxdb3_local, time, args=None):
    influxdb3_local.query("SELECT val1,val2,tag1 FROM foo")
    influxdb3_local.info("successfully queried")"#,
    );

    let plugin_dir = temp_dir.path().to_str().unwrap();
    let plugin_name = plugin_path.file_name().unwrap().to_str().unwrap();

    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir)
        .spawn()
        .await;

    // Write some test data
    server
        .write_lp_to_db(
            "foo",
            r#"foo,tag1=bar,tag2=blep val1=5,val2=99
foo,tag1=bar,tag2=blep val1=10,val2=99
foo,tag1=bar,tag2=blep val1=10,val2=199
foo,tag1=bar,tag2=mlem val1=10,val2=199
foo,tag1=bar,tag2=mloem val1=5,val2=199"#,
            Precision::Second,
        )
        .await
        .unwrap();

    let db_name = "foo";

    let result = server
        .test_schedule_plugin(db_name, plugin_name, "*/5 * * * * *")
        .add_input_argument("region=us-east")
        .run()
        .expect("Failed to run schedule plugin test");

    debug!(result = ?result, "test schedule plugin");
    assert_eq!(result["errors"], json!([]));
    assert_eq!(result["log_lines"], json!(["INFO: successfully queried"]));
}

#[test_log::test(tokio::test)]
async fn test_schedule_plugin_test_with_strftime() {
    use crate::server::ConfigProvider;
    use influxdb3_client::Precision;

    let (temp_dir, plugin_path) = create_plugin_in_temp_dir(
        r#"
import datetime
def process_scheduled_call(influxdb3_local, schedule_time, args=None):
    timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    influxdb3_local.info(f"Current timestamp: {timestamp}")
    influxdb3_local.info(f"args are {args}")
    influxdb3_local.info("Successfully called")"#,
    );

    let plugin_dir = temp_dir.path().to_str().unwrap();
    let plugin_name = plugin_path.file_name().unwrap().to_str().unwrap();

    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir)
        .spawn()
        .await;

    // Write some test data
    server
        .write_lp_to_db(
            "foo",
            "cpu,host=host1,region=us-east usage=0.75\n\
             cpu,host=host2,region=us-west usage=0.82\n\
             cpu,host=host3,region=us-east usage=0.91",
            Precision::Nanosecond,
        )
        .await
        .unwrap();

    let db_name = "foo";

    // Run the schedule plugin test using the fluent API
    let result = server
        .test_schedule_plugin(db_name, plugin_name, "*/5 * * * * *") // Run every 5 seconds
        .add_input_argument("region=us-east")
        .run()
        .unwrap();

    debug!(result = ?result, "test schedule plugin");

    // The trigger_time will be dynamic, so we'll just verify it exists and is in the right format
    let trigger_time = result["trigger_time"].as_str().unwrap();
    assert!(trigger_time.contains('T')); // Basic RFC3339 format check

    // Check the rest of the response structure
    // Modified expectations to include the timestamp message
    let log_lines = &result["log_lines"];
    assert_eq!(log_lines.as_array().unwrap().len(), 3);
    assert!(
        log_lines[0]
            .as_str()
            .unwrap()
            .starts_with("INFO: Current timestamp:")
    );
    assert_eq!(
        log_lines[1].as_str().unwrap(),
        "INFO: args are {'region': 'us-east'}"
    );
    assert_eq!(log_lines[2].as_str().unwrap(), "INFO: Successfully called");

    assert_eq!(result["database_writes"], serde_json::json!({}));
    assert_eq!(result["errors"], serde_json::json!([]));
}

#[test_log::test(tokio::test)]
async fn test_wal_plugin_errors() {
    use crate::server::ConfigProvider;
    use influxdb3_client::Precision;

    struct Test {
        name: &'static str,
        plugin_code: &'static str,
        expected_error: &'static str,
    }

    let tests = vec![
        Test {
            name: "invalid_python",
            plugin_code: r#"
        lkjasdf9823
        jjjjj / sss"#,
            expected_error: "error executing plugin: IndentationError: unexpected indent (<string>, line 2)",
        },
        Test {
            name: "no_process_writes",
            plugin_code: r#"
def not_process_writes(influxdb3_local, table_batches, args=None):
    influxdb3_local.info("done")"#,
            expected_error: "error executing plugin: the process_writes function is not present in the plugin. Should be defined as: process_writes(influxdb3_local, table_batches, args=None)",
        },
        Test {
            name: "no_args",
            plugin_code: r#"
def process_writes(influxdb3_local, table_batches):
    influxdb3_local.info("done")
"#,
            expected_error: "error executing plugin: TypeError: process_writes() takes 2 positional arguments but 3 were given",
        },
        Test {
            name: "no_table_batches",
            plugin_code: r#"
def process_writes(influxdb3_local, args=None):
    influxdb3_local.info("done")
"#,
            expected_error: "error executing plugin: TypeError: process_writes() takes from 1 to 2 positional arguments but 3 were given",
        },
        Test {
            name: "no_influxdb3_local",
            plugin_code: r#"
def process_writes(table_batches, args=None):
    influxdb3_local.info("done")
"#,
            expected_error: "error executing plugin: TypeError: process_writes() takes from 1 to 2 positional arguments but 3 were given",
        },
        Test {
            name: "line_builder_no_field",
            plugin_code: r#"
def process_writes(influxdb3_local, table_batches, args=None):
    line = LineBuilder("some_table")
    influxdb3_local.write(line)
"#,
            expected_error: "error executing plugin: InvalidLineError: At least one field is required: some_table",
        },
        Test {
            name: "query_no_table",
            plugin_code: r#"
def process_writes(influxdb3_local, table_batches, args=None):
    influxdb3_local.query("SELECT foo FROM not_here")
"#,
            expected_error: "error executing plugin: QueryError: error: error while planning query: Error during planning: table 'public.iox.not_here' not found executing query: SELECT foo FROM not_here",
        },
    ];

    let plugin_dir = TempDir::new().unwrap();

    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir.path().to_str().unwrap())
        .spawn()
        .await;

    server
        .write_lp_to_db(
            "foo",
            "cpu,host=s1,region=us-east usage=0.9\n\
            cpu,host=s2,region=us-east usage=0.89\n\
            cpu,host=s1,region=us-east usage=0.85",
            Precision::Nanosecond,
        )
        .await
        .unwrap();

    let db_name = "foo";
    let lp = "test_input,tag1=tag1_value,tag2=tag2_value field1=1i 500";
    let input_args = vec!["arg1=arg1_value", "host=s2"];

    for test in tests {
        let mut plugin_file = NamedTempFile::new_in(plugin_dir.path()).unwrap();
        writeln!(plugin_file, "{}", test.plugin_code).unwrap();
        let plugin_name = plugin_file.path().file_name().unwrap().to_str().unwrap();

        // Using the fluent API for testing wal plugins
        let result = server
            .test_wal_plugin(db_name, plugin_name)
            .with_line_protocol(lp)
            .with_input_arguments(input_args.clone())
            .run()
            .unwrap();

        debug!(result = ?result, "test wal plugin");

        let errors = result.get("errors").unwrap().as_array().unwrap();
        let error = errors[0].as_str().unwrap();
        assert_eq!(
            error,
            test.expected_error,
            "test: {}, response was: {}",
            test.name,
            serde_json::to_string_pretty(&result).unwrap()
        );
    }
}

#[test_log::test(tokio::test)]
async fn test_load_wal_plugin_from_gh() {
    use crate::server::ConfigProvider;
    use influxdb3_client::Precision;

    let plugin_dir = TempDir::new().unwrap();

    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir.path().to_str().unwrap())
        .spawn()
        .await;

    server
        .write_lp_to_db(
            "foo",
            "cpu,host=s1,region=us-east usage=0.9\n\
            cpu,host=s2,region=us-east usage=0.89\n\
            cpu,host=s1,region=us-east usage=0.85",
            Precision::Nanosecond,
        )
        .await
        .unwrap();

    let db_name = "foo";

    // this will pull from https://github.com/influxdata/influxdb3_plugins/blob/main/examples/wal_plugin/wal_plugin.py
    let plugin_name = "gh:examples/wal_plugin/wal_plugin.py";
    let lp = "test_input,tag1=tag1_value,tag2=tag2_value field1=1i 500";

    // Run the test using the fluent API
    let result = server
        .test_wal_plugin(db_name, plugin_name)
        .with_line_protocol(lp)
        .run()
        .unwrap();

    debug!(result = ?result, "test wal plugin");

    let expected_result = r#"{
  "log_lines": [
    "INFO: wal_plugin.py done"
  ],
  "database_writes": {
    "foo": [
      "write_reports,table_name=test_input row_count=1i"
    ]
  },
  "errors": []
}"#;
    let expected_result = serde_json::from_str::<serde_json::Value>(expected_result).unwrap();
    assert_eq!(result, expected_result);
}
#[test_log::test(tokio::test)]
async fn test_request_plugin_and_trigger() {
    let plugin_code = r#"
import json

def process_request(influxdb3_local, query_parameters, request_headers, request_body, args=None):
    for k, v in query_parameters.items():
        influxdb3_local.info(f"query_parameters: {k}={v}")
    for k, v in request_headers.items():
        influxdb3_local.info(f"request_headers: {k}={v}")

    request_data = json.loads(request_body)

    influxdb3_local.info("parsed JSON request body:", request_data)

    # write the data to the database
    line = LineBuilder("request_data").tag("tag1", "tag1_value").int64_field("field1", 1)
    # get a string of the line to return as the body
    line_str = line.build()

    influxdb3_local.write(line)

    return {"status": "ok", "line": line_str}
"#;

    let (temp_dir, plugin_path) = create_plugin_in_temp_dir(plugin_code);

    let plugin_dir = temp_dir.path().to_str().unwrap();
    let plugin_filename = plugin_path.file_name().unwrap().to_str().unwrap();

    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir)
        .spawn()
        .await;
    let db_name = "foo";

    // Setup: create database and plugin
    server.create_database(db_name).run().unwrap();

    let trigger_path = "foo";
    // creating the trigger should enable it
    let result = server
        .create_trigger(db_name, trigger_path, plugin_filename, "request:bar")
        .add_trigger_argument("test_arg=hello")
        .run()
        .unwrap();

    debug!(result = ?result, "create trigger");
    assert_contains!(&result, "Trigger foo created successfully");

    // send an HTTP request to the server
    let client = server.http_client();
    let response = client
        .post(format!("{}/api/v3/engine/bar", server.client_addr()))
        .header("Content-Type", "application/json")
        .query(&[("q1", "whatevs")])
        .body(r#"{"hello": "world"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let body = response.text().await.unwrap();
    let body = serde_json::from_str::<serde_json::Value>(&body).unwrap();

    // the plugin was just supposed to return the line that it wrote into the DB
    assert_eq!(
        body,
        json!({"status": "ok", "line": "request_data,tag1=tag1_value field1=1i"})
    );

    // query to see if the plugin did the write into the DB
    let val = server
        .query_sql(db_name)
        .with_sql("SELECT tag1, field1 FROM request_data")
        .run()
        .unwrap();

    assert_eq!(val, json!([{"tag1": "tag1_value", "field1": 1}]));

    // now update it to make sure that it reloads
    let plugin_code = r#"
import json

def process_request(influxdb3_local, query_parameters, request_headers, request_body, args=None):
    return {"status": "updated"}
"#;
    // Rewrite the file.
    let mut file = fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(&plugin_path)
        .unwrap();
    file.write_all(plugin_code.as_bytes()).unwrap();

    // send an HTTP request to the server
    let response = client
        .post(format!("{}/api/v3/engine/bar", server.client_addr()))
        .header("Content-Type", "application/json")
        .query(&[("q1", "whatevs")])
        .body(r#"{"hello": "world"}"#)
        .send()
        .await
        .unwrap();

    let body = response.text().await.unwrap();
    let body = serde_json::from_str::<serde_json::Value>(&body).unwrap();
    assert_eq!(body, json!({"status": "updated"}));
}
#[test_log::test(tokio::test)]
async fn test_flask_string_response() {
    let plugin_code = r#"
def process_request(influxdb3_local, query_parameters, request_headers, request_body, args=None):
    # Return a simple string (should become HTML with 200 status)
    return "Hello, World!"
"#;

    let (temp_dir, plugin_path) = create_plugin_in_temp_dir(plugin_code);

    let plugin_dir = temp_dir.path().to_str().unwrap();
    let plugin_filename = plugin_path.file_name().unwrap().to_str().unwrap();

    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir)
        .spawn()
        .await;
    let db_name = "flask_test_string";

    // Setup: create database and plugin
    server
        .create_database(db_name)
        .run()
        .expect("Failed to create database");

    let trigger_path = "string_test";
    server
        .create_trigger(db_name, trigger_path, plugin_filename, "request:test_route")
        .run()
        .expect("Failed to create trigger");

    // Send request to test string response
    let client = server.http_client();
    let response = client
        .get(format!("{}/api/v3/engine/test_route", server.client_addr()))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    assert_eq!(response.headers().get("content-type").unwrap(), "text/html");
    let body = response.text().await.unwrap();
    assert_eq!(body, "Hello, World!");
}

#[test_log::test(tokio::test)]
async fn test_flask_dict_json_response() {
    let plugin_code = r#"
def process_request(influxdb3_local, query_parameters, request_headers, request_body, args=None):
    # Return a dictionary (should be converted to JSON)
    return {"message": "Hello, World!", "status": "success"}
"#;
    let (temp_dir, plugin_path) = create_plugin_in_temp_dir(plugin_code);

    let plugin_dir = temp_dir.path().to_str().unwrap();
    let plugin_filename = plugin_path.file_name().unwrap().to_str().unwrap();

    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir)
        .spawn()
        .await;
    let db_name = "flask_test_dict";

    // Setup: create database and plugin
    server
        .create_database(db_name)
        .run()
        .expect("Failed to create database");

    let trigger_path = "dict_test";
    server
        .create_trigger(db_name, trigger_path, plugin_filename, "request:test_route")
        .run()
        .expect("Failed to create trigger");

    // Send request to test dict/JSON response
    let client = server.http_client();
    let response = client
        .get(format!("{}/api/v3/engine/test_route", server.client_addr()))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "application/json"
    );
    let body = response.json::<serde_json::Value>().await.unwrap();
    assert_eq!(
        body,
        json!({"message": "Hello, World!", "status": "success"})
    );
}

#[test_log::test(tokio::test)]
async fn test_flask_tuple_response_with_status() {
    let plugin_code = r#"
def process_request(influxdb3_local, query_parameters, request_headers, request_body, args=None):
    # Return a tuple with content and status code
    return "Created successfully", 201
"#;
    let (temp_dir, plugin_path) = create_plugin_in_temp_dir(plugin_code);

    let plugin_dir = temp_dir.path().to_str().unwrap();
    let plugin_filename = plugin_path.file_name().unwrap().to_str().unwrap();

    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir)
        .spawn()
        .await;
    let db_name = "flask_test_tuple_status";

    // Setup: create database and plugin
    server
        .create_database(db_name)
        .run()
        .expect("Failed to create database");

    let trigger_path = "tuple_status_test";
    server
        .create_trigger(db_name, trigger_path, plugin_filename, "request:test_route")
        .run()
        .expect("Failed to create trigger");

    // Send request to test tuple with status response
    let client = server.http_client();
    let response = client
        .get(format!("{}/api/v3/engine/test_route", server.client_addr()))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 201);
    assert_eq!(response.headers().get("content-type").unwrap(), "text/html");
    let body = response.text().await.unwrap();
    assert_eq!(body, "Created successfully");
}

#[test_log::test(tokio::test)]
async fn test_flask_tuple_response_with_headers() {
    let plugin_code = r#"
def process_request(influxdb3_local, query_parameters, request_headers, request_body, args=None):
    # Return a tuple with content and headers
    return "Custom Content-Type", {"Content-Type": "text/plain", "X-Custom-Header": "test-value"}
"#;
    let (temp_dir, plugin_path) = create_plugin_in_temp_dir(plugin_code);

    let plugin_dir = temp_dir.path().to_str().unwrap();
    let plugin_filename = plugin_path.file_name().unwrap().to_str().unwrap();

    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir)
        .spawn()
        .await;
    let db_name = "flask_test_tuple_headers";

    // Setup: create database and plugin
    server
        .create_database(db_name)
        .run()
        .expect("Failed to create database");

    let trigger_path = "tuple_headers_test";
    server
        .create_trigger(db_name, trigger_path, plugin_filename, "request:test_route")
        .run()
        .expect("Failed to create trigger");

    // Send request to test tuple with headers response
    let client = server.http_client();
    let response = client
        .get(format!("{}/api/v3/engine/test_route", server.client_addr()))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "text/plain"
    );
    assert_eq!(
        response.headers().get("x-custom-header").unwrap(),
        "test-value"
    );
    let body = response.text().await.unwrap();
    assert_eq!(body, "Custom Content-Type");
}

#[test_log::test(tokio::test)]
async fn test_flask_tuple_response_with_status_and_headers() {
    let plugin_code = r#"
def process_request(influxdb3_local, query_parameters, request_headers, request_body, args=None):
    # Return a tuple with content, status, and headers
    return "Not Found", 404, {"Content-Type": "text/plain", "X-Error-Code": "NOT_FOUND"}
"#;
    let (temp_dir, plugin_path) = create_plugin_in_temp_dir(plugin_code);

    let plugin_dir = temp_dir.path().to_str().unwrap();
    let plugin_filename = plugin_path.file_name().unwrap().to_str().unwrap();

    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir)
        .spawn()
        .await;
    let db_name = "flask_test_tuple_status_headers";

    // Setup: create database and plugin
    server
        .create_database(db_name)
        .run()
        .expect("Failed to create database");

    let trigger_path = "tuple_status_headers_test";
    server
        .create_trigger(db_name, trigger_path, plugin_filename, "request:test_route")
        .run()
        .expect("Failed to create trigger");

    // Send request to test tuple with status and headers response
    let client = server.http_client();
    let response = client
        .get(format!("{}/api/v3/engine/test_route", server.client_addr()))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 404);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "text/plain"
    );
    assert_eq!(response.headers().get("x-error-code").unwrap(), "NOT_FOUND");
    let body = response.text().await.unwrap();
    assert_eq!(body, "Not Found");
}

#[test_log::test(tokio::test)]
async fn test_flask_list_json_response() {
    let plugin_code = r#"
def process_request(influxdb3_local, query_parameters, request_headers, request_body, args=None):
    # Return a list (should be converted to JSON array)
    return ["item1", "item2", "item3"]
"#;
    let (temp_dir, plugin_path) = create_plugin_in_temp_dir(plugin_code);

    let plugin_dir = temp_dir.path().to_str().unwrap();
    let plugin_filename = plugin_path.file_name().unwrap().to_str().unwrap();

    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir)
        .spawn()
        .await;
    let db_name = "flask_test_list";

    // Setup: create database and plugin
    server
        .create_database(db_name)
        .run()
        .expect("Failed to create database");

    let trigger_path = "list_test";
    server
        .create_trigger(db_name, trigger_path, plugin_filename, "request:test_route")
        .run()
        .expect("Failed to create trigger");

    // Send request to test list/JSON response
    let client = server.http_client();
    let response = client
        .get(format!("{}/api/v3/engine/test_route", server.client_addr()))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "application/json"
    );
    let body = response.json::<serde_json::Value>().await.unwrap();
    assert_eq!(body, json!(["item1", "item2", "item3"]));
}

#[test_log::test(tokio::test)]
async fn test_flask_iterator_response() {
    let plugin_code = r#"
def process_request(influxdb3_local, query_parameters, request_headers, request_body, args=None):
    # Return a generator that yields strings
    def generate_content():
        yield "Line 1\n"
        yield "Line 2\n"
        yield "Line 3\n"

    return generate_content()
"#;
    let (temp_dir, plugin_path) = create_plugin_in_temp_dir(plugin_code);

    let plugin_dir = temp_dir.path().to_str().unwrap();
    let plugin_filename = plugin_path.file_name().unwrap().to_str().unwrap();

    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir)
        .spawn()
        .await;
    let db_name = "flask_test_iterator";

    // Setup: create database and plugin
    server
        .create_database(db_name)
        .run()
        .expect("Failed to create database");

    let trigger_path = "iterator_test";
    server
        .create_trigger(db_name, trigger_path, plugin_filename, "request:test_route")
        .run()
        .expect("Failed to create trigger");

    // Send request to test iterator/generator response
    let client = server.http_client();
    let response = client
        .get(format!("{}/api/v3/engine/test_route", server.client_addr()))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    assert_eq!(response.headers().get("content-type").unwrap(), "text/html");
    let body = response.text().await.unwrap();
    assert_eq!(body, "Line 1\nLine 2\nLine 3\n");
}
#[test_log::test(tokio::test)]
async fn test_flask_response_object() {
    let plugin_code = r#"
def process_request(influxdb3_local, query_parameters, request_headers, request_body, args=None):
    # Creating a mock Flask Response object
    class FlaskResponse:
        def __init__(self, response, status=200, headers=None):
            self.response = response
            self.status_code = status
            self.headers = headers or {}

        def get_data(self):
            return self.response

        def __flask_response__(self):
            return True

    # Return a Flask Response object
    response = FlaskResponse(
        "Custom Flask Response",
        status=202,
        headers={"Content-Type": "text/custom", "X-Generated-By": "FlaskResponse"}
    )
    return response
"#;
    let (temp_dir, plugin_path) = create_plugin_in_temp_dir(plugin_code);

    let plugin_dir = temp_dir.path().to_str().unwrap();
    let plugin_filename = plugin_path.file_name().unwrap().to_str().unwrap();

    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir)
        .spawn()
        .await;
    let db_name = "flask_test_response_obj";

    // Setup: create database and trigger
    server.create_database(db_name).run().unwrap();

    let trigger_path = "response_obj_test";
    server
        .create_trigger(db_name, trigger_path, plugin_filename, "request:test_route")
        .run()
        .unwrap();

    // Send request to test Flask Response object
    let client = server.http_client();
    let response = client
        .get(format!("{}/api/v3/engine/test_route", server.client_addr()))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 202);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "text/custom"
    );
    assert_eq!(
        response.headers().get("x-generated-by").unwrap(),
        "FlaskResponse"
    );
    let body = response.text().await.unwrap();
    assert_eq!(body, "Custom Flask Response");
}

#[test_log::test(tokio::test)]
async fn test_flask_json_dict_with_status_in_tuple() {
    let plugin_code = r#"
def process_request(influxdb3_local, query_parameters, request_headers, request_body, args=None):
    # Return a tuple with a dictionary and status code
    return {"error": "Not Found", "code": 404}, 404
"#;
    let (temp_dir, plugin_path) = create_plugin_in_temp_dir(plugin_code);

    let plugin_dir = temp_dir.path().to_str().unwrap();
    let plugin_filename = plugin_path.file_name().unwrap().to_str().unwrap();

    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir)
        .spawn()
        .await;
    let db_name = "flask_test_json_status";

    // Setup: create database and trigger
    server.create_database(db_name).run().unwrap();

    let trigger_path = "json_status_test";
    server
        .create_trigger(db_name, trigger_path, plugin_filename, "request:test_route")
        .run()
        .unwrap();

    // Send request to test JSON dict with status
    let client = server.http_client();
    let response = client
        .get(format!("{}/api/v3/engine/test_route", server.client_addr()))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 404);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "application/json"
    );
    let body = response.json::<serde_json::Value>().await.unwrap();
    assert_eq!(body, json!({"error": "Not Found", "code": 404}));
}

#[test_log::test(tokio::test)]
async fn test_trigger_create_validates_file_present() {
    let plugin_dir = TempDir::new().unwrap();

    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir.path().to_str().unwrap())
        .spawn()
        .await;
    let db_name = "foo";

    // Setup: create database
    server.create_database(db_name).run().unwrap();

    let trigger_path = "foo";
    // creating the trigger should return a 404 error from github
    let result = server
        .create_trigger(db_name, trigger_path, "gh:not_a_file.py", "request:foo")
        .add_trigger_argument("test_arg=hello")
        .run();

    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("error reading file from Github: 404 Not Found"));
}

fn check_logs(response: &Value, expected_logs: &[&str]) {
    assert!(
        response["errors"].as_array().unwrap().is_empty(),
        "Unexpected errors in response {:#?}, expected logs {:#?}",
        response,
        expected_logs
    );
    let logs = response["log_lines"].as_array().unwrap();
    assert_eq!(
        expected_logs, logs,
        "mismatched log lines, expected {:#?}, got {:#?}, errors are {:#?}",
        expected_logs, logs, response["errors"]
    );
}
#[test_log::test(tokio::test)]
async fn test_basic_cache_functionality() {
    // Create plugin file that uses the cache
    let plugin_code = r#"
def process_scheduled_call(influxdb3_local, schedule_time, args=None):
    # Store a value in the cache
    influxdb3_local.cache.put("test_key", "test_value")

    # Retrieve the value
    value = influxdb3_local.cache.get("test_key")
    influxdb3_local.info(f"Retrieved value: {value}")

    # Verify the value matches what we stored
    assert value == "test_value", f"Expected 'test_value', got {value}"
    influxdb3_local.info("Cache test passed")"#;
    let (temp_dir, plugin_path) = create_plugin_in_temp_dir(plugin_code);

    let plugin_dir = temp_dir.path().to_str().unwrap();
    let plugin_name = plugin_path.file_name().unwrap().to_str().unwrap();

    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir)
        .spawn()
        .await;

    let db_name = "foo";

    // Setup: create database
    server.create_database(db_name).run().unwrap();

    // Run the schedule plugin test
    let res = server
        .test_schedule_plugin(db_name, plugin_name, "* * * * * *")
        .with_cache_name("test_basic_cache")
        .run()
        .unwrap();

    // Check that the cache operations were successful
    let expected_logs = [
        "INFO: Retrieved value: test_value",
        "INFO: Cache test passed",
    ];

    check_logs(&res, &expected_logs);
}

#[test_log::test(tokio::test)]
async fn test_cache_ttl() {
    // Create plugin file that tests cache TTL
    let plugin_code = r#"
import time

def process_scheduled_call(influxdb3_local, schedule_time, args=None):
    # Store a value with 1 second TTL
    influxdb3_local.cache.put("ttl_key", "expires_soon", ttl=1.0)

    # Immediately check - should exist
    value = influxdb3_local.cache.get("ttl_key")
    influxdb3_local.info(f"Immediate retrieval: {value}")

    # Wait for TTL to expire
    influxdb3_local.info("Waiting for TTL to expire...")
    time.sleep(1.5)

    # Check again - should be gone
    expired_value = influxdb3_local.cache.get("ttl_key")
    influxdb3_local.info(f"After expiration: {expired_value}")

    # Verify the value is None after TTL expiration
    assert expired_value is None, f"Expected None after TTL expiration, got {expired_value}"
    influxdb3_local.info("TTL test passed")"#;

    let (temp_dir, plugin_path) = create_plugin_in_temp_dir(plugin_code);

    let plugin_dir = temp_dir.path().to_str().unwrap();
    let plugin_name = plugin_path.file_name().unwrap().to_str().unwrap();

    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir)
        .spawn()
        .await;

    let db_name = "foo";

    // Setup: create database
    server.create_database(db_name).run().unwrap();

    // Run the schedule plugin test
    let res = server
        .test_schedule_plugin(db_name, plugin_name, "* * * * * *")
        .with_cache_name("ttl_test_cache")
        .run()
        .unwrap();

    // Check logs to verify TTL functionality
    let expected_logs = [
        "INFO: Immediate retrieval: expires_soon",
        "INFO: Waiting for TTL to expire...",
        "INFO: After expiration: None",
        "INFO: TTL test passed",
    ];

    check_logs(&res, &expected_logs);
}

#[test_log::test(tokio::test)]
async fn test_cache_namespaces() {
    // Create plugin file that tests different cache namespaces
    let plugin_code = r#"
def process_scheduled_call(influxdb3_local, schedule_time, args=None):
    # Store value in local (trigger) cache
    influxdb3_local.cache.put("namespace_key", "trigger_value", use_global=False)

    # Store different value in global cache with same key
    influxdb3_local.cache.put("namespace_key", "global_value", use_global=True)

    # Retrieve from both caches
    trigger_value = influxdb3_local.cache.get("namespace_key", use_global=False)
    global_value = influxdb3_local.cache.get("namespace_key", use_global=True)

    influxdb3_local.info(f"Trigger cache value: {trigger_value}")
    influxdb3_local.info(f"Global cache value: {global_value}")

    # Verify namespace isolation
    assert trigger_value == "trigger_value", f"Expected 'trigger_value', got {trigger_value}"
    assert global_value == "global_value", f"Expected 'global_value', got {global_value}"
    influxdb3_local.info("Cache namespace test passed")"#;

    let (temp_dir, plugin_path) = create_plugin_in_temp_dir(plugin_code);

    let plugin_dir = temp_dir.path().to_str().unwrap();
    let plugin_name = plugin_path.file_name().unwrap().to_str().unwrap();

    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir)
        .spawn()
        .await;

    let db_name = "foo";

    // Setup: create database
    server.create_database(db_name).run().unwrap();

    // Run the schedule plugin test
    let res = server
        .test_schedule_plugin(db_name, plugin_name, "* * * * * *")
        .with_cache_name("test_cache_namespaces")
        .run()
        .unwrap();

    // Check logs to verify namespace isolation
    let expected_logs = [
        "INFO: Trigger cache value: trigger_value",
        "INFO: Global cache value: global_value",
        "INFO: Cache namespace test passed",
    ];

    check_logs(&res, &expected_logs);
}

#[test_log::test(tokio::test)]
async fn test_cache_persistence_across_runs() {
    // Create plugin file that stores a value in the cache
    let first_plugin_code = r#"
def process_scheduled_call(influxdb3_local, schedule_time, args=None):
    # Store a value that should persist across test runs with the same cache name
    influxdb3_local.cache.put("persistent_key", "I should persist")
    influxdb3_local.info("Stored value in shared test cache")"#;

    let (temp_dir, first_plugin_file) = create_plugin_in_temp_dir(first_plugin_code);

    let plugin_dir = temp_dir.path().to_str().unwrap();

    // Create second plugin file that retrieves the value
    let second_plugin_code = r#"
def process_scheduled_call(influxdb3_local, schedule_time, args=None):
    # Retrieve the value from the previous run
    value = influxdb3_local.cache.get("persistent_key")
    influxdb3_local.info(f"Retrieved value from previous run: {value}")

    # Verify the value persisted
    assert value == "I should persist", f"Expected 'I should persist', got {value}"
    influxdb3_local.info("Cache persistence test passed")"#;

    let second_plugin_file =
        write_plugin_to_temp_dir(&temp_dir, "second_plugin.py", second_plugin_code);

    let first_plugin_name = first_plugin_file.file_name().unwrap().to_str().unwrap();
    let second_plugin_name = second_plugin_file.file_name().unwrap().to_str().unwrap();

    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir)
        .spawn()
        .await;

    let db_name = "foo";
    let cache_name = "shared_test_cache";

    // Setup: create database
    server.create_database(db_name).run().unwrap();

    // First run - store the value
    let first_res = server
        .test_schedule_plugin(db_name, first_plugin_name, "* * * * * *")
        .with_cache_name(cache_name)
        .run()
        .unwrap();

    let expected_logs = ["INFO: Stored value in shared test cache"];
    check_logs(&first_res, &expected_logs);

    // Second run - retrieve the value
    let second_res = server
        .test_schedule_plugin(db_name, second_plugin_name, "*/5 * * * * *")
        .with_cache_name(cache_name)
        .run()
        .unwrap();

    // Check logs to verify persistence
    let expected_logs = [
        "INFO: Retrieved value from previous run: I should persist",
        "INFO: Cache persistence test passed",
    ];

    check_logs(&second_res, &expected_logs);
}

#[test_log::test(tokio::test)]
async fn test_cache_deletion() {
    // Create plugin file that tests cache deletion
    let plugin_code = r#"
def process_scheduled_call(influxdb3_local, schedule_time, args=None):
    # Store some values
    influxdb3_local.cache.put("delete_key", "delete me")

    # Verify the value exists
    before_delete = influxdb3_local.cache.get("delete_key")
    influxdb3_local.info(f"Before deletion: {before_delete}")

    # Delete the value
    deleted = influxdb3_local.cache.delete("delete_key")
    influxdb3_local.info(f"Deletion result: {deleted}")

    # Try to retrieve the deleted value
    after_delete = influxdb3_local.cache.get("delete_key")
    influxdb3_local.info(f"After deletion: {after_delete}")

    # Verify deletion was successful
    assert deleted is True, f"Expected True for successful deletion, got {deleted}"
    assert after_delete is None, f"Expected None after deletion, got {after_delete}"

    # Test deleting non-existent key
    non_existent = influxdb3_local.cache.delete("non_existent_key")
    influxdb3_local.info(f"Deleting non-existent key: {non_existent}")
    assert non_existent is False, f"Expected False for non-existent key, got {non_existent}"

    influxdb3_local.info("Cache deletion test passed")"#;

    let (temp_dir, plugin_path) = create_plugin_in_temp_dir(plugin_code);

    let plugin_dir = temp_dir.path().to_str().unwrap();
    let plugin_name = plugin_path.file_name().unwrap().to_str().unwrap();

    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir)
        .spawn()
        .await;

    let db_name = "foo";

    // Setup: create database
    server.create_database(db_name).run().unwrap();

    // Run the schedule plugin test
    let res = server
        .test_schedule_plugin(db_name, plugin_name, "* * * * * *")
        .with_cache_name("test_cache_deletion")
        .run()
        .unwrap();

    // Check logs to verify deletion functionality
    let expected_logs = [
        "INFO: Before deletion: delete me",
        "INFO: Deletion result: True",
        "INFO: After deletion: None",
        "INFO: Deleting non-existent key: False",
        "INFO: Cache deletion test passed",
    ];

    check_logs(&res, &expected_logs);
}

#[test_log::test(tokio::test)]
async fn test_cache_with_wal_plugin() {
    // Create plugin file that uses cache in a WAL plugin
    let plugin_code = r#"
def process_writes(influxdb3_local, table_batches, args=None):
    # Count the number of records processed
    record_count = sum(len(batch['rows']) for batch in table_batches)

    # Get previous count from cache or default to 0
    previous_count = influxdb3_local.cache.get("processed_records") or 0
    if previous_count == 0:
        influxdb3_local.info("First run, initializing count")
    else:
        influxdb3_local.info(f"Previous count: {previous_count}")

    # Update the count in cache
    new_count = previous_count + record_count
    influxdb3_local.cache.put("processed_records", new_count)
    influxdb3_local.info(f"Updated count: {new_count}")

    # We're not modifying any records for this test
    return table_batches"#;

    let (temp_dir, plugin_path) = create_plugin_in_temp_dir(plugin_code);

    let plugin_dir = temp_dir.path().to_str().unwrap();
    let plugin_name = plugin_path.file_name().unwrap().to_str().unwrap();

    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir)
        .spawn()
        .await;

    // Make sure each test creates its own database
    let db_name = "foo";
    let cache_name = "test_cache";

    // Setup: create database
    server.create_database(db_name).run().unwrap();

    // First run with 3 data points
    let first_lp =
        "cpu,host=host1 usage=0.75\ncpu,host=host2 usage=0.82\ncpu,host=host3 usage=0.91";

    // First test run
    let first_res = server
        .test_wal_plugin(db_name, plugin_name)
        .with_line_protocol(first_lp)
        .with_cache_name(cache_name)
        .run()
        .unwrap();

    // Check first run logs
    let init_log = "INFO: First run, initializing count";
    let update_log = "INFO: Updated count: 3";

    check_logs(&first_res, &[init_log, update_log]);

    // Second run with 2 more data points
    let second_lp = "cpu,host=host4 usage=0.65\ncpu,host=host5 usage=0.72";

    // Second test run
    let second_res = server
        .test_wal_plugin(db_name, plugin_name)
        .with_line_protocol(second_lp)
        .with_cache_name(cache_name)
        .run()
        .unwrap();

    // Check second run logs
    let prev_log = "INFO: Previous count: 3";
    let update_log = "INFO: Updated count: 5";
    check_logs(&second_res, &[prev_log, update_log]);
}

#[test_log::test(tokio::test)]
async fn test_trigger_cache_cleanup() {
    // Create a scheduled trigger script that writes different data based on whether it has cache data
    let trigger_script = r#"
def process_scheduled_call(influxdb3_local, schedule_time, args=None):
    # Check if we've run before by looking for a cache value
    run_count = influxdb3_local.cache.get("run_counter")

    influxdb3_local.info(f"run count {run_count}")

    if run_count is None:
        line = LineBuilder("cache_test").tag("test", "cleanup").int64_field("count", 1)
        influxdb3_local.write(line)
        influxdb3_local.cache.put("run_counter", 1)
    else:
        influxdb3_local.cache.put("run_counter", run_count + 1)"#;

    let (temp_dir, plugin_path) = create_plugin_in_temp_dir(trigger_script);

    let plugin_dir = temp_dir.path().to_str().unwrap();
    let trigger_filename = plugin_path.file_name().unwrap().to_str().unwrap();

    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir)
        .spawn()
        .await;

    let db_name = "foo";
    let trigger_name = "cache_cleanup_trigger";

    // Setup: create database
    server.create_database(db_name).run().unwrap();

    // Create scheduled trigger
    server
        .create_trigger(db_name, trigger_name, trigger_filename, "cron:* * * * * *")
        .run()
        .unwrap();

    // Wait for trigger to run several times
    tokio::time::sleep(std::time::Duration::from_millis(3100)).await;

    // Query to see what values were written before disabling
    let first_query_result = server
        .query_sql(db_name)
        .with_sql("SELECT count(*) FROM cache_test")
        .run()
        .unwrap();

    // There should be a single row
    assert_eq!(json!([{"count(*)":1}]), first_query_result);

    // Disable trigger (which should clear its cache)
    server.disable_trigger(db_name, trigger_name).run().unwrap();

    // Re-enable trigger
    server.enable_trigger(db_name, trigger_name).run().unwrap();

    // Wait for trigger to run again
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;

    // Query results after re-enabling
    let second_query_result = server
        .query_sql(db_name)
        .with_sql("SELECT count(*) FROM cache_test")
        .run()
        .unwrap();

    // If cache was cleared, we should see a second row
    assert_eq!(json!([{"count(*)":2}]), second_query_result);
}

#[test_log::test(tokio::test)]
async fn test_complex_object_caching() {
    // Create plugin file that tests caching of complex Python objects
    let plugin_code = r#"
def process_scheduled_call(influxdb3_local, schedule_time, args=None):
    # Create a complex object (nested dictionaries and lists)
    complex_obj = {
        "string_key": "value",
        "int_key": 42,
        "float_key": 3.14,
        "bool_key": True,
        "list_key": [1, 2, 3, "four", 5.0],
        "nested_dict": {
            "inner_key": "inner_value",
            "inner_list": [{"a": 1}, {"b": 2}]
        }
    }

    # Store in cache
    influxdb3_local.cache.put("complex_obj", complex_obj)
    influxdb3_local.info("Stored complex object in cache")

    # Retrieve from cache
    retrieved_obj = influxdb3_local.cache.get("complex_obj")

    # Verify structure was preserved
    influxdb3_local.info(f"Retrieved object type: {type(retrieved_obj).__name__}")
    influxdb3_local.info(f"Retrieved nested_dict type: {type(retrieved_obj['nested_dict']).__name__}")
    influxdb3_local.info(f"Retrieved list_key type: {type(retrieved_obj['list_key']).__name__}")

    # Test with some assertions
    assert retrieved_obj["string_key"] == "value", "String value mismatch"
    assert retrieved_obj["int_key"] == 42, "Integer value mismatch"
    assert retrieved_obj["nested_dict"]["inner_key"] == "inner_value", "Nested value mismatch"
    assert retrieved_obj["list_key"][3] == "four", "List value mismatch"
    assert retrieved_obj["nested_dict"]["inner_list"][1]["b"] == 2, "Deeply nested value mismatch"

    influxdb3_local.info("Complex object verification passed")"#;

    let (temp_dir, plugin_path) = create_plugin_in_temp_dir(plugin_code);

    let plugin_dir = temp_dir.path().to_str().unwrap();
    let plugin_name = plugin_path.file_name().unwrap().to_str().unwrap();

    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir)
        .spawn()
        .await;

    let db_name = "foo";

    // Setup: create database
    server.create_database(db_name).run().unwrap();

    // Run the schedule plugin test
    let res = server
        .test_schedule_plugin(db_name, plugin_name, "* * * * * *")
        .with_cache_name("test_complex_objects")
        .run()
        .unwrap();

    // Check logs to verify complex object preservation
    let expected_logs = [
        "INFO: Stored complex object in cache",
        "INFO: Retrieved object type: dict",
        "INFO: Retrieved nested_dict type: dict",
        "INFO: Retrieved list_key type: list",
        "INFO: Complex object verification passed",
    ];

    check_logs(&res, &expected_logs);
}
#[test_log::test(tokio::test)]
async fn write_and_query_via_stdin() {
    let server = TestServer::spawn().await;
    let db_name = "foo";

    // Write data via stdin
    let result = server
        .write(db_name)
        .run_with_stdin("bar,tag1=1,tag2=2 field1=1,field2=2 0")
        .unwrap();

    assert_eq!("success", result);
    debug!(result = ?result, "wrote data to database");

    // Query data
    let result = server
        .query_sql(db_name)
        .with_sql("SELECT * FROM bar")
        .run()
        .unwrap();

    debug!(result = ?result, "queried data from database");
    assert_eq!(
        json!([{"field1":1.0,"field2":2.0,"tag1":"1","tag2":"2","time":"1970-01-01T00:00:00"}]),
        result
    );
}

#[test_log::test(tokio::test)]
async fn write_and_query_via_file() {
    let server = TestServer::spawn().await;
    let db_name = "foo";

    // Write data via file
    let result = server
        .write(db_name)
        .with_file("tests/server/fixtures/file.lp")
        .run()
        .unwrap();

    assert_eq!("success", result);
    debug!(result = ?result, "wrote data to database");

    // Query data via file
    let result = server
        .query_sql(db_name)
        .with_query_file("tests/server/fixtures/file.sql")
        .run()
        .unwrap();

    debug!(result = ?result, "queried data from database");
    assert_eq!(
        json!([{"field1":1.0,"field2":2.0,"tag1":"1","tag2":"2","time":"1970-01-01T00:00:00"}]),
        result
    );
}

#[test_log::test(tokio::test)]
async fn write_and_query_via_string() {
    let server = TestServer::spawn().await;
    let db_name = "foo";

    // Write data directly
    let result = server
        .write(db_name)
        .with_line_protocol("bar,tag1=1,tag2=2 field1=1,field2=2 0")
        .run()
        .unwrap();

    assert_eq!("success", result);
    debug!(result = ?result, "wrote data to database");

    // Query data directly
    let result = server
        .query_sql(db_name)
        .with_sql("SELECT * FROM bar")
        .run()
        .unwrap();

    debug!(result = ?result, "queried data from database");
    assert_eq!(
        json!([{"field1":1.0,"field2":2.0,"tag1":"1","tag2":"2","time":"1970-01-01T00:00:00"}]),
        result
    );
}

#[test_log::test(tokio::test)]
async fn write_with_precision_arg() {
    let server = TestServer::spawn().await;
    let db_name = "foo";
    let table_name = "bar";

    struct TestCase {
        name: &'static str,
        precision: Option<&'static str>,
        expected: &'static str,
    }

    let tests = vec![
        TestCase {
            name: "default precision is seconds",
            precision: None,
            expected: "1970-01-01T00:00:01",
        },
        TestCase {
            name: "set seconds precision",
            precision: Some("s"),
            expected: "1970-01-01T00:00:01",
        },
        TestCase {
            name: "set milliseconds precision",
            precision: Some("ms"),
            expected: "1970-01-01T00:00:00.001",
        },
        TestCase {
            name: "set microseconds precision",
            precision: Some("us"),
            expected: "1970-01-01T00:00:00.000001",
        },
        TestCase {
            name: "set nanoseconds precision",
            precision: Some("ns"),
            expected: "1970-01-01T00:00:00.000000001",
        },
    ];

    for test in tests {
        let TestCase {
            name,
            precision,
            expected,
        } = test;
        let name_tag = name.replace(' ', "_");
        let lp = format!("{table_name},name={name_tag} theanswer=42 1");

        // Build write query with precision if specified
        let mut write_query = server.write(db_name).with_line_protocol(&lp);
        if let Some(prec) = precision {
            write_query = write_query.with_precision(prec);
        }

        // Execute write
        write_query.run().unwrap();

        // Verify result
        let result = server
            .query_sql(db_name)
            .with_sql(format!(
                "SELECT * FROM {table_name} WHERE name='{name_tag}'"
            ))
            .run()
            .unwrap();
        assert_eq!(
            result,
            json!([{
                "name": name_tag,
                "theanswer": 42.0,
                "time": expected,
            }]),
            "test failed: {name}",
        );
    }

    // Test invalid precision
    let lp = format!("{table_name},name=invalid_precision theanswer=42 1");
    let result = server
        .write(db_name)
        .with_line_protocol(&lp)
        .with_precision("fake")
        .run();

    // This should result in an error
    assert!(result.is_err());
    insta::assert_snapshot!("invalid_precision", format!("{}", result.unwrap_err()));
}

#[test_log::test(tokio::test)]
async fn test_wal_overwritten() {
    let tmp_dir = TempDir::new().unwrap();
    let tmp_dir_path = tmp_dir.path().to_str().unwrap();
    let mut p1 = TestServer::configure()
        .with_node_id("node-0")
        .with_object_store_dir(tmp_dir_path)
        .spawn()
        .await;

    // perform a write so that a WAL file is written/wal folder created:
    p1.write("foo")
        .with_line_protocol("bar,process=p1 f1=true")
        .run()
        .unwrap();

    // start another process with the same node id:
    let mut p2 = TestServer::configure()
        .with_node_id("node-0")
        .with_object_store_dir(tmp_dir_path)
        .spawn()
        .await;

    // perform a write from p2 that will create the next wal file:
    p2.write("foo")
        .with_line_protocol("bar,process=p2 f1=true")
        .run()
        .unwrap();

    // perform another write from p1 which will fail since the next WAL file was already written:
    let result = p1
        .write("foo")
        .with_line_protocol("bar,process=p1 f1=true")
        .run()
        .unwrap_err();

    debug!(?result, "second write to p1 that should fail");

    assert_contains!(
        result.to_string(),
        "another process has written to the WAL ahead of this one"
    );

    // give p1 some time to shutdown:
    for _ in 0..10 {
        if p1.is_stopped() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        p1.is_stopped(),
        "p1 should have stopped due to internal shutdown"
    );
    assert!(!p2.is_stopped(), "p2 should not be stopped");
}

#[test_log::test(tokio::test)]
async fn test_create_admin_token() {
    let server = TestServer::configure()
        .with_auth()
        .with_no_admin_token()
        .spawn()
        .await;
    let args = &["--tls-ca", "../testing-certs/rootCA.pem"];
    let result = server
        .run(vec!["create", "token", "--admin"], args)
        .unwrap();
    println!("{:?}", result);
    assert_contains!(&result, "New token created successfully!");
}

#[test_log::test(tokio::test)]
async fn test_create_admin_token_json_format() {
    let server = TestServer::configure()
        .with_auth()
        .with_no_admin_token()
        .spawn()
        .await;
    let args = &[
        "--tls-ca",
        "../testing-certs/rootCA.pem",
        "--format",
        "json",
    ];
    let result = server
        .run(vec!["create", "token", "--admin"], args)
        .unwrap();

    let value: Value =
        serde_json::from_str(&result).expect("token creation response should be in json format");
    let token = value
        .get("token")
        .expect("token to be present")
        .as_str()
        .expect("token to be a str");
    // check if the token generated works by using it to regenerate
    let result = server
        .run_with_confirmation(
            vec!["create", "token", "--admin"],
            &[
                "--regenerate",
                "--tls-ca",
                "../testing-certs/rootCA.pem",
                "--token",
                token,
            ],
        )
        .unwrap();
    assert_contains!(&result, "New token created successfully!");
}

#[test_log::test(tokio::test)]
async fn test_create_admin_token_allowed_once() {
    let server = TestServer::configure()
        .with_auth()
        .with_no_admin_token()
        .spawn()
        .await;
    let args = &["--tls-ca", "../testing-certs/rootCA.pem"];
    let result = server
        .run(vec!["create", "token", "--admin"], args)
        .unwrap();
    assert_contains!(&result, "New token created successfully!");

    let result = server
        .run(vec!["create", "token", "--admin"], args)
        .unwrap();
    assert_contains!(
        &result,
        "Failed to create token, error: ApiError { code: 409, message: \"token name already exists, _admin\" }"
    );
}

#[test_log::test(tokio::test)]
async fn test_regenerate_admin_token() {
    // when created with_auth, TestServer spins up server and generates admin token.
    let mut server = TestServer::configure().with_auth().spawn().await;
    let args = &["--tls-ca", "../testing-certs/rootCA.pem"];
    let result = server
        .run(vec!["create", "token", "--admin"], args)
        .unwrap();
    // already has admin token, so it cannot be created again
    assert_contains!(
        &result,
        "Failed to create token, error: ApiError { code: 409, message: \"token name already exists, _admin\" }"
    );

    // regenerating token is allowed
    let result = server
        .run_with_confirmation(
            vec!["create", "token", "--admin"],
            &["--regenerate", "--tls-ca", "../testing-certs/rootCA.pem"],
        )
        .unwrap();
    assert_contains!(&result, "New token created successfully!");
    let old_token = server.token().expect("admin token to be present");
    let new_token = parse_token(result);
    assert!(old_token != &new_token);

    // old token cannot access
    let res = server
        .create_database("sample_db")
        .run()
        .err()
        .unwrap()
        .to_string();
    assert_contains!(&res, "401 Unauthorized");

    // new token should allow
    server.set_token(Some(new_token));
    let res = server.create_database("sample_db").run().unwrap();
    assert_contains!(&res, "Database \"sample_db\" created successfully");
}

#[test_log::test(tokio::test)]
async fn test_delete_token() {
    let server = TestServer::configure()
        .with_auth()
        .with_no_admin_token()
        .spawn()
        .await;
    let args = &[];
    let result = server
        .run(
            vec![
                "create",
                "token",
                "--admin",
                "--tls-ca",
                "../testing-certs/rootCA.pem",
            ],
            args,
        )
        .unwrap();
    assert_contains!(&result, "New token created successfully!");
    let token = parse_token(result);

    let result = server
        .run(
            vec!["delete", "token"],
            &[
                "--token-name",
                "_admin",
                "--token",
                &token,
                "--tls-ca",
                "../testing-certs/rootCA.pem",
            ],
        )
        .unwrap();
    assert_contains!(
        result,
        "The operator token \"_admin\" is required and cannot be deleted. To regenerate an operator token, use: influxdb3 create token --admin --regenerate --token [TOKEN]"
    );

    // you should be able to create the token again
    let result = server
        .run(
            vec![
                "create",
                "token",
                "--admin",
                "--tls-ca",
                "../testing-certs/rootCA.pem",
            ],
            args,
        )
        .unwrap();
    assert_contains!(
        &result,
        "Failed to create token, error: ApiError { code: 409, message: \"token name already exists, _admin\" }"
    );
}

#[test_log::test(tokio::test)]
async fn test_create_admin_token_endpoint_disabled() {
    let server = TestServer::configure().spawn().await;
    let args = &["--tls-ca", "../testing-certs/rootCA.pem"];
    let expected = "code: 405, message: \"endpoint disabled, started without auth\"";

    let run_args = vec!["create", "token", "--admin"];
    let result = server.run(run_args, args).unwrap();
    assert_contains!(&result, expected);

    let regen_args = vec!["create", "token", "--admin", "--regenerate"];
    let result = server.run_with_confirmation(regen_args, args).unwrap();
    assert_contains!(&result, expected);
}
