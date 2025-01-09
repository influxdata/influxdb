use std::{
    io::Write,
    process::{Command, Stdio},
    thread,
};

use crate::TestServer;
use assert_cmd::cargo::CommandCargoExt;
use observability_deps::tracing::debug;
use pretty_assertions::assert_eq;
use serde_json::{json, Value};
use test_helpers::assert_contains;
use test_helpers::tempfile::NamedTempFile;

pub fn run(args: &[&str]) -> String {
    let process = Command::cargo_bin("influxdb3")
        .unwrap()
        .args(args)
        .stdout(Stdio::piped())
        .output()
        .unwrap();

    String::from_utf8_lossy(&process.stdout).trim().into()
}

pub fn run_and_err(args: &[&str]) -> String {
    let process = Command::cargo_bin("influxdb3")
        .unwrap()
        .args(args)
        .stderr(Stdio::piped())
        .output()
        .unwrap();

    String::from_utf8_lossy(&process.stderr).trim().into()
}

pub fn run_with_confirmation(args: &[&str]) -> String {
    let mut child_process = Command::cargo_bin("influxdb3")
        .unwrap()
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();

    let mut stdin = child_process.stdin.take().expect("failed to open stdin");
    thread::spawn(move || {
        stdin
            .write_all(b"yes\n")
            .expect("cannot write confirmation msg to stdin");
    });

    String::from_utf8(child_process.wait_with_output().unwrap().stdout)
        .unwrap()
        .trim()
        .into()
}

pub fn run_with_confirmation_and_err(args: &[&str]) -> String {
    let mut child_process = Command::cargo_bin("influxdb3")
        .unwrap()
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    let mut stdin = child_process.stdin.take().expect("failed to open stdin");
    thread::spawn(move || {
        stdin
            .write_all(b"yes\n")
            .expect("cannot write confirmation msg to stdin");
    });

    String::from_utf8(child_process.wait_with_output().unwrap().stderr)
        .unwrap()
        .trim()
        .into()
}

// Helper function to create a temporary Python plugin file
fn create_plugin_file(code: &str) -> NamedTempFile {
    let mut file = NamedTempFile::new().unwrap();
    writeln!(file, "{}", code).unwrap();
    file
}

#[test_log::test(tokio::test)]
async fn test_show_databases() {
    let server = TestServer::spawn().await;
    let server_addr = server.client_addr();
    let output = run_with_confirmation(&["create", "database", "foo", "--host", &server_addr]);
    debug!(output, "create database");
    let output = run_with_confirmation(&["create", "database", "bar", "--host", &server_addr]);
    debug!(output, "create database");
    let output = run(&["show", "databases", "--host", &server_addr]);
    assert_eq!(
        "\
        +---------------+\n\
        | iox::database |\n\
        +---------------+\n\
        | bar           |\n\
        | foo           |\n\
        +---------------+\
        ",
        output
    );
    let output = run(&[
        "show",
        "databases",
        "--host",
        &server_addr,
        "--format",
        "json",
    ]);
    assert_eq!(
        r#"[{"iox::database":"bar"},{"iox::database":"foo"}]"#,
        output
    );
    let output = run(&[
        "show",
        "databases",
        "--host",
        &server_addr,
        "--format",
        "csv",
    ]);
    assert_eq!(
        "\
        iox::database\n\
        bar\n\
        foo\
        ",
        output
    );
    let output = run(&[
        "show",
        "databases",
        "--host",
        &server_addr,
        "--format",
        "json_lines",
    ]);
    assert_eq!(
        "\
        {\"iox::database\":\"bar\"}\n\
        {\"iox::database\":\"foo\"}\
        ",
        output
    );
    run_with_confirmation(&["delete", "database", "foo", "--host", &server_addr]);
    let output = run(&["show", "databases", "--host", &server_addr]);
    assert_eq!(
        "\
        +---------------+\n\
        | iox::database |\n\
        +---------------+\n\
        | bar           |\n\
        +---------------+",
        output
    );
    let output = run(&[
        "show",
        "databases",
        "--host",
        &server_addr,
        "--show-deleted",
    ]);
    // don't assert on actual output since it contains a time stamp which would be flaky
    assert_contains!(output, "foo-");
}

#[test_log::test(tokio::test)]
async fn test_create_database() {
    let server = TestServer::spawn().await;
    let server_addr = server.client_addr();
    let db_name = "foo";
    let result = run_with_confirmation(&["create", "database", db_name, "--host", &server_addr]);
    debug!(result = ?result, "create database");
    assert_contains!(&result, "Database \"foo\" created successfully");
}

#[test_log::test(tokio::test)]
async fn test_create_database_limit() {
    let server = TestServer::spawn().await;
    let server_addr = server.client_addr();
    let db_name = "foo";
    for i in 0..5 {
        let name = format!("{db_name}{i}");
        let result = run_with_confirmation(&["create", "database", &name, "--host", &server_addr]);
        debug!(result = ?result, "create database");
        assert_contains!(&result, format!("Database \"{name}\" created successfully"));
    }

    let result =
        run_with_confirmation_and_err(&["create", "database", "foo5", "--host", &server_addr]);
    debug!(result = ?result, "create database");
    assert_contains!(
        &result,
        "Adding a new database would exceed limit of 5 databases"
    );
}

#[test_log::test(tokio::test)]
async fn test_delete_database() {
    let server = TestServer::spawn().await;
    let server_addr = server.client_addr();
    let db_name = "foo";
    server
        .write_lp_to_db(
            db_name,
            "cpu,t1=a,t2=b,t3=c f1=true,f2=\"hello\",f3=4i,f4=4u,f5=5 1000",
            influxdb3_client::Precision::Second,
        )
        .await
        .expect("write to db");
    let result = run_with_confirmation(&["delete", "database", db_name, "--host", &server_addr]);
    debug!(result = ?result, "delete database");
    assert_contains!(&result, "Database \"foo\" deleted successfully");
}

#[test_log::test(tokio::test)]
async fn test_delete_missing_database() {
    let server = TestServer::spawn().await;
    let server_addr = server.client_addr();
    let db_name = "foo";
    let result =
        run_with_confirmation_and_err(&["delete", "database", db_name, "--host", &server_addr]);
    debug!(result = ?result, "delete missing database");
    assert_contains!(&result, "404");
}

#[test_log::test(tokio::test)]
async fn test_create_table() {
    let server = TestServer::spawn().await;
    let server_addr = server.client_addr();
    let db_name = "foo";
    let table_name = "bar";
    let result = run_with_confirmation(&["create", "database", db_name, "--host", &server_addr]);
    debug!(result = ?result, "create database");
    assert_contains!(&result, "Database \"foo\" created successfully");
    let result = run_with_confirmation(&[
        "create",
        "table",
        table_name,
        "--database",
        db_name,
        "--host",
        &server_addr,
        "--tags",
        "one",
        "two",
        "three",
        "--fields",
        "four:utf8",
        "five:uint64",
        "six:float64",
        "seven:int64",
        "eight:bool",
    ]);
    debug!(result = ?result, "create table");
    assert_contains!(&result, "Table \"foo\".\"bar\" created successfully");
    // Check that we can query the table and that it has no values
    let result = server
        .api_v3_query_sql(&[
            ("db", "foo"),
            ("q", "SELECT * FROM bar"),
            ("format", "json"),
        ])
        .await
        .json::<Value>()
        .await
        .unwrap();
    assert_eq!(result, json!([]));
    server
        .write_lp_to_db(
            db_name,
            format!("{table_name},one=1,two=2,three=3 four=\"4\",five=5u,six=6,seven=7i,eight=true 1000"),
            influxdb3_client::Precision::Second,
        )
        .await
        .expect("write to db");
    // Check that we can get data from the table
    let result = server
        .api_v3_query_sql(&[
            ("db", "foo"),
            ("q", "SELECT * FROM bar"),
            ("format", "json"),
        ])
        .await
        .json::<Value>()
        .await
        .unwrap();
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
            "time": "1970-01-01T00:16:40"
        }])
    );
}

#[test_log::test(tokio::test)]
async fn test_delete_table() {
    let server = TestServer::spawn().await;
    let server_addr = server.client_addr();
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
    let result = run_with_confirmation(&[
        "delete",
        "table",
        table_name,
        "--database",
        db_name,
        "--host",
        &server_addr,
    ]);
    debug!(result = ?result, "delete table");
    assert_contains!(&result, "Table \"foo\".\"cpu\" deleted successfully");
}

#[test_log::test(tokio::test)]
async fn test_delete_missing_table() {
    let server = TestServer::spawn().await;
    let server_addr = server.client_addr();
    let db_name = "foo";
    let table_name = "mem";
    server
        .write_lp_to_db(
            db_name,
            format!("{table_name},t1=a,t2=b,t3=c f1=true,f2=\"hello\",f3=4i,f4=4u,f5=5 1000"),
            influxdb3_client::Precision::Second,
        )
        .await
        .expect("write to db");

    let result = run_with_confirmation_and_err(&[
        "delete",
        "table",
        "cpu",
        "--database",
        db_name,
        "--host",
        &server_addr,
    ]);
    debug!(result = ?result, "delete missing table");
    assert_contains!(&result, "404");
}

#[tokio::test]
async fn test_create_delete_distinct_cache() {
    let server = TestServer::spawn().await;
    let server_addr = server.client_addr();
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
    // first create the cache:
    let result = run(&[
        "create",
        "distinct_cache",
        "--host",
        &server_addr,
        "--database",
        db_name,
        "--table",
        table_name,
        "--columns",
        "t1,t2",
        cache_name,
    ]);
    assert_contains!(&result, "new cache created");
    // doing the same thing over again will be a no-op
    let result = run(&[
        "create",
        "distinct_cache",
        "--host",
        &server_addr,
        "--database",
        db_name,
        "--table",
        table_name,
        "--columns",
        "t1,t2",
        cache_name,
    ]);
    assert_contains!(
        &result,
        "a cache already exists for the provided parameters"
    );
    // now delete it:
    let result = run(&[
        "delete",
        "distinct_cache",
        "--host",
        &server_addr,
        "--database",
        db_name,
        "--table",
        table_name,
        cache_name,
    ]);
    assert_contains!(&result, "distinct cache deleted successfully");
    // trying to delete again should result in an error as the cache no longer exists:
    let result = run_and_err(&[
        "delete",
        "distinct_cache",
        "--host",
        &server_addr,
        "--database",
        db_name,
        "--table",
        table_name,
        cache_name,
    ]);
    assert_contains!(&result, "[404 Not Found]: cache not found");
}

#[test_log::test(tokio::test)]
async fn test_create_plugin() {
    let server = TestServer::spawn().await;
    let server_addr = server.client_addr();
    let db_name = "foo";
    let plugin_name = "test_plugin";

    // Create database first
    let result = run_with_confirmation(&["create", "database", "--host", &server_addr, db_name]);
    assert_contains!(&result, "Database \"foo\" created successfully");

    // Create plugin file
    let plugin_file = create_plugin_file(
        r#"
def process_rows(iterator, output):
    pass
"#,
    );

    // Create plugin
    let result = run_with_confirmation(&[
        "create",
        "plugin",
        "--database",
        db_name,
        "--host",
        &server_addr,
        "--code-filename",
        plugin_file.path().to_str().unwrap(),
        "--entry-point",
        "process_rows",
        plugin_name,
    ]);
    debug!(result = ?result, "create plugin");
    assert_contains!(&result, "Plugin test_plugin created successfully");
}

#[test_log::test(tokio::test)]
async fn test_delete_plugin() {
    let server = TestServer::spawn().await;
    let server_addr = server.client_addr();
    let db_name = "foo";
    let plugin_name = "test_plugin";

    // Setup: create database and plugin
    run_with_confirmation(&["create", "database", "--host", &server_addr, db_name]);

    let plugin_file = create_plugin_file(
        r#"
def process_rows(iterator, output):
    pass
"#,
    );

    run_with_confirmation(&[
        "create",
        "plugin",
        "--database",
        db_name,
        "--host",
        &server_addr,
        "--code-filename",
        plugin_file.path().to_str().unwrap(),
        "--entry-point",
        "process_rows",
        plugin_name,
    ]);

    // Delete plugin
    let result = run_with_confirmation(&[
        "delete",
        "plugin",
        "--database",
        db_name,
        "--host",
        &server_addr,
        plugin_name,
    ]);
    debug!(result = ?result, "delete plugin");
    assert_contains!(&result, "Plugin test_plugin deleted successfully");
}

#[test_log::test(tokio::test)]
async fn test_create_trigger() {
    let server = TestServer::spawn().await;
    let server_addr = server.client_addr();
    let db_name = "foo";
    let plugin_name = "test_plugin";
    let trigger_name = "test_trigger";

    // Setup: create database and plugin
    run_with_confirmation(&["create", "database", "--host", &server_addr, db_name]);

    let plugin_file = create_plugin_file(
        r#"
def process_rows(iterator, output):
    pass
"#,
    );

    run_with_confirmation(&[
        "create",
        "plugin",
        "--database",
        db_name,
        "--host",
        &server_addr,
        "--code-filename",
        plugin_file.path().to_str().unwrap(),
        "--entry-point",
        "process_rows",
        plugin_name,
    ]);

    // Create trigger
    let result = run_with_confirmation(&[
        "create",
        "trigger",
        "--database",
        db_name,
        "--host",
        &server_addr,
        "--plugin",
        plugin_name,
        "--trigger-spec",
        "all_tables",
        trigger_name,
    ]);
    debug!(result = ?result, "create trigger");
    assert_contains!(&result, "Trigger test_trigger created successfully");
}

#[test_log::test(tokio::test)]
async fn test_trigger_activation() {
    let server = TestServer::spawn().await;
    let server_addr = server.client_addr();
    let db_name = "foo";
    let plugin_name = "test_plugin";
    let trigger_name = "test_trigger";

    // Setup: create database, plugin, and trigger
    run_with_confirmation(&["create", "database", "--host", &server_addr, db_name]);

    let plugin_file = create_plugin_file(
        r#"
def process_rows(iterator, output):
    pass
"#,
    );

    run_with_confirmation(&[
        "create",
        "plugin",
        "--database",
        db_name,
        "--host",
        &server_addr,
        "--code-filename",
        plugin_file.path().to_str().unwrap(),
        "--entry-point",
        "process_rows",
        plugin_name,
    ]);

    run_with_confirmation(&[
        "create",
        "trigger",
        "--database",
        db_name,
        "--host",
        &server_addr,
        "--plugin",
        plugin_name,
        "--trigger-spec",
        "all_tables",
        trigger_name,
    ]);

    // Test activation
    let result = run_with_confirmation(&[
        "activate",
        "trigger",
        "--database",
        db_name,
        "--host",
        &server_addr,
        trigger_name,
    ]);
    debug!(result = ?result, "activate trigger");
    assert_contains!(&result, "Trigger test_trigger activated successfully");

    // Test deactivation
    let result = run_with_confirmation(&[
        "deactivate",
        "trigger",
        "--database",
        db_name,
        "--host",
        &server_addr,
        trigger_name,
    ]);
    debug!(result = ?result, "deactivate trigger");
    assert_contains!(&result, "Trigger test_trigger deactivated successfully");
}

#[test_log::test(tokio::test)]
async fn test_delete_active_trigger() {
    let server = TestServer::spawn().await;
    let server_addr = server.client_addr();
    let db_name = "foo";
    let plugin_name = "test_plugin";
    let trigger_name = "test_trigger";

    // Setup: create database, plugin, and active trigger
    run_with_confirmation(&["create", "database", "--host", &server_addr, db_name]);

    let plugin_file = create_plugin_file(
        r#"
def process_rows(iterator, output):
    pass
"#,
    );

    run_with_confirmation(&[
        "create",
        "plugin",
        "--database",
        db_name,
        "--host",
        &server_addr,
        "--code-filename",
        plugin_file.path().to_str().unwrap(),
        "--entry-point",
        "process_rows",
        plugin_name,
    ]);

    run_with_confirmation(&[
        "create",
        "trigger",
        "--database",
        db_name,
        "--host",
        &server_addr,
        "--plugin",
        plugin_name,
        "--trigger-spec",
        "all_tables",
        trigger_name,
    ]);

    run_with_confirmation(&[
        "activate",
        "trigger",
        "--database",
        db_name,
        "--host",
        &server_addr,
        trigger_name,
    ]);

    // Try to delete active trigger without force flag
    let result = run_with_confirmation_and_err(&[
        "delete",
        "trigger",
        "--database",
        db_name,
        "--host",
        &server_addr,
        trigger_name,
    ]);
    debug!(result = ?result, "delete active trigger without force");
    assert_contains!(&result, "command failed");

    // Delete active trigger with force flag
    let result = run_with_confirmation(&[
        "delete",
        "trigger",
        "--database",
        db_name,
        "--host",
        &server_addr,
        trigger_name,
        "--force",
    ]);
    debug!(result = ?result, "delete active trigger with force");
    assert_contains!(&result, "Trigger test_trigger deleted successfully");
}

#[test_log::test(tokio::test)]
async fn test_table_specific_trigger() {
    let server = TestServer::spawn().await;
    let server_addr = server.client_addr();
    let db_name = "foo";
    let table_name = "bar";
    let plugin_name = "test_plugin";
    let trigger_name = "test_trigger";

    // Setup: create database, table, and plugin
    run_with_confirmation(&["create", "database", "--host", &server_addr, db_name]);

    run_with_confirmation(&[
        "create",
        "table",
        "--database",
        db_name,
        "--host",
        &server_addr,
        "--tags",
        "tag1",
        "--fields",
        "field1:float64",
        table_name,
    ]);

    let plugin_file = create_plugin_file(
        r#"
def process_rows(iterator, output):
    pass
"#,
    );

    run_with_confirmation(&[
        "create",
        "plugin",
        "--database",
        db_name,
        "--host",
        &server_addr,
        "--code-filename",
        plugin_file.path().to_str().unwrap(),
        "--entry-point",
        "process_rows",
        plugin_name,
    ]);

    // Create table-specific trigger
    let result = run_with_confirmation(&[
        "create",
        "trigger",
        "--database",
        db_name,
        "--host",
        &server_addr,
        "--plugin",
        plugin_name,
        "--trigger-spec",
        &format!("table:{}", table_name),
        trigger_name,
    ]);
    debug!(result = ?result, "create table-specific trigger");
    assert_contains!(&result, "Trigger test_trigger created successfully");
}

#[test]
fn test_create_token() {
    let result = run_with_confirmation(&["create", "token"]);
    assert_contains!(
        &result,
        "This will grant you access to every HTTP endpoint or deny it otherwise"
    );
}

#[tokio::test]
async fn distinct_cache_create_and_delete() {
    let server = TestServer::spawn().await;
    let db_name = "foo";
    let server_addr = server.client_addr();
    server
        .write_lp_to_db(
            db_name,
            "cpu,t1=a,t2=b,t3=c f1=true,f2=\"hello\",f3=4i,f4=4u,f5=5 1000",
            influxdb3_client::Precision::Second,
        )
        .await
        .expect("write to db");

    let result = run_with_confirmation(&[
        "create",
        "distinct_cache",
        "-H",
        &server_addr,
        "-d",
        db_name,
        "-t",
        "cpu",
        "--columns",
        "t1,t2",
        "--max-cardinality",
        "20000",
        "--max-age",
        "200s",
        "cache_money",
    ]);

    insta::assert_yaml_snapshot!(result);

    let result = run_with_confirmation(&[
        "delete",
        "distinct_cache",
        "-H",
        &server_addr,
        "-d",
        db_name,
        "-t",
        "cpu",
        "cache_money",
    ]);

    insta::assert_yaml_snapshot!(result);
}
