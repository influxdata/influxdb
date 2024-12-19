use std::{
    io::Write,
    process::{Command, Stdio},
    thread,
};

use assert_cmd::cargo::CommandCargoExt;
use observability_deps::tracing::debug;
use pretty_assertions::assert_eq;
use serde_json::{json, Value};
use test_helpers::assert_contains;

use crate::TestServer;

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

#[test_log::test(tokio::test)]
async fn test_create_database() {
    let server = TestServer::spawn().await;
    let server_addr = server.client_addr();
    let db_name = "foo";
    let result = run_with_confirmation(&[
        "database",
        "create",
        "--dbname",
        db_name,
        "--host",
        &server_addr,
    ]);
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
        let result = run_with_confirmation(&[
            "database",
            "create",
            "--dbname",
            &name,
            "--host",
            &server_addr,
        ]);
        debug!(result = ?result, "create database");
        assert_contains!(&result, format!("Database \"{name}\" created successfully"));
    }

    let result = run_with_confirmation_and_err(&[
        "database",
        "create",
        "--dbname",
        "foo5",
        "--host",
        &server_addr,
    ]);
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
    let result = run_with_confirmation(&[
        "database",
        "delete",
        "--dbname",
        db_name,
        "--host",
        &server_addr,
    ]);
    debug!(result = ?result, "delete database");
    assert_contains!(&result, "Database \"foo\" deleted successfully");
}

#[test_log::test(tokio::test)]
async fn test_delete_missing_database() {
    let server = TestServer::spawn().await;
    let server_addr = server.client_addr();
    let db_name = "foo";
    let result = run_with_confirmation_and_err(&[
        "database",
        "delete",
        "--dbname",
        db_name,
        "--host",
        &server_addr,
    ]);
    debug!(result = ?result, "delete missing database");
    assert_contains!(&result, "404");
}

#[test_log::test(tokio::test)]
async fn test_create_table() {
    let server = TestServer::spawn().await;
    let server_addr = server.client_addr();
    let db_name = "foo";
    let table_name = "bar";
    let result = run_with_confirmation(&[
        "database",
        "create",
        "--dbname",
        db_name,
        "--host",
        &server_addr,
    ]);
    debug!(result = ?result, "create database");
    assert_contains!(&result, "Database \"foo\" created successfully");
    let result = run_with_confirmation(&[
        "table",
        "create",
        "--dbname",
        db_name,
        "--table",
        table_name,
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
        "table",
        "delete",
        "--dbname",
        db_name,
        "--table",
        table_name,
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
        "table",
        "delete",
        "--dbname",
        db_name,
        "--table",
        "cpu",
        "--host",
        &server_addr,
    ]);
    debug!(result = ?result, "delete missing table");
    assert_contains!(&result, "404");
}

#[tokio::test]
async fn test_create_delete_meta_cache() {
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
        "meta-cache",
        "create",
        "--host",
        &server_addr,
        "--dbname",
        db_name,
        "--table",
        table_name,
        "--cache-name",
        cache_name,
        "--columns",
        "t1,t2",
    ]);
    assert_contains!(&result, "new cache created");
    // doing the same thing over again will be a no-op
    let result = run(&[
        "meta-cache",
        "create",
        "--host",
        &server_addr,
        "--dbname",
        db_name,
        "--table",
        table_name,
        "--cache-name",
        cache_name,
        "--columns",
        "t1,t2",
    ]);
    assert_contains!(
        &result,
        "a cache already exists for the provided parameters"
    );
    // now delete it:
    let result = run(&[
        "meta-cache",
        "delete",
        "--host",
        &server_addr,
        "--dbname",
        db_name,
        "--table",
        table_name,
        "--cache-name",
        cache_name,
    ]);
    assert_contains!(&result, "meta cache deleted successfully");
    // trying to delete again should result in an error as the cache no longer exists:
    let result = run_and_err(&[
        "meta-cache",
        "delete",
        "--host",
        &server_addr,
        "--dbname",
        db_name,
        "--table",
        table_name,
        "--cache-name",
        cache_name,
    ]);
    assert_contains!(&result, "[404 Not Found]: cache not found");
}
