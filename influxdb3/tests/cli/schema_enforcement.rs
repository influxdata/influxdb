//! CLI coverage for the schema enforcement surface in the oss workspace:
//! `create database --schema-mode`, which a Core server refuses for
//! `explicit`, and `update table --tags/--fields`, which drives
//! `PATCH /api/v3/configure/table`.

use crate::server::{ConfigProvider, TestServer};
use test_helpers::assert_contains;

const CA: [&str; 2] = ["--tls-ca", "../testing-certs/rootCA.pem"];

/// The flag is accepted by the CLI, so the same binary can drive an
/// Enterprise server, but a Core server refuses `explicit` and the error
/// names Enterprise. Nothing is created.
#[test_log::test(tokio::test)]
async fn create_database_with_explicit_schema_mode_is_enterprise_only() {
    let server = TestServer::configure().with_no_admin_token().spawn().await;

    let err = server
        .run(
            vec!["create", "database", "sensors", "--schema-mode", "explicit"],
            &CA,
        )
        .expect_err("explicit schema mode should be refused");
    assert_contains!(
        &err.to_string(),
        "explicit schema mode is only available in InfluxDB 3 Enterprise"
    );

    let out = server
        .run(vec!["show", "databases"], &CA)
        .expect("show databases");
    assert!(!out.contains("sensors"), "got {out}");
}

/// `--schema-mode implicit` is accepted and means what the default means.
#[test_log::test(tokio::test)]
async fn create_database_with_implicit_schema_mode() {
    let server = TestServer::configure().with_no_admin_token().spawn().await;

    server
        .run(
            vec!["create", "database", "sensors", "--schema-mode", "implicit"],
            &CA,
        )
        .expect("create database");
    server
        .run(
            vec!["write", "-d", "sensors"],
            &[CA[0], CA[1], "cpu,host=a usage=1.0"],
        )
        .expect("first write creates the table");
    server
        .run(
            vec!["write", "-d", "sensors"],
            &[CA[0], CA[1], "cpu,host=a,region=west usage=1.0"],
        )
        .expect("later write widens the schema");
}

/// The flag is a `clap::ValueEnum`, so an unknown value is refused before any
/// request is sent, and the error lists what is accepted.
#[test_log::test(tokio::test)]
async fn create_database_rejects_an_unknown_schema_mode() {
    let server = TestServer::configure().with_no_admin_token().spawn().await;

    let err = server
        .run(
            vec!["create", "database", "nope", "--schema-mode", "strict"],
            &CA,
        )
        .expect_err("unknown schema mode should be refused");
    let msg = err.to_string();
    assert_contains!(&msg, "invalid value 'strict'");
    assert_contains!(&msg, "[possible values: implicit, explicit]");
}

/// `update table` declares columns ahead of any write, and the declaration
/// binds the column's type: a later write at another type is rejected.
#[test_log::test(tokio::test)]
async fn update_table_adds_columns() {
    let server = TestServer::configure().with_no_admin_token().spawn().await;

    server
        .run(vec!["create", "database", "sensors"], &CA)
        .expect("create database");
    server
        .run(
            vec![
                "create",
                "table",
                "-d",
                "sensors",
                "cpu",
                "--tags",
                "host",
                "-f",
                "usage:float64",
            ],
            &CA,
        )
        .expect("create table");

    let out = server
        .run(
            vec![
                "update",
                "table",
                "-d",
                "sensors",
                "cpu",
                "--tags",
                "region",
                "-f",
                "free:int64",
            ],
            &CA,
        )
        .expect("add columns");
    assert_contains!(
        &out,
        "Table \"sensors\".\"cpu\" updated with 1 tag(s) and 1 field(s)"
    );

    // A write matching the declared types is accepted.
    server
        .run(
            vec!["write", "-d", "sensors"],
            &[CA[0], CA[1], "cpu,host=a,region=west usage=1.0,free=2i"],
        )
        .expect("write at the declared types should be accepted");

    // The declaration fixed `free` as int64, so a float is rejected.
    let err = server
        .run(
            vec!["write", "-d", "sensors"],
            &[CA[0], CA[1], "cpu,host=a,region=west usage=1.0,free=2.0"],
        )
        .expect_err("write at another type should be rejected");
    assert_contains!(&err.to_string(), "free");
}

/// `update table` with neither `--tags` nor `--fields` sends nothing.
#[test_log::test(tokio::test)]
async fn update_table_requires_columns() {
    let server = TestServer::configure().with_no_admin_token().spawn().await;

    server
        .run(vec!["create", "database", "sensors"], &CA)
        .expect("create database");

    let err = server
        .run(vec!["update", "table", "-d", "sensors", "cpu"], &CA)
        .expect_err("empty update should be refused");
    assert_contains!(
        &err.to_string(),
        "one of --tags or --fields is required for update table"
    );
}
