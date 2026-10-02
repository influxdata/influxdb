//! CLI coverage for explicit schema mode: `create database --schema-mode` and
//! `update table --tags/--fields`, which drives `PATCH
//! /api/v3/configure/table`.

use crate::server::{ConfigProvider, TestServer};
use test_helpers::assert_contains;

const CA: [&str; 2] = ["--tls-ca", "../testing-certs/rootCA.pem"];

/// `--schema-mode explicit` reaches the catalog: the database it creates takes
/// its table schemas from declarations, and rejects a write naming anything
/// undeclared.
#[test_log::test(tokio::test)]
async fn create_database_with_explicit_schema_mode() {
    let server = TestServer::configure().with_no_admin_token().spawn().await;

    server
        .run(
            vec!["create", "database", "sensors", "--schema-mode", "explicit"],
            &CA,
        )
        .expect("create explicit database");
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
        .expect("declare table");

    // A write matching the declaration is accepted.
    server
        .run(
            vec!["write", "-d", "sensors"],
            &[CA[0], CA[1], "cpu,host=a usage=1.0"],
        )
        .expect("declared write should be accepted");

    // An undeclared column is rejected, and the error names it.
    let err = server
        .run(
            vec!["write", "-d", "sensors"],
            &[CA[0], CA[1], "cpu,host=a,region=west usage=1.0"],
        )
        .expect_err("undeclared tag should be rejected");
    assert_contains!(&err.to_string(), "region");

    // As is an undeclared table.
    let err = server
        .run(
            vec!["write", "-d", "sensors"],
            &[CA[0], CA[1], "mem,host=a used=1.0"],
        )
        .expect_err("undeclared table should be rejected");
    assert_contains!(&err.to_string(), "mem");
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

#[test_log::test(tokio::test)]
async fn update_table_declares_columns_an_enforced_write_then_needs() {
    let server = TestServer::configure().with_no_admin_token().spawn().await;

    server
        .run(
            vec!["create", "database", "sensors", "--schema-mode", "explicit"],
            &CA,
        )
        .expect("create explicit database");
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
        .expect("declare table");

    // Undeclared columns are rejected, and the error names the column.
    let err = server
        .run(
            vec!["write", "-d", "sensors"],
            &[CA[0], CA[1], "cpu,host=a,region=west usage=1.0"],
        )
        .expect_err("undeclared tag should be rejected");
    assert_contains!(&err.to_string(), "region");

    // Adding them through the CLI makes the same write succeed.
    server
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
    server
        .run(
            vec!["write", "-d", "sensors"],
            &[CA[0], CA[1], "cpu,host=a,region=west usage=1.0,free=2i"],
        )
        .expect("write should be accepted once the columns are declared");
}

/// Without `--schema-mode`, a database keeps schema-on-write.
#[test_log::test(tokio::test)]
async fn create_database_defaults_to_implicit() {
    let server = TestServer::configure().with_no_admin_token().spawn().await;

    server
        .run(vec!["create", "database", "open"], &CA)
        .expect("create database");
    server
        .run(
            vec!["write", "-d", "open"],
            &[CA[0], CA[1], "cpu,host=a usage=1.0"],
        )
        .expect("first write creates the table");
    server
        .run(
            vec!["write", "-d", "open"],
            &[CA[0], CA[1], "cpu,host=a,region=west usage=1.0"],
        )
        .expect("later write widens the schema");
}
