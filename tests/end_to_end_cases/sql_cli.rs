use assert_cmd::Command;
use predicates::prelude::*;

use crate::common::server_fixture::ServerFixture;

use super::scenario::{create_readable_database, create_two_partition_database, rand_name};

#[tokio::test]
async fn test_error_connecting() {
    let addr = "http://hope_this_addr_does_not_exist";
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("sql")
        .arg("--host")
        .arg(addr)
        .write_stdin("exit")
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "Error connecting to http://hope_this_addr_does_not_exist",
        ));
}

#[tokio::test]
async fn test_basic() {
    let fixture = ServerFixture::create_shared().await;
    let addr = fixture.grpc_base();
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("sql")
        .arg("--host")
        .arg(addr)
        .write_stdin("exit")
        .assert()
        .success()
        .stdout(
            predicate::str::contains("Ready for commands")
                .and(predicate::str::contains("Connected to IOx Server")),
        );
}

#[tokio::test]
async fn test_help() {
    let fixture = ServerFixture::create_shared().await;
    let addr = fixture.grpc_base();
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("sql")
        .arg("--host")
        .arg(addr)
        .write_stdin("help;")
        .assert()
        .success()
        .stdout(
            predicate::str::contains("# Basic IOx SQL Primer").and(predicate::str::contains(
                "SHOW DATABASES: List databases available on the server",
            )),
        );
}

#[tokio::test]
async fn test_exit() {
    let fixture = ServerFixture::create_shared().await;
    let addr = fixture.grpc_base();
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("sql")
        .arg("--host")
        .arg(addr)
        // help should not be run as it is after the exit command
        .write_stdin("exit;\nhelp;")
        .assert()
        .success()
        .stdout(predicate::str::contains("# Basic IOx SQL Primer").not());
}

#[tokio::test]
async fn test_sql_show_databases() {
    let fixture = ServerFixture::create_shared().await;
    let addr = fixture.grpc_base();

    let db_name1 = rand_name();
    create_readable_database(&db_name1, fixture.grpc_channel()).await;

    let db_name2 = rand_name();
    create_readable_database(&db_name2, fixture.grpc_channel()).await;

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("sql")
        .arg("--host")
        .arg(addr)
        .write_stdin("show databases;")
        .assert()
        .success()
        .stdout(
            predicate::str::contains("| db_name")
                .and(predicate::str::contains(&db_name1))
                .and(predicate::str::contains(&db_name2)),
        );
}

#[tokio::test]
async fn test_sql_use_database() {
    let fixture = ServerFixture::create_shared().await;
    let addr = fixture.grpc_base();

    let db_name = rand_name();
    create_two_partition_database(&db_name, fixture.grpc_channel()).await;

    let expected_output = r#"
+------+---------+----------+---------------------+-------+
| host | running | sleeping | time                | total |
+------+---------+----------+---------------------+-------+
| foo  | 4       | 514      | 2020-06-23 06:38:30 | 519   |
+------+---------+----------+---------------------+-------+
"#
    .trim();

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("sql")
        .arg("--host")
        .arg(addr)
        .write_stdin(format!("use {};\n\nselect * from cpu;", db_name))
        .assert()
        .success()
        .stdout(predicate::str::contains(expected_output));
}
