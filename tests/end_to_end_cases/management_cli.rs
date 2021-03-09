use assert_cmd::Command;
use predicates::prelude::*;

use crate::common::server_fixture::ServerFixture;

pub async fn test(server_fixture: &ServerFixture) {
    let addr = server_fixture.grpc_url_base();

    test_writer_id(addr).await;
    test_create_database(addr).await;
}

async fn test_writer_id(addr: &str) {
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("writer")
        .arg("set")
        .arg("32")
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains("Ok"));

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("writer")
        .arg("get")
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains("32"));
}

async fn test_create_database(addr: &str) {
    let db = "management-cli-test";

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("get")
        .arg(db)
        .arg("--host")
        .arg(addr)
        .assert()
        .failure()
        .stderr(predicate::str::contains("Database not found"));

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("create")
        .arg(db)
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains("Ok"));

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("get")
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains(db));

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("get")
        .arg(db)
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains(format!("name: \"{}\"", db)));
}
