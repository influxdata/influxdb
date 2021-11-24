use crate::common::server_fixture::{ServerFixture, ServerType};

use assert_cmd::Command;
use predicates::prelude::*;

#[tokio::test]
async fn test_server_id_database() {
    assert_server_id(ServerFixture::create_single_use(ServerType::Database).await).await;
}

#[tokio::test]
async fn test_server_id_router() {
    assert_server_id(ServerFixture::create_single_use(ServerType::Router).await).await;
}

async fn assert_server_id(server_fixture: ServerFixture) {
    let addr = server_fixture.grpc_base();
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("server")
        .arg("set")
        .arg("32")
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains("Ok"));

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("server")
        .arg("get")
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains("32"));

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("server")
        .arg("set")
        .arg("42")
        .arg("--host")
        .arg(addr)
        .assert()
        .failure()
        .stderr(predicate::str::contains("ID already set"));
}
