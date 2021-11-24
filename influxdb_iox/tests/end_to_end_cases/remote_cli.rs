use crate::common::server_fixture::{ServerFixture, ServerType};

use assert_cmd::Command;
use predicates::prelude::*;

#[tokio::test]
async fn test_remotes_router() {
    assert_remotes(ServerFixture::create_single_use(ServerType::Router).await).await;
}

async fn assert_remotes(server_fixture: ServerFixture) {
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
        .arg("remote")
        .arg("list")
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains("no remotes configured"));

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("server")
        .arg("remote")
        .arg("set")
        .arg("1")
        .arg("http://1.2.3.4:1234")
        .arg("--host")
        .arg(addr)
        .assert()
        .success();

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("server")
        .arg("remote")
        .arg("list")
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains("http://1.2.3.4:1234"));

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("server")
        .arg("remote")
        .arg("remove")
        .arg("1")
        .arg("--host")
        .arg(addr)
        .assert()
        .success();

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("server")
        .arg("remote")
        .arg("list")
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains("no remotes configured"));
}
