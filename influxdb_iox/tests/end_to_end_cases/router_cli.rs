use crate::{
    common::server_fixture::{ServerFixture, ServerType},
    end_to_end_cases::scenario::rand_name,
};
use assert_cmd::Command;
use predicates::prelude::*;

#[tokio::test]
async fn test_router_crud() {
    let server_fixture = ServerFixture::create_shared(ServerType::Router).await;
    let addr = server_fixture.grpc_base();
    let router_name = rand_name();

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("router")
        .arg("get")
        .arg(&router_name)
        .arg("--host")
        .arg(addr)
        .assert()
        .failure()
        .stderr(predicate::str::contains(format!(
            "Resource router/{} not found",
            router_name,
        )));

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("router")
        .arg("create-or-update")
        .arg(&router_name)
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains(format!(
            "Created/Updated router {}",
            router_name
        )));

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("router")
        .arg("list")
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains(&router_name));

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("router")
        .arg("get")
        .arg(&router_name)
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(
            predicate::str::contains(&router_name).and(predicate::str::contains(format!(
                r#""name": "{}"#,
                &router_name
            ))), // validate the defaults have been set reasonably
        );

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("router")
        .arg("delete")
        .arg(&router_name)
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains(format!(
            "Deleted router {}",
            router_name
        )));

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("router")
        .arg("list")
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains(&router_name).not());
}
