use assert_cmd::Command;
use predicates::prelude::*;

use crate::common::server_fixture::DEFAULT_SERVER_ID;

use super::scenario::{fixture_broken_catalog, rand_name};

#[tokio::test]
async fn test_dump_catalog() {
    let db_name = rand_name();
    let fixture = fixture_broken_catalog(&db_name).await;

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("debug")
        .arg("dump-catalog")
        .arg("--object-store")
        .arg("file")
        .arg("--data-dir")
        .arg(fixture.dir())
        .arg("--server-id")
        .arg(DEFAULT_SERVER_ID.to_string())
        .arg(&db_name)
        .assert()
        .success()
        .stdout(
            predicate::str::contains("Transaction").and(predicate::str::contains("DecodeError")),
        );
}
