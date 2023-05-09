//! Tests that we still support running using deprecated names so that deployments continue to work
//! while transitioning.

use assert_cmd::Command;
use predicates::prelude::*;
use std::time::Duration;
use tempfile::tempdir;
use test_helpers_end_to_end::{AddAddrEnv, BindAddresses, ServerType};

#[tokio::test]
async fn ingester2_runs_ingester() {
    let tmpdir = tempdir().unwrap();
    let addrs = BindAddresses::default();

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .args(["run", "ingester2", "-v"])
        .env_clear()
        .env("HOME", tmpdir.path())
        .env("INFLUXDB_IOX_WAL_DIRECTORY", tmpdir.path())
        .env("INFLUXDB_IOX_CATALOG_TYPE", "memory")
        .add_addr_env(ServerType::Ingester, &addrs)
        .timeout(Duration::from_secs(5))
        .assert()
        .failure()
        .stderr(predicate::str::contains("error: unrecognized subcommand 'ingester2'").not())
        .stdout(predicate::str::contains(
            "InfluxDB IOx Ingester server ready",
        ));
}

#[tokio::test]
async fn compactor2_runs_compactor() {
    let tmpdir = tempdir().unwrap();
    let addrs = BindAddresses::default();

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .args(["run", "compactor2", "-v"])
        .env_clear()
        .env("HOME", tmpdir.path())
        .env("INFLUXDB_IOX_WAL_DIRECTORY", tmpdir.path())
        .env("INFLUXDB_IOX_CATALOG_TYPE", "memory")
        .add_addr_env(ServerType::Compactor, &addrs)
        .timeout(Duration::from_secs(5))
        .assert()
        .failure()
        .stderr(predicate::str::contains("error: unrecognized subcommand 'compactor2'").not())
        .stdout(predicate::str::contains(
            "InfluxDB IOx Compactor server ready",
        ));
}
