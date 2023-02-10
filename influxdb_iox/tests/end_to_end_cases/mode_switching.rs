// This file can be deleted when everything has been switched over to the RPC write path.

use assert_cmd::Command;
use predicates::prelude::*;
use std::time::Duration;

#[test]
fn router2_errors_without_mode_env_var() {
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .env_clear()
        .arg("run")
        .arg("router2")
        .arg("--ingester-addresses")
        .arg("http://required:8082")
        .timeout(Duration::from_secs(2))
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "`INFLUXDB_IOX_RPC_MODE` was not specified but `router2` was the command run",
        ));
}

#[test]
fn ingester2_errors_without_mode_env_var() {
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .env_clear()
        .arg("run")
        .arg("ingester2")
        .arg("--wal-directory")
        .arg("required")
        .arg("--catalog")
        .arg("memory")
        .timeout(Duration::from_secs(2))
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "`INFLUXDB_IOX_RPC_MODE` was not specified but `ingester2` was the command run",
        ));
}

#[test]
fn querier_without_ingesters_without_mode_env_var_uses_write_buffer() {
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .env_clear()
        .arg("run")
        .arg("querier")
        .arg("-v")
        .arg("--catalog")
        .arg("memory")
        .timeout(Duration::from_secs(2))
        .assert()
        .failure()
        .stdout(predicate::str::contains("using the write buffer path"));
}

#[test]
fn querier_without_ingesters_with_mode_env_var_uses_rpc_write() {
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .env_clear()
        .env("INFLUXDB_IOX_RPC_MODE", "2")
        .arg("run")
        .arg("querier")
        .arg("-v")
        .arg("--catalog")
        .arg("memory")
        .timeout(Duration::from_secs(2))
        .assert()
        .failure()
        .stdout(predicate::str::contains("using the RPC write path"));
}

#[test]
fn compactor2_errors_without_mode_env_var() {
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .env_clear()
        .arg("run")
        .arg("compactor2")
        .timeout(Duration::from_secs(2))
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "`INFLUXDB_IOX_RPC_MODE` was not specified but `compactor2` was the command run",
        ));
}
