// This file can be deleted when everything has been switched over to the RPC write path.

use assert_cmd::Command;
use predicates::prelude::*;
use std::time::Duration;

#[test]
fn router_errors_with_mode_env_var() {
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .env_clear()
        .env("INFLUXDB_IOX_RPC_MODE", "2")
        .arg("run")
        .arg("router")
        .arg("--write-buffer")
        .arg("file")
        .arg("--write-buffer-addr")
        .arg("required")
        .timeout(Duration::from_secs(2))
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "`INFLUXDB_IOX_RPC_MODE` was specified but `router` was the command run",
        ));
}

#[test]
fn router2_errors_without_mode_env_var() {
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .env_clear()
        .arg("run")
        .arg("router2")
        .arg("--ingester-addresses")
        .arg("required")
        .timeout(Duration::from_secs(2))
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "`INFLUXDB_IOX_RPC_MODE` was not specified but `router2` was the command run",
        ));
}

#[test]
fn ingester_errors_with_mode_env_var() {
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .env_clear()
        .env("INFLUXDB_IOX_RPC_MODE", "2")
        .arg("run")
        .arg("ingester")
        .arg("--write-buffer")
        .arg("required")
        .arg("--write-buffer-addr")
        .arg("required")
        .arg("--shard-index-range-start")
        .arg("0")
        .arg("--shard-index-range-end")
        .arg("1")
        .arg("--pause-ingest-size-bytes")
        .arg("300")
        .arg("--persist-memory-threshold-bytes")
        .arg("500")
        .timeout(Duration::from_secs(2))
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "`INFLUXDB_IOX_RPC_MODE` was specified but `ingester` was the command run",
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
fn querier_errors_with_mode_env_var_and_shard_to_ingester_mapping() {
    let shard_to_ingesters_json = r#"{
          "ingesters": {
            "i1": {
              "addr": "arbitrary"
            }
          },
          "shards": {
            "0": {
              "ingester": "i1"
            }
        }
    }"#;

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .env_clear()
        .env("INFLUXDB_IOX_RPC_MODE", "2")
        .arg("run")
        .arg("querier")
        .arg("--shard-to-ingesters")
        .arg(shard_to_ingesters_json)
        .arg("--catalog")
        .arg("memory")
        .timeout(Duration::from_secs(2))
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "`INFLUXDB_IOX_RPC_MODE` is set but shard to ingester mappings were provided",
        ));
}

#[test]
fn querier_errors_without_mode_env_var_and_ingester_addresses() {
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .env_clear()
        .arg("run")
        .arg("querier")
        .arg("--ingester-addresses")
        .arg("arbitrary")
        .arg("--catalog")
        .arg("memory")
        .timeout(Duration::from_secs(2))
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "`INFLUXDB_IOX_RPC_MODE` is unset but ingester addresses were provided",
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
fn compactor_errors_with_mode_env_var() {
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .env_clear()
        .env("INFLUXDB_IOX_RPC_MODE", "2")
        .arg("run")
        .arg("compactor")
        .arg("--shard-index-range-start")
        .arg("0")
        .arg("--shard-index-range-end")
        .arg("1")
        .timeout(Duration::from_secs(2))
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "`INFLUXDB_IOX_RPC_MODE` was specified but `compactor` was the command run",
        ));
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
