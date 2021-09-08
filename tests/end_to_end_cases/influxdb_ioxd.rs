use assert_cmd::Command;
use predicates::prelude::*;
use std::time::Duration;

#[tokio::test]
async fn test_logging() {
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("run")
        .arg("--log-filter")
        .arg("info")
        .timeout(Duration::from_secs(1))
        .assert()
        .failure()
        // Tokio-trace output
        .stdout(predicate::str::contains("InfluxDB IOx server starting"))
        // log crate output
        .stdout(predicate::str::contains("InfluxDB IOx server ready"));

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("run")
        .arg("--log-filter")
        .arg("error")
        .timeout(Duration::from_secs(1))
        .assert()
        .failure()
        // Tokio-trace output
        .stdout(predicate::str::contains("InfluxDB IOx server starting").not())
        // log crate output
        .stdout(predicate::str::contains("InfluxDB IOx server ready").not());
}
