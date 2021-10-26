use assert_cmd::Command;
use predicates::prelude::*;
use std::time::Duration;

#[ignore]
#[tokio::test]
async fn test_logging() {
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .args(&[
            "run",
            "--log-filter",
            "info",
            "--api-bind",
            "127.0.0.1:0",
            "--grpc-bind",
            "127.0.0.1:0",
        ])
        .timeout(Duration::from_secs(1))
        .assert()
        .failure()
        // Tokio-trace output
        .stdout(predicate::str::contains("InfluxDB IOx server starting"))
        // log crate output
        .stdout(predicate::str::contains("InfluxDB IOx server ready"));

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .args(&[
            "run",
            "--log-filter",
            "error",
            "--api-bind",
            "127.0.0.1:0",
            "--grpc-bind",
            "127.0.0.1:0",
        ])
        .timeout(Duration::from_secs(1))
        .assert()
        .failure()
        // Tokio-trace output
        .stdout(predicate::str::contains("InfluxDB IOx server starting").not())
        // log crate output
        .stdout(predicate::str::contains("InfluxDB IOx server ready").not());
}
