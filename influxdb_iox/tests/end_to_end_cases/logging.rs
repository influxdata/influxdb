use assert_cmd::Command;
use predicates::prelude::*;
use std::time::Duration;

#[tokio::test]
async fn test_logging() {
    // Testing with querier mode because it has the least amount of setup needed.
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .args(&[
            "run",
            "querier",
            "--log-filter",
            "info",
            "--catalog",
            "memory",
        ])
        .timeout(Duration::from_secs(2))
        .assert()
        .failure()
        // Tokio-trace output
        .stdout(predicate::str::contains("InfluxDB IOx server starting"))
        // log crate output
        .stdout(predicate::str::contains("Binding gRPC services"));

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .args(&[
            "run",
            "querier",
            "--log-filter",
            "error",
            "--catalog",
            "memory",
        ])
        .timeout(Duration::from_secs(2))
        .assert()
        .failure()
        // Tokio-trace output
        .stdout(predicate::str::contains("InfluxDB IOx server starting").not())
        // log crate output
        .stdout(predicate::str::contains("Binding gRPC services").not());
}
