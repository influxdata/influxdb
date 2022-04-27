use assert_cmd::Command;
use predicates::prelude::*;

#[tokio::test]
async fn test_git_version() {
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("--version")
        .assert()
        .success()
        .stdout(
            predicate::str::contains("UNKNOWN")
                .not()
                .and(predicate::str::is_match("revision [0-9a-f]{40}").unwrap()),
        );
}

#[tokio::test]
async fn test_print_cpu() {
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("debug")
        .arg("print-cpu")
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "rustc is using the following target options",
        ));
}
