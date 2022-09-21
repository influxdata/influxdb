use assert_cmd::Command;
use predicates::prelude::*;
use std::{fs, io};
use test_helpers_end_to_end::maybe_skip_integration;

#[tokio::test]
async fn compactor_generate_has_defaults() {
    let database_url = maybe_skip_integration!();

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("compactor")
        .arg("generate")
        .arg("--catalog-dsn")
        .arg(database_url)
        .assert()
        .success();
}

#[tokio::test]
async fn compactor_generate_zeroes_are_invalid() {
    let database_url = maybe_skip_integration!();

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("compactor")
        .arg("generate")
        .arg("--catalog-dsn")
        .arg(database_url)
        .arg("--num-partitions")
        .arg("0")
        .arg("--num-files")
        .arg("0")
        .arg("--num-cols")
        .arg("0")
        .arg("--num-rows")
        .arg("0")
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "number would be zero for non-zero type",
        ));
}

#[tokio::test]
async fn compactor_generate_creates_files_and_catalog_entries() {
    let database_url = maybe_skip_integration!();
    let dir = tempfile::tempdir().expect("could not get temporary directory");

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("compactor")
        .arg("generate")
        .arg("--catalog-dsn")
        .arg(database_url)
        .arg("--object-store")
        .arg("file")
        .arg("--data-dir")
        .arg(dir.path().to_str().unwrap())
        .assert()
        .success();

    let files = fs::read_dir(dir)
        .unwrap()
        .collect::<Result<Vec<_>, io::Error>>()
        .unwrap();

    assert!(!files.is_empty());
}
