use assert_cmd::Command;
use predicates::prelude::*;

pub async fn test(addr: impl AsRef<str>) {
    test_writer_id(addr.as_ref()).await;
    test_create_database(addr.as_ref()).await;
}

async fn test_writer_id(addr: &str) {
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("writer")
        .arg("set")
        .arg("32")
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains("Ok"));

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("writer")
        .arg("get")
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains("32"));
}

async fn test_create_database(addr: &str) {
    let db = "management-cli-test";

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("get")
        .arg(db)
        .arg("--host")
        .arg(addr)
        .assert()
        .failure()
        .stderr(predicate::str::contains("Database not found"));

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("create")
        .arg(db)
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains("Ok"));

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("get")
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains(db));

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("get")
        .arg(db)
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains(format!("name: \"{}\"", db)));
}
