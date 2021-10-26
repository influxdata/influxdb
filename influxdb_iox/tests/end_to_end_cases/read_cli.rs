use assert_cmd::Command;
use predicates::prelude::*;
use test_helpers::make_temp_file;

use crate::common::server_fixture::ServerFixture;

use super::scenario::rand_name;

#[tokio::test]
pub async fn test() {
    let server_fixture = ServerFixture::create_single_use().await;
    let db_name = rand_name();
    let addr = server_fixture.grpc_base();

    set_server_id(addr).await;
    wait_server_initialized(addr).await;
    create_database(&db_name, addr).await;
    test_read_default(&db_name, addr).await;
    test_read_format_pretty(&db_name, addr).await;
    test_read_format_csv(&db_name, addr).await;
    test_read_format_json(&db_name, addr).await;
    test_read_error(&db_name, addr).await;
}

async fn set_server_id(addr: &str) {
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("server")
        .arg("set")
        .arg("23")
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains("Ok"));
}

async fn wait_server_initialized(addr: &str) {
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("server")
        .arg("wait-server-initialized")
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains("Server initialized."));
}

async fn create_database(db_name: &str, addr: &str) {
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("create")
        .arg(db_name)
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains("Ok"));

    let lp_data = vec![
        "cpu,region=west user=23.2 100",
        "cpu,region=west user=21.0 150",
    ];

    let lp_data_file = make_temp_file(lp_data.join("\n"));

    // read from temp file
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("write")
        .arg(db_name)
        .arg(lp_data_file.as_ref())
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains("2 Lines OK"));
}

async fn test_read_default(db_name: &str, addr: &str) {
    let expected = r#"
+--------+--------------------------------+------+
| region | time                           | user |
+--------+--------------------------------+------+
| west   | 1970-01-01T00:00:00.000000100Z | 23.2 |
| west   | 1970-01-01T00:00:00.000000150Z | 21   |
+--------+--------------------------------+------+
"#
    .trim();

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("query")
        .arg(db_name)
        .arg("select * from cpu order by time")
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains(expected));
}

async fn test_read_format_pretty(db_name: &str, addr: &str) {
    let expected = r#"
+--------+--------------------------------+------+
| region | time                           | user |
+--------+--------------------------------+------+
| west   | 1970-01-01T00:00:00.000000100Z | 23.2 |
| west   | 1970-01-01T00:00:00.000000150Z | 21   |
+--------+--------------------------------+------+
"#
    .trim();

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("query")
        .arg(db_name)
        .arg("select * from cpu order by time")
        .arg("--host")
        .arg(addr)
        .arg("--format")
        .arg("pretty")
        .assert()
        .success()
        .stdout(predicate::str::contains(expected));
}

async fn test_read_format_csv(db_name: &str, addr: &str) {
    let expected =
        "west,1970-01-01T00:00:00.000000100,23.2\nwest,1970-01-01T00:00:00.000000150,21.0";

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("query")
        .arg(db_name)
        .arg("select * from cpu order by time")
        .arg("--host")
        .arg(addr)
        .arg("--format")
        .arg("csv")
        .assert()
        .success()
        .stdout(predicate::str::contains(expected));
}

async fn test_read_format_json(db_name: &str, addr: &str) {
    let expected = r#"[{"region":"west","time":"1970-01-01 00:00:00.000000100","user":23.2},{"region":"west","time":"1970-01-01 00:00:00.000000150","user":21.0}]"#;

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("query")
        .arg(db_name)
        .arg("select * from cpu order by time")
        .arg("--host")
        .arg(addr)
        .arg("--format")
        .arg("json")
        .assert()
        .success()
        .stdout(predicate::str::contains(expected));
}

async fn test_read_error(db_name: &str, addr: &str) {
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("query")
        .arg(db_name)
        .arg("select * from unknown_table")
        .arg("--host")
        .arg(addr)
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "Table or CTE with name 'unknown_table' not found",
        ));

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("query")
        .arg(db_name)
        .arg("select * from cpu order by time")
        .arg("--host")
        .arg(addr)
        .arg("--format")
        .arg("not_a_valid_format")
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "Unknown format type: not_a_valid_format. Expected one of 'pretty', 'csv' or 'json'",
        ));
}
