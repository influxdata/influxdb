use assert_cmd::Command;
use predicates::prelude::*;
use test_helpers::make_temp_file;

use crate::common::server_fixture::ServerFixture;

use super::util::rand_name;

pub async fn test(server_fixture: &ServerFixture) {
    let db_name = rand_name();
    let addr = server_fixture.grpc_url_base();
    create_database(&db_name, addr).await;
    test_read_default(&db_name, addr).await;
    test_read_format_pretty(&db_name, addr).await;
    test_read_format_csv(&db_name, addr).await;
    test_read_format_json(&db_name, addr).await;
    test_read_error(&db_name, addr).await;
}

async fn create_database(db_name: &str, addr: &str) {
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("create")
        .arg(db_name)
        .arg("-m")
        .arg("100") // give it a mutable buffer
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
    let expected = "+--------+------+------+\n\
                    | region | time | user |\n\
                    +--------+------+------+\n\
                    | west   | 100  | 23.2 |\n\
                    | west   | 150  | 21   |\n\
                    +--------+------+------+";

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("query")
        .arg(db_name)
        .arg("select * from cpu")
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains(expected));
}

async fn test_read_format_pretty(db_name: &str, addr: &str) {
    let expected = "+--------+------+------+\n\
                    | region | time | user |\n\
                    +--------+------+------+\n\
                    | west   | 100  | 23.2 |\n\
                    | west   | 150  | 21   |\n\
                    +--------+------+------+";

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("query")
        .arg(db_name)
        .arg("select * from cpu")
        .arg("--host")
        .arg(addr)
        .arg("--format")
        .arg("pretty")
        .assert()
        .success()
        .stdout(predicate::str::contains(expected));
}

async fn test_read_format_csv(db_name: &str, addr: &str) {
    let expected = "region,time,user\nwest,100,23.2\nwest,150,21.0";

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("query")
        .arg(db_name)
        .arg("select * from cpu")
        .arg("--host")
        .arg(addr)
        .arg("--format")
        .arg("csv")
        .assert()
        .success()
        .stdout(predicate::str::contains(expected));
}

async fn test_read_format_json(db_name: &str, addr: &str) {
    let expected =
        r#"[{"region":"west","time":100,"user":23.2},{"region":"west","time":150,"user":21.0}]"#;

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("query")
        .arg(db_name)
        .arg("select * from cpu")
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
            "no chunks found in builder for table",
        ));

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("query")
        .arg(db_name)
        .arg("select * from cpu")
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
