use assert_cmd::Command;
use predicates::prelude::*;
use test_helpers::make_temp_file;

use crate::common::server_fixture::ServerFixture;

use super::util::rand_name;

pub async fn test(server_fixture: &ServerFixture) {
    let db_name = rand_name();
    let addr = server_fixture.grpc_url_base();
    create_database(&db_name, addr).await;
    test_write(&db_name, addr).await;
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
}

async fn test_write(db_name: &str, addr: &str) {
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

    // try reading a non existent file
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("write")
        .arg(db_name)
        .arg("this_file_does_not_exist")
        .arg("--host")
        .arg(addr)
        .assert()
        .failure()
        .stderr(
            predicate::str::contains(r#"Error reading file "this_file_does_not_exist":"#)
                .and(predicate::str::contains("No such file or directory")),
        );
}
