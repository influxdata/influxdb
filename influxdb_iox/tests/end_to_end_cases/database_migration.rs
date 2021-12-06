use std::path::PathBuf;

use assert_cmd::Command;
use predicates::prelude::*;
use tempfile::TempDir;

use crate::{
    common::server_fixture::{ServerFixture, ServerType},
    end_to_end_cases::scenario::rand_name,
};

/// Contains tests using the CLI and other tools to test scenarios for
/// moving data from one server/database to another
#[tokio::test]
async fn migrate_database_files_from_one_server_to_another() {
    let server_fixture = ServerFixture::create_single_use(ServerType::Database).await;
    let addr = server_fixture.grpc_base();

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("server")
        .arg("set")
        .arg("3113")
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains("Ok"));

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("server")
        .arg("wait-server-initialized")
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains("Server initialized."));

    let db_name = rand_name();
    let db = &db_name;

    // Create a database on one server
    let stdout = String::from_utf8(
        Command::cargo_bin("influxdb_iox")
            .unwrap()
            .arg("database")
            .arg("create")
            .arg(db)
            .arg("--host")
            .arg(addr)
            .assert()
            .success()
            .stdout(predicate::str::contains("Created"))
            .get_output()
            .stdout
            .clone(),
    )
    .unwrap();

    let db_uuid = stdout.lines().last().unwrap().trim();

    // figure out where the database lives and copy its data to a temporary directory,
    // as you might copy data from remote object storage to local disk for debugging.

    // Assume data layout is <dir>/dbs/<uuid>
    let mut source_dir: PathBuf = server_fixture.dir().into();
    source_dir.push("dbs");
    source_dir.push(db_uuid);

    let tmp_dir = TempDir::new().expect("making tmp dir");
    let target_dir = tmp_dir.path();
    println!("Copying data from {:?} to {:?}", source_dir, target_dir);

    Command::new("cp")
        .arg("-R")
        .arg(source_dir.to_string_lossy().to_string())
        .arg(target_dir.to_string_lossy().to_string())
        .assert()
        .success();

    // stop the first server (note this call blocks until the process stops)
    std::mem::drop(server_fixture);

    // Now start another server that can claim the database
    let server_fixture = ServerFixture::create_shared(ServerType::Database).await;
    let addr = server_fixture.grpc_base();

    // copy the data from tmp_dir/<uuid> to the new server's location
    let mut source_dir: PathBuf = tmp_dir.path().into();
    source_dir.push(db_uuid);

    let mut target_dir: PathBuf = server_fixture.dir().into();
    target_dir.push("dbs");

    println!("Copying data from {:?} to {:?}", source_dir, target_dir);
    Command::new("cp")
        .arg("-R")
        .arg(source_dir.to_string_lossy().to_string())
        .arg(target_dir.to_string_lossy().to_string())
        .assert()
        .success();

    // Claiming without --force doesn't work as owner.pb still record the other server owning it
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("claim")
        .arg(db_uuid)
        .arg("--host")
        .arg(addr)
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "is already owned by the server with ID 3113",
        ));

    // however with --force the owner.pb file is updated forcibly
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("claim")
        .arg(db_uuid)
        .arg("--host")
        .arg(addr)
        .arg("--force") // sudo make me a sandwich
        .assert()
        .success()
        .stdout(predicate::str::contains(format!(
            "Claimed database {}",
            db_name
        )));
}
