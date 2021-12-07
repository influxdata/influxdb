//! Contains tests using the CLI and other tools to test scenarios for
//! moving data from one server/database to another

use std::path::{Path, PathBuf};

use assert_cmd::Command;
use predicates::prelude::*;
use tempfile::TempDir;
use uuid::Uuid;

use crate::{
    common::server_fixture::{ServerFixture, ServerType},
    end_to_end_cases::scenario::{data_dir, db_data_dir, Scenario},
};

/// Copy the `source_dir` directory into the `target_dir` directory using the `cp` command
fn cp_dir(source_dir: impl AsRef<Path>, target_dir: impl AsRef<Path>) {
    let source_dir = source_dir.as_ref();
    let target_dir = target_dir.as_ref();

    // needed so that if the target server has had no databases
    // created yet, it will have no `data` directory. See #3292
    println!("Ensuring {:?} directory exists", target_dir);
    Command::new("mkdir")
        .arg("-p")
        .arg(target_dir.to_string_lossy().to_string())
        .assert()
        .success();

    println!("Copying data from {:?} to {:?}", source_dir, target_dir);

    Command::new("cp")
        .arg("-R")
        .arg(source_dir.to_string_lossy().to_string())
        .arg(target_dir.to_string_lossy().to_string())
        .assert()
        .success();
}

/// Creates a new database on a shared server, writes data to it,
/// shuts it down cleanly, and copies the files to Tempdir/uuid
///
/// Returns (db_name, uuid, tmp_dir)
async fn create_copied_database() -> (String, Uuid, TempDir) {
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

    let mut management_client = server_fixture.management_client();
    let scenario = Scenario::new();
    let (db_name, db_uuid) = scenario.create_database(&mut management_client).await;

    // todo write data and force it to be written to disk

    // figure out where the database lives and copy its data to a temporary directory,
    // as you might copy data from remote object storage to local disk for debugging.
    let source_dir = db_data_dir(server_fixture.dir(), db_uuid);
    let tmp_dir = TempDir::new().expect("making tmp dir");
    cp_dir(source_dir, tmp_dir.path());

    // stop the first server (note this call blocks until the process stops)
    std::mem::drop(server_fixture);

    (db_name.to_string(), db_uuid, tmp_dir)
}

#[tokio::test]
async fn migrate_database_files_from_one_server_to_another() {
    let (db_name, db_uuid, tmp_dir) = create_copied_database().await;

    // Now start another server that can claim the database
    let server_fixture = ServerFixture::create_shared(ServerType::Database).await;
    let addr = server_fixture.grpc_base();

    // copy the data from tmp_dir/<uuid> to the new server's location
    let mut source_dir: PathBuf = tmp_dir.path().into();
    source_dir.push(db_uuid.to_string());

    let target_dir = data_dir(server_fixture.dir());
    cp_dir(source_dir, &target_dir);

    // Claiming without --force doesn't work as owner.pb still record the other server owning it
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("claim")
        .arg(db_uuid.to_string())
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
        .arg(db_uuid.to_string())
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
