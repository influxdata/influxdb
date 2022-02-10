//! Contains tests using the CLI and other tools to test scenarios for
//! moving data from one server/database to another

use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use arrow_util::assert_batches_eq;
use assert_cmd::Command;
use data_types::chunk_metadata::ChunkStorage;
use predicates::prelude::*;
use tempfile::TempDir;
use test_helpers::assert_contains;
use uuid::Uuid;

use crate::{
    common::server_fixture::{ServerFixture, ServerType},
    end_to_end_cases::scenario::{
        create_readable_database, data_dir, db_data_dir, rand_name, wait_for_database_initialized,
        wait_for_exact_chunk_states, wait_for_operations_to_complete,
    },
};

use super::scenario::DatabaseBuilder;

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

/// Creates a new database on a shared server, writes data to
/// `the_table` table, shuts it down cleanly, and copies the files to
/// Tempdir/uuid
///
/// Returns (db_name, uuid, tmp_dir)
async fn create_copied_database() -> (String, Uuid, TempDir) {
    let fixture = ServerFixture::create_single_use(ServerType::Database).await;
    let addr = fixture.grpc_base();

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
    let db_uuid = DatabaseBuilder::new(db_name.clone())
        .persist(true)
        .persist_age_threshold_seconds(1)
        .late_arrive_window_seconds(1)
        .build(fixture.grpc_channel())
        .await;

    let lp_lines: Vec<_> = (0..1_000)
        .map(|i| format!("the_table,tag1=val{} x={} {}", i, i * 10, i))
        .collect();

    let mut write_client = fixture.write_client();

    let num_lines_written = write_client
        .write_lp(&db_name, lp_lines.join("\n"), 0)
        .await
        .expect("successful write");
    assert_eq!(num_lines_written, 1000);

    wait_for_exact_chunk_states(
        &fixture,
        &db_name,
        vec![ChunkStorage::ReadBufferAndObjectStore],
        Duration::from_secs(5),
    )
    .await;

    // figure out where the database lives and copy its data to a temporary directory,
    // as you might copy data from remote object storage to local disk for debugging.
    let source_dir = db_data_dir(fixture.dir(), db_uuid);
    let tmp_dir = TempDir::new().expect("making tmp dir");
    cp_dir(source_dir, tmp_dir.path());

    // stop the first server (note this call blocks until the process stops)
    std::mem::drop(fixture);

    (db_name.to_string(), db_uuid, tmp_dir)
}

#[tokio::test]
async fn migrate_all_database_files_from_one_server_to_another() {
    // Create a database with some parquet files
    let (db_name, db_uuid, tmp_dir) = create_copied_database().await;

    // Now start another server that can claim the database
    let fixture = ServerFixture::create_shared(ServerType::Database).await;
    let addr = fixture.grpc_base();

    // copy the data from tmp_dir/<uuid> to the new server's location
    let mut source_dir: PathBuf = tmp_dir.path().into();
    source_dir.push(db_uuid.to_string());

    let target_dir = data_dir(fixture.dir());
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

#[tokio::test]
async fn migrate_table_files_from_one_server_to_another() {
    let (_, db_uuid, tmp_dir) = create_copied_database().await;

    // Now start another server and create a database to receive the files
    let fixture = ServerFixture::create_shared(ServerType::Database).await;
    let addr = fixture.grpc_base();

    let db_name = rand_name();
    let new_db_uuid = create_readable_database(&db_name, fixture.grpc_channel()).await;
    let sql_query = "select count(*) from the_table";

    // No data for the_table yet
    let mut flight_client = fixture.flight_client();
    let query_results = flight_client
        .perform_query(&db_name, sql_query)
        .await
        .unwrap_err()
        .to_string();
    assert_contains!(query_results, "'the_table' not found");

    // copy the data from tmp_dir/<uuid>/data/the_table to the new server's location
    let mut source_dir: PathBuf = tmp_dir.path().into();
    source_dir.push(db_uuid.to_string());
    source_dir.push("data");
    source_dir.push("the_table");

    let mut target_dir = db_data_dir(fixture.dir(), new_db_uuid);
    target_dir.push("data");
    cp_dir(source_dir, &target_dir);

    // rebuilding without --force doesn't work as db is in ok state
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("recover")
        .arg("rebuild")
        .arg(&db_name)
        .arg("--host")
        .arg(addr)
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "in invalid state (Initialized) for transition (RebuildPreservedCatalog)",
        ));

    // however with --force rebuilding will work
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("recover")
        .arg("rebuild")
        .arg(&db_name)
        .arg("--force") // sudo make me a sandwich
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains("operation"));

    // Wait for the rebuild to complete (maybe not needed given we
    // wait right below for the server to be initialized)
    wait_for_operations_to_complete(&fixture, &db_name, Duration::from_secs(5)).await;

    // Wait for all databases to complete re-initialization here
    wait_for_database_initialized(&fixture, &db_name, Duration::from_secs(5)).await;

    // Now the data should be available for the_table
    let batches = flight_client
        .perform_query(&db_name, sql_query)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let expected = vec![
        "+-----------------+",
        "| COUNT(UInt8(1)) |",
        "+-----------------+",
        "| 1000            |",
        "+-----------------+",
    ];

    assert_batches_eq!(expected, &batches);
}
