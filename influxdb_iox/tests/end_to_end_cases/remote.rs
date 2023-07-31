//! Tests the `influxdb_iox remote` commands
use std::path::Path;

use super::get_object_store_id;
use assert_cmd::Command;
use futures::FutureExt;
use import_export::file::ExportedContents;
use predicates::prelude::*;
use tempfile::tempdir;
use test_helpers_end_to_end::{maybe_skip_integration, MiniCluster, Step, StepTest, StepTestState};
use tokio::fs;

/// Get all Parquet files for a table, using the command `remote store get-table`
#[tokio::test]
async fn remote_store_get_table() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();
    let table_name = "my_awesome_table";
    let other_table_name = "my_ordinary_table";

    let mut cluster = MiniCluster::create_shared(database_url).await;

    StepTest::new(
        &mut cluster,
        vec![
            // Persist some data
            Step::RecordNumParquetFiles,
            Step::WriteLineProtocol(format!("{table_name},tag1=A,tag2=B val=42i 123456")),
            Step::WaitForPersisted {
                expected_increase: 1,
            },
            // Persist some more data for the same table in a 2nd Parquet file
            Step::RecordNumParquetFiles,
            Step::WriteLineProtocol(format!("{table_name},tag1=C,tag2=B val=9000i 789000")),
            Step::WaitForPersisted {
                expected_increase: 1,
            },
            // Persist some more data for a different table
            Step::RecordNumParquetFiles,
            Step::WriteLineProtocol(format!("{other_table_name},tag1=A,tag2=B val=42i 123456")),
            Step::WaitForPersisted {
                expected_increase: 1,
            },
            Step::Custom(Box::new(move |state: &mut StepTestState| {
                async move {
                    let router_addr = state.cluster().router().router_grpc_base().to_string();
                    let namespace = state.cluster().namespace().to_string();

                    // Ensure files are actually written to the filesystem
                    let dir = tempfile::tempdir().expect("could not get temporary directory");

                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .current_dir(&dir)
                        .arg("-h")
                        .arg(&router_addr)
                        .arg("remote")
                        .arg("store")
                        .arg("get-table")
                        .arg(&namespace)
                        .arg(table_name)
                        .assert()
                        .success();

                    // There should be a directory created that, by
                    // default, is named the same as the table
                    let table_dir = dir.as_ref().join(table_name);
                    assert_two_parquet_files_and_meta(&table_dir);

                    // The `-o` argument should specify where the files go instead of a directory
                    // named after the table. Note that this `Command` doesn't set `current dir`;
                    // the `-o` argument shouldn't have anything to do with the current working
                    // directory.
                    let custom_output_dir = dir.as_ref().join("my_special_directory");

                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-h")
                        .arg(&router_addr)
                        .arg("remote")
                        .arg("store")
                        .arg("get-table")
                        .arg("-o")
                        .arg(&custom_output_dir)
                        .arg(&namespace)
                        // This time ask for the table that only has one Parquet file
                        .arg(other_table_name)
                        .assert()
                        .success();

                    let contents = assert_one_parquet_file_and_meta(&custom_output_dir);

                    // Specifying a table that doesn't exist prints an error message
                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .current_dir(&dir)
                        .arg("-h")
                        .arg(&router_addr)
                        .arg("remote")
                        .arg("store")
                        .arg("get-table")
                        .arg(&namespace)
                        .arg("nacho-table")
                        .assert()
                        .failure()
                        .stderr(predicate::str::contains("Table nacho-table not found"));

                    // Specifying a namespace that doesn't exist prints an error message
                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .current_dir(&dir)
                        .arg("-h")
                        .arg(&router_addr)
                        .arg("remote")
                        .arg("store")
                        .arg("get-table")
                        .arg("nacho-namespace")
                        .arg(table_name)
                        .assert()
                        .failure()
                        .stderr(predicate::str::contains(
                            "Namespace nacho-namespace not found",
                        ));

                    // Running the same command again shouldn't download any new files
                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-h")
                        .arg(&router_addr)
                        .arg("remote")
                        .arg("store")
                        .arg("get-table")
                        .arg("-o")
                        .arg(&custom_output_dir)
                        .arg(&namespace)
                        .arg(other_table_name)
                        .assert()
                        .success()
                        .stdout(predicate::str::contains(format!(
                            "skipping file 1 of 1 ({} already exists with expected file size)",
                            contents.parquet_file_name(0).unwrap(),
                        )));

                    // If the file sizes don't match, re-download that file
                    fs::write(&contents.parquet_files()[0], b"not parquet")
                        .await
                        .unwrap();

                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-h")
                        .arg(&router_addr)
                        .arg("remote")
                        .arg("store")
                        .arg("get-table")
                        .arg("-o")
                        .arg(&custom_output_dir)
                        .arg(&namespace)
                        .arg(other_table_name)
                        .assert()
                        .success()
                        .stdout(predicate::str::contains(format!(
                            "downloading file 1 of 1 ({})...",
                            contents.parquet_file_name(0).unwrap(),
                        )));
                }
                .boxed()
            })),
            Step::Custom(Box::new(move |state: &mut StepTestState| {
                async move {
                    // Test that we can download files from the querier (not just the router)
                    // to ensure it has the correct grpc services
                    let querier_addr = state.cluster().querier().querier_grpc_base().to_string();
                    let namespace = state.cluster().namespace().to_string();

                    // Ensure files are actually written to the filesystem
                    let dir = tempfile::tempdir().expect("could not get temporary directory");

                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .current_dir(&dir)
                        .arg("-h")
                        .arg(&querier_addr)
                        .arg("remote")
                        .arg("store")
                        .arg("get-table")
                        .arg(&namespace)
                        .arg(table_name)
                        .assert()
                        .success();

                    // There should be a directory created that, by
                    // default, is named the same as the table
                    let table_dir = dir.as_ref().join(table_name);
                    assert_two_parquet_files_and_meta(&table_dir);
                }
                .boxed()
            })),
        ],
    )
    .run()
    .await
}

/// Asserts that the directory contains metadata and parquet files for
/// 1 partition that has 1 table with 1 parquet files
fn assert_one_parquet_file_and_meta(table_dir: &Path) -> ExportedContents {
    let contents = ExportedContents::try_new(table_dir).unwrap();
    assert_eq!(contents.parquet_files().len(), 1);
    assert_eq!(contents.parquet_json_files().len(), 1);
    assert_eq!(contents.table_json_files().len(), 1);
    assert_eq!(contents.partition_json_files().len(), 1);
    contents
}

/// Asserts that the directory contains metadata and parquet files for
/// 1 partition that has 1 table with 2 parquet files
fn assert_two_parquet_files_and_meta(table_dir: &Path) {
    let contents = ExportedContents::try_new(table_dir).unwrap();
    assert_eq!(contents.parquet_files().len(), 2);
    assert_eq!(contents.parquet_json_files().len(), 2);
    assert_eq!(contents.table_json_files().len(), 1);
    assert_eq!(contents.partition_json_files().len(), 1);
}

/// remote partition command and getting a parquet file from the object store and pulling the
/// files, using these commands:
///
/// - `remote partition show`
/// - `remote store get`
#[tokio::test]
async fn remote_partition_and_get_from_store_and_pull() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    // The test below assumes a specific partition id, so use a
    // non-shared one here so concurrent tests don't interfere with
    // each other
    let mut cluster = MiniCluster::create_non_shared(database_url).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::RecordNumParquetFiles,
            Step::WriteLineProtocol(String::from(
                "my_awesome_table,tag1=A,tag2=B val=42i 123456",
            )),
            // wait for partitions to be persisted
            Step::WaitForPersisted {
                expected_increase: 1,
            },
            // Run the 'remote partition' command
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    let router_addr = state.cluster().router().router_grpc_base().to_string();

                    // Validate the output of the remote partition CLI command
                    //
                    // Looks like:
                    // {
                    //     "id": "1",
                    //     "namespaceId": 1,
                    //     "tableId": 1,
                    //     "partitionId": "1",
                    //     "objectStoreId": "fa6cdcd1-cbc2-4fb7-8b51-4773079124dd",
                    //     "minTime": "123456",
                    //     "maxTime": "123456",
                    //     "fileSizeBytes": "2029",
                    //     "rowCount": "1",
                    //     "createdAt": "1650019674289347000"
                    // }

                    let out = Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-h")
                        .arg(&router_addr)
                        .arg("remote")
                        .arg("partition")
                        .arg("show")
                        .arg("1")
                        .assert()
                        .success()
                        .stdout(predicate::str::contains(
                            r#""hashId": "uGKn6bMp7mpBjN4ZEZjq6xUSdT8ZuHqB3vKubD0O0jc=""#,
                        ))
                        .get_output()
                        .stdout
                        .clone();

                    let object_store_id = get_object_store_id(&out);

                    let dir = tempdir().unwrap();
                    let f = dir.path().join("tmp.parquet");
                    let filename = f.as_os_str().to_str().unwrap();

                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-h")
                        .arg(&router_addr)
                        .arg("remote")
                        .arg("store")
                        .arg("get")
                        .arg(&object_store_id)
                        .arg(filename)
                        .assert()
                        .success()
                        .stdout(
                            predicate::str::contains("wrote")
                                .and(predicate::str::contains(filename)),
                        );
                }
                .boxed()
            })),
        ],
    )
    .run()
    .await
}
