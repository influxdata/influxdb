//! Tests CLI commands

use assert_cmd::Command;
use futures::FutureExt;
use predicates::prelude::*;
use serde_json::Value;
use std::time::Duration;
use tempfile::tempdir;
use test_helpers_end_to_end_ng::{
    maybe_skip_integration, MiniCluster, Step, StepTest, StepTestState,
};

#[tokio::test]
async fn default_mode_is_run_all_in_one() {
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .args(&["-v"])
        .timeout(Duration::from_secs(2))
        .assert()
        .failure()
        .stdout(predicate::str::contains("starting all in one server"));
}

#[tokio::test]
async fn default_run_mode_is_all_in_one() {
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .args(&["run", "-v"])
        .timeout(Duration::from_secs(2))
        .assert()
        .failure()
        .stdout(predicate::str::contains("starting all in one server"));
}

/// remote partition command and getting a parquet file from the object store and pulling the files
#[tokio::test]
async fn remote_partition_and_get_from_store_and_pull() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    // The test below assumes a specific partition id, so use a
    // non-shared one here so concurrent tests don't interfere with
    // each other
    let mut cluster = MiniCluster::create_non_shared_standard(database_url).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol(String::from(
                "my_awesome_table,tag1=A,tag2=B val=42i 123456",
            )),
            // wait for partitions to be persisted
            Step::WaitForPersisted,
            // Run the 'remote partition' command
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    let router_addr = state.cluster().router2().router_grpc_base().to_string();
                    let namespace = state.cluster().namespace().to_string();

                    // Validate the output of the remote partittion CLI command
                    //
                    // Looks like:
                    // {
                    //     "id": "1",
                    //     "sequencerId": 1,
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
                        .stdout(
                            predicate::str::contains(r#""id": "1""#)
                                .and(predicate::str::contains(r#""partitionId": "1","#)),
                        )
                        .get_output()
                        .stdout
                        .clone();

                    let v: Value = serde_json::from_slice(&out).unwrap();
                    let id = v.as_array().unwrap()[0]
                        .as_object()
                        .unwrap()
                        .get("objectStoreId")
                        .unwrap()
                        .as_str()
                        .unwrap();

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
                        .arg(id)
                        .arg(filename)
                        .assert()
                        .success()
                        .stdout(
                            predicate::str::contains("wrote")
                                .and(predicate::str::contains(filename)),
                        );

                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-h")
                        .arg(&router_addr)
                        .arg("remote")
                        .arg("partition")
                        .arg("pull")
                        .arg("--catalog")
                        .arg("memory")
                        .arg("--object-store")
                        .arg("memory")
                        .arg(&namespace)
                        .arg("my_awesome_table")
                        .arg("1970-01-01")
                        .assert()
                        .success()
                        .stdout(
                            predicate::str::contains("wrote file")
                                .and(predicate::str::contains(id)),
                        );
                }
                .boxed()
            })),
        ],
    )
    .run()
    .await
}
