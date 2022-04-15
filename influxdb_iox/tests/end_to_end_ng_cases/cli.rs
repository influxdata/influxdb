use assert_cmd::Command;
use futures::FutureExt;
use predicates::prelude::*;
use test_helpers_end_to_end_ng::{maybe_skip_integration, MiniCluster, Step, StepTest, TestConfig};

/// Tests CLI commands

/// remote partition command
#[tokio::test]
async fn remote_partition() {
    let database_url = maybe_skip_integration!();

    let router2_config = TestConfig::new_router2(&database_url);
    // generate parquet files quickly
    let ingester_config = TestConfig::new_ingester(&router2_config).with_fast_parquet_generation();

    // Set up the cluster  ====================================
    let mut cluster = MiniCluster::new()
        .with_router2(router2_config)
        .await
        .with_ingester(ingester_config)
        .await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol(String::from(
                "my_awesome_table,tag1=A,tag2=B val=42i 123456",
            )),
            // wait for partitions to be persisted
            Step::WaitForPersisted,
            // Run the 'remote partition' command
            Step::Custom(Box::new(|cluster: &mut MiniCluster| {
                async {
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

                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-h")
                        .arg(cluster.router2().router_grpc_base().as_ref())
                        .arg("remote")
                        .arg("partition")
                        .arg("show")
                        .arg("1")
                        .assert()
                        .success()
                        .stdout(
                            predicate::str::contains(r#""id": "1""#)
                                .and(predicate::str::contains(r#""partitionId": "1","#)),
                        );
                }
                .boxed()
            })),
        ],
    )
    .run()
    .await
}
