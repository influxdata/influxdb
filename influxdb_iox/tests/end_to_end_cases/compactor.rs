use assert_cmd::Command;
use futures::FutureExt;
use predicates::prelude::*;
use test_helpers_end_to_end::{
    maybe_skip_integration, MiniCluster, Step, StepTest, StepTestState, TestConfig,
};

#[tokio::test]
async fn shard_id_greater_than_num_shards_is_invalid() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    let ingester_config = TestConfig::new_ingester(&database_url);
    let router_config = TestConfig::new_router(&ingester_config);
    let querier_config = TestConfig::new_querier(&ingester_config).with_querier_mem_pool_bytes(1);
    let compactor_config = TestConfig::new_compactor(&ingester_config).with_compactor_shards(
        2,   // num shards 2
        100, // and shard id > num shards; not valid
    );

    let mut cluster = MiniCluster::new()
        .with_router(router_config)
        .await
        .with_ingester(ingester_config)
        .await
        .with_querier(querier_config)
        .await
        .with_compactor_config(compactor_config);

    StepTest::new(
        &mut cluster,
        vec![Step::CompactExpectingError {
            expected_message: "shard_id out of range".into(),
        }],
    )
    .run()
    .await
}

#[test]
fn shard_id_without_num_shards_is_invalid() {
    // This test doesn't use MiniCluster/TestConfig/with_compactor_shards because those exist to
    // enable *correct* configuration of components, and this test is testing an *invalid*
    // configuration of the compactor command.
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("run")
        .arg("compactor")
        .env("INFLUXDB_IOX_COMPACTION_SHARD_ID", "1") // only provide shard ID
        .env("INFLUXDB_IOX_CATALOG_DSN", "memory")
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "the following required arguments were not provided:\n  --compaction-shard-count <SHARD_COUNT>",
        ));
}

#[test]
fn num_shards_without_shard_id_is_invalid() {
    // This test doesn't use MiniCluster/TestConfig/with_compactor_shards because those exist to
    // enable *correct* configuration of components, and this test is testing an *invalid*
    // configuration of the compactor command.
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("run")
        .arg("compactor")
        .env("INFLUXDB_IOX_COMPACTION_SHARD_COUNT", "1") // only provide shard count
        .env_remove("HOSTNAME")
        .env("INFLUXDB_IOX_CATALOG_DSN", "memory")
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "shard_count must be paired with either shard_id or hostname",
        ));
}

#[test]
fn num_shards_with_hostname_is_valid() {
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("run")
        .arg("compactor")
        .env("INFLUXDB_IOX_COMPACTION_SHARD_COUNT", "3") // provide shard count
        .env("HOSTNAME", "iox-shared-compactor-8") // provide shard id via hostname
        .env("INFLUXDB_IOX_CATALOG_DSN", "memory")
        .assert()
        .failure()
        .stderr(predicate::str::contains("shard_id out of range"));
}

#[tokio::test]
async fn sharded_compactor_0_always_compacts_partition_1() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    // The test below assumes a specific partition id, and it needs to customize the compactor
    // config, so use a non-shared minicluster here.
    let ingester_config = TestConfig::new_ingester(&database_url);
    let router_config = TestConfig::new_router(&ingester_config);
    let querier_config = TestConfig::new_querier(&ingester_config).with_querier_mem_pool_bytes(1);
    let compactor_config = TestConfig::new_compactor(&ingester_config).with_compactor_shards(
        2, // num shards 2
        0, // shard ID 0, which will always get partition ID 1
    );

    let mut cluster = MiniCluster::new()
        .with_router(router_config)
        .await
        .with_ingester(ingester_config)
        .await
        .with_querier(querier_config)
        .await
        .with_compactor_config(compactor_config);

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
            // Run the compactor
            Step::Compact,
            // Run the 'remote partition' command
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    let router_addr = state.cluster().router().router_grpc_base().to_string();

                    // Validate the output of the remote partition CLI command
                    //
                    // Looks something like:
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
                    //     "compactionLevel": "1",
                    //     "createdAt": "1650019674289347000"
                    // }
                    Command::cargo_bin("influxdb_iox")
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
                            // Important parts are the expected partition identifier
                            predicate::str::contains(
                                r#""hashId": "uGKn6bMp7mpBjN4ZEZjq6xUSdT8ZuHqB3vKubD0O0jc=""#,
                            )
                            // and compaction level
                            .and(predicate::str::contains(r#""compactionLevel": 1"#)),
                        );
                }
                .boxed()
            })),
        ],
    )
    .run()
    .await
}

#[tokio::test]
async fn sharded_compactor_1_never_compacts_partition_1() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    // The test below assumes a specific partition id, and it needs to customize the compactor
    // config, so use a non-shared minicluster here.
    let ingester_config = TestConfig::new_ingester(&database_url);
    let router_config = TestConfig::new_router(&ingester_config);
    let querier_config = TestConfig::new_querier(&ingester_config).with_querier_mem_pool_bytes(1);
    let compactor_config = TestConfig::new_compactor(&ingester_config).with_compactor_shards(
        2, // num shards 2
        1, // shard ID 1, which will never get partition ID 1
    );

    let mut cluster = MiniCluster::new()
        .with_router(router_config)
        .await
        .with_ingester(ingester_config)
        .await
        .with_querier(querier_config)
        .await
        .with_compactor_config(compactor_config);

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
            // Run the compactor
            Step::Compact,
            // Run the 'remote partition' command
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    let router_addr = state.cluster().router().router_grpc_base().to_string();

                    // Validate the output of the remote partition CLI command
                    //
                    // Looks something like:
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
                    //     "compactionLevel": "1",
                    //     "createdAt": "1650019674289347000"
                    // }
                    Command::cargo_bin("influxdb_iox")
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
                            // Important parts are the expected partition identifier
                            predicate::str::contains(
                                r#""hashId": "uGKn6bMp7mpBjN4ZEZjq6xUSdT8ZuHqB3vKubD0O0jc=""#,
                            )
                            // and compaction level is 0 so it's not returned
                            .and(predicate::str::contains("compactionLevel").not()),
                        );
                }
                .boxed()
            })),
        ],
    )
    .run()
    .await
}
