//! Test `influxdb_iox namespace` commands

use assert_cmd::Command;
use futures::FutureExt;
use predicates::prelude::*;
use test_helpers_end_to_end::{maybe_skip_integration, MiniCluster, Step, StepTest, StepTestState};

use super::wait_for_query_result_with_namespace;

#[tokio::test]
async fn list() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    let mut cluster = MiniCluster::create_shared(database_url).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol(String::from(
                "my_awesome_table2,tag1=A,tag2=B val=42i 123456",
            )),
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    let querier_addr = state.cluster().querier().querier_grpc_base().to_string();

                    // Validate the output of the schema CLI command
                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-h")
                        .arg(&querier_addr)
                        .arg("namespace")
                        .arg("list")
                        .assert()
                        .success()
                        .stdout(predicate::str::contains(state.cluster().namespace()));
                }
                .boxed()
            })),
        ],
    )
    .run()
    .await
}

#[tokio::test]
async fn retention() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();
    let mut cluster = MiniCluster::create_shared(database_url).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol(String::from(
                "my_awesome_table2,tag1=A,tag2=B val=42i 123456",
            )),
            // Set the retention period to 2 hours
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    let addr = state.cluster().router().router_grpc_base().to_string();
                    let namespace = state.cluster().namespace();
                    let retention_period_hours = 2;
                    let retention_period_ns =
                        retention_period_hours as i64 * 60 * 60 * 1_000_000_000;

                    // Validate the output of the namespace retention command
                    //
                    //     {
                    //      "id": "1",
                    //      "name": "0911430016317810_8303971312605107",
                    //      "retentionPeriodNs": "7200000000000"
                    //    }
                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-h")
                        .arg(&addr)
                        .arg("namespace")
                        .arg("retention")
                        .arg("--retention-hours")
                        .arg(retention_period_hours.to_string())
                        .arg(namespace)
                        .assert()
                        .success()
                        .stdout(
                            predicate::str::contains(namespace)
                                .and(predicate::str::contains(retention_period_ns.to_string())),
                        );
                }
                .boxed()
            })),
            // set the retention period to null
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    let addr = state.cluster().router().router_grpc_base().to_string();
                    let namespace = state.cluster().namespace();
                    let retention_period_hours = 0; // will be updated to null

                    // Validate the output of the namespace retention command
                    //
                    //     {
                    //      "id": "1",
                    //      "name": "6699752880299094_1206270074309156"
                    //    }
                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-h")
                        .arg(&addr)
                        .arg("namespace")
                        .arg("retention")
                        .arg("--retention-hours")
                        .arg(retention_period_hours.to_string())
                        .arg(namespace)
                        .assert()
                        .success()
                        .stdout(
                            predicate::str::contains(namespace)
                                .and(predicate::str::contains("retentionPeriodNs".to_string()))
                                .not(),
                        );
                }
                .boxed()
            })),
            // create a new namespace and set the retention period to 2 hours
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    let addr = state.cluster().router().router_grpc_base().to_string();
                    let namespace = "namespace_2";
                    let retention_period_hours = 2;
                    let retention_period_ns =
                        retention_period_hours as i64 * 60 * 60 * 1_000_000_000;

                    // Validate the output of the namespace retention command
                    //
                    //     {
                    //      "id": "1",
                    //      "name": "namespace_2",
                    //      "retentionPeriodNs": "7200000000000"
                    //    }
                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-h")
                        .arg(&addr)
                        .arg("namespace")
                        .arg("create")
                        .arg("--retention-hours")
                        .arg(retention_period_hours.to_string())
                        .arg(namespace)
                        .assert()
                        .success()
                        .stdout(
                            predicate::str::contains(namespace)
                                .and(predicate::str::contains(retention_period_ns.to_string())),
                        );
                }
                .boxed()
            })),
            // create a namespace without retention. 0 represeting null/infinite will be used
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    let addr = state.cluster().router().router_grpc_base().to_string();
                    let namespace = "namespace_3";

                    // Validate the output of the namespace retention command
                    //
                    //     {
                    //      "id": "1",
                    //      "name": "namespace_3",
                    //    }
                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-h")
                        .arg(&addr)
                        .arg("namespace")
                        .arg("create")
                        .arg(namespace)
                        .assert()
                        .success()
                        .stdout(
                            predicate::str::contains(namespace)
                                .and(predicate::str::contains("retentionPeriodNs".to_string()))
                                .not(),
                        );
                }
                .boxed()
            })),
            // create a namespace retention 0 represeting null/infinite will be used
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    let addr = state.cluster().router().router_grpc_base().to_string();
                    let namespace = "namespace_4";
                    let retention_period_hours = 0;

                    // Validate the output of the namespace retention command
                    //
                    //     {
                    //      "id": "1",
                    //      "name": "namespace_4",
                    //    }
                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-h")
                        .arg(&addr)
                        .arg("namespace")
                        .arg("create")
                        .arg("--retention-hours")
                        .arg(retention_period_hours.to_string())
                        .arg(namespace)
                        .assert()
                        .success()
                        .stdout(
                            predicate::str::contains(namespace)
                                .and(predicate::str::contains("retentionPeriodNs".to_string()))
                                .not(),
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
async fn deletion() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();
    let mut cluster = MiniCluster::create_shared(database_url).await;

    const NAMESPACE_NAME: &str = "bananas_namespace";

    StepTest::new(
        &mut cluster,
        vec![
            // create a new namespace without retention policy
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    let addr = state.cluster().router().router_grpc_base().to_string();
                    let retention_period_hours = 0;

                    // Validate the output of the namespace retention command
                    //
                    //     {
                    //      "id": "1",
                    //      "name": "bananas_namespace",
                    //    }
                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-h")
                        .arg(&addr)
                        .arg("namespace")
                        .arg("create")
                        .arg("--retention-hours")
                        .arg(retention_period_hours.to_string())
                        .arg(NAMESPACE_NAME)
                        .assert()
                        .success()
                        .stdout(
                            predicate::str::contains(NAMESPACE_NAME)
                                .and(predicate::str::contains("retentionPeriodNs".to_string()))
                                .not(),
                        );
                }
                .boxed()
            })),
            // delete the newly created namespace
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    let addr = state.cluster().router().router_grpc_base().to_string();

                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-h")
                        .arg(&addr)
                        .arg("namespace")
                        .arg("delete")
                        .arg(NAMESPACE_NAME)
                        .assert()
                        .success()
                        .stdout(
                            predicate::str::contains("Deleted namespace")
                                .and(predicate::str::contains(NAMESPACE_NAME)),
                        );
                }
                .boxed()
            })),
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    let addr = state.cluster().router().router_grpc_base().to_string();

                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-h")
                        .arg(&addr)
                        .arg("namespace")
                        .arg("list")
                        .assert()
                        .success()
                        .stdout(predicate::str::contains(NAMESPACE_NAME).not());
                }
                .boxed()
            })),
        ],
    )
    .run()
    .await
}

#[tokio::test]
async fn create_service_limits() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();
    let mut cluster = MiniCluster::create_shared(database_url).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    let addr = state.cluster().router().router_grpc_base().to_string();
                    let namespace_name = "ns1";

                    // {
                    //   "id": <foo>,
                    //   "name": "ns1",
                    //   "serviceProtectionLimits": {
                    //     "maxTables": 123,
                    //     "maxColumnsPerTable": 200
                    //   }
                    // }
                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-h")
                        .arg(&addr)
                        .arg("namespace")
                        .arg("create")
                        .arg(namespace_name)
                        .arg("--max-tables")
                        .arg("123")
                        .assert()
                        .success()
                        .stdout(
                            predicate::str::contains(namespace_name)
                                .and(predicate::str::contains(r#""maxTables": 123"#))
                                .and(predicate::str::contains(r#""maxColumnsPerTable": 200"#)),
                        );
                }
                .boxed()
            })),
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    let addr = state.cluster().router().router_grpc_base().to_string();
                    let namespace_name = "ns2";

                    // {
                    //   "id": <foo>,
                    //   "name": "ns2",
                    //   "serviceProtectionLimits": {
                    //     "maxTables": 500,
                    //     "maxColumnsPerTable": 321
                    //   }
                    // }
                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-h")
                        .arg(&addr)
                        .arg("namespace")
                        .arg("create")
                        .arg(namespace_name)
                        .arg("--max-columns-per-table")
                        .arg("321")
                        .assert()
                        .success()
                        .stdout(
                            predicate::str::contains(namespace_name)
                                .and(predicate::str::contains(r#""maxTables": 500"#))
                                .and(predicate::str::contains(r#""maxColumnsPerTable": 321"#)),
                        );
                }
                .boxed()
            })),
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    let addr = state.cluster().router().router_grpc_base().to_string();
                    let namespace_name = "ns3";

                    // {
                    //   "id": <foo>,
                    //   "name": "ns3",
                    //   "serviceProtectionLimits": {
                    //     "maxTables": 123,
                    //     "maxColumnsPerTable": 321
                    //   }
                    // }
                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-h")
                        .arg(&addr)
                        .arg("namespace")
                        .arg("create")
                        .arg(namespace_name)
                        .arg("--max-tables")
                        .arg("123")
                        .arg("--max-columns-per-table")
                        .arg("321")
                        .assert()
                        .success()
                        .stdout(
                            predicate::str::contains(namespace_name)
                                .and(predicate::str::contains(r#""maxTables": 123"#))
                                .and(predicate::str::contains(r#""maxColumnsPerTable": 321"#)),
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
async fn update_service_limit() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();
    let mut cluster = MiniCluster::create_shared(database_url).await;

    const NAMESPACE_NAME: &str = "service_limiter_namespace";

    StepTest::new(
        &mut cluster,
        vec![
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    let addr = state.cluster().router().router_grpc_base().to_string();

                    // {
                    //   "id": <foo>,
                    //   "name": "service_limiter_namespace",
                    //   "serviceProtectionLimits": {
                    //     "maxTables": 500,
                    //     "maxColumnsPerTable": 200
                    //   }
                    // }
                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-h")
                        .arg(&addr)
                        .arg("namespace")
                        .arg("create")
                        .arg(NAMESPACE_NAME)
                        .assert()
                        .success()
                        .stdout(
                            predicate::str::contains(NAMESPACE_NAME)
                                .and(predicate::str::contains(r#""maxTables": 500"#))
                                .and(predicate::str::contains(r#""maxColumnsPerTable": 200"#)),
                        );
                }
                .boxed()
            })),
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    let addr = state.cluster().router().router_grpc_base().to_string();

                    // {
                    //   "id": <foo>,
                    //   "name": "service_limiter_namespace",
                    //   "serviceProtectionLimits": {
                    //     "maxTables": 1337,
                    //     "maxColumnsPerTable": 200
                    //   }
                    // }
                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-h")
                        .arg(&addr)
                        .arg("namespace")
                        .arg("update-limit")
                        .arg("--max-tables")
                        .arg("1337")
                        .arg(NAMESPACE_NAME)
                        .assert()
                        .success()
                        .stdout(
                            predicate::str::contains(NAMESPACE_NAME)
                                .and(predicate::str::contains(r#""maxTables": 1337"#))
                                .and(predicate::str::contains(r#""maxColumnsPerTable": 200"#)),
                        );
                }
                .boxed()
            })),
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    let addr = state.cluster().router().router_grpc_base().to_string();

                    // {
                    //   "id": <foo>,
                    //   "name": "service_limiter_namespace",
                    //   "serviceProtectionLimits": {
                    //     "maxTables": 1337,
                    //     "maxColumnsPerTable": 42
                    //   }
                    // }
                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-h")
                        .arg(&addr)
                        .arg("namespace")
                        .arg("update-limit")
                        .arg("--max-columns-per-table")
                        .arg("42")
                        .arg(NAMESPACE_NAME)
                        .assert()
                        .success()
                        .stdout(
                            predicate::str::contains(NAMESPACE_NAME)
                                .and(predicate::str::contains(r#""maxTables": 1337"#))
                                .and(predicate::str::contains(r#""maxColumnsPerTable": 42"#)),
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
async fn create_partition_template_negative() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();
    let mut cluster = MiniCluster::create_shared(database_url).await;

    const NAMESPACE_NAME: &str = "ns_negative";

    StepTest::new(
        &mut cluster,
        vec![
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    let addr = state.cluster().router().router_grpc_base().to_string();

                    // No partition tempplate specified
                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-h")
                        .arg(&addr)
                        .arg("namespace")
                        .arg("create")
                        .arg(NAMESPACE_NAME)
                        .arg("--partition-template")
                        .assert()
                        .failure()
                        .stderr(
                            predicate::str::contains(
                                "error: a value is required for '--partition-template <PARTITION_TEMPLATE>' but none was supplied"
                            )
                        );

                    // Wrong spelling `prts`
                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-h")
                        .arg(&addr)
                        .arg("namespace")
                        .arg("create")
                        .arg(NAMESPACE_NAME)
                        .arg("--partition-template")
                        .arg("{\"prts\": [{\"tagValue\": \"location\"}, {\"tagValue\": \"state\"}, {\"timeFormat\": \"%Y-%m\"}] }")
                        .assert()
                        .failure()
                        .stderr(predicate::str::contains(
                            "Client Error: Invalid partition template format : unknown field `prts`",
                        ));

                    // Time as tag
                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-h")
                        .arg(&addr)
                        .arg("namespace")
                        .arg("create")
                        .arg(NAMESPACE_NAME)
                        .arg("--partition-template")
                        .arg("{\"parts\": [{\"tagValue\": \"location\"}, {\"tagValue\": \"time\"}, {\"timeFormat\": \"%Y-%m\"}] }")
                        .assert()
                        .failure()
                        .stderr(predicate::str::contains(
                            "Client error: Client specified an invalid argument: invalid tag value in partition template: time cannot be used",
                        ));

                    // Time format is `%42` which is invalid
                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-h")
                        .arg(&addr)
                        .arg("namespace")
                        .arg("create")
                        .arg(NAMESPACE_NAME)
                        .arg("--partition-template")
                        .arg("{\"parts\": [{\"tagValue\": \"location\"}, {\"timeFormat\": \"%42\"}] }")
                        .assert()
                        .failure()
                        .stderr(predicate::str::contains(
                            "Client error: Client specified an invalid argument: invalid strftime format in partition template",
                        ));

                    // Over 8 parts
                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-h")
                        .arg(&addr)
                        .arg("namespace")
                        .arg("create")
                        .arg(NAMESPACE_NAME)
                        .arg("--partition-template")
                        .arg("{\"parts\": [{\"tagValue\": \"1\"},{\"tagValue\": \"2\"},{\"timeFormat\": \"%Y-%m\"},{\"tagValue\": \"4\"},{\"tagValue\": \"5\"},{\"tagValue\": \"6\"},{\"tagValue\": \"7\"},{\"tagValue\": \"8\"},{\"tagValue\": \"9\"}]}")
                        .assert()
                        .failure()
                        .stderr(predicate::str::contains(
                            "Partition templates may have a maximum of 8 parts",
                        ));
                }
                .boxed()
            }))
        ],
    )
    .run()
    .await
}

#[tokio::test]
async fn create_partition_template_positive() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();
    let mut cluster = MiniCluster::create_shared(database_url).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    let addr = state.cluster().router().router_grpc_base().to_string();

                    let namespace_name_1 = "ns_partition_template_1";
                    // No partition template specified
                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-h")
                        .arg(&addr)
                        .arg("namespace")
                        .arg("create")
                        .arg(namespace_name_1)
                        .assert()
                        .success()
                        .stdout(predicate::str::contains(namespace_name_1));

                    // Partition template with time format
                    let namespace_name_2 = "ns_partition_template_2";
                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-h")
                        .arg(&addr)
                        .arg("namespace")
                        .arg("create")
                        .arg(namespace_name_2)
                        .arg("--partition-template")
                        .arg("{\"parts\":[{\"timeFormat\":\"%Y-%m\"}] }")
                        .assert()
                        .success()
                        .stdout(predicate::str::contains(namespace_name_2));

                    // Partition template with tag value
                    let namespace_name_3 = "ns_partition_template_3";
                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-h")
                        .arg(&addr)
                        .arg("namespace")
                        .arg("create")
                        .arg(namespace_name_3)
                        .arg("--partition-template")
                        .arg("{\"parts\":[{\"tagValue\":\"col1\"}] }")
                        .assert()
                        .success()
                        .stdout(predicate::str::contains(namespace_name_3));

                    // Partition template with time format, tag value, and tag of unsual column name
                    let namespace_name_4 = "ns_partition_template_4";
                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-h")
                        .arg(&addr)
                        .arg("namespace")
                        .arg("create")
                        .arg(namespace_name_4)
                        .arg("--partition-template")
                        .arg("{\"parts\":[{\"tagValue\":\"col1\"},{\"timeFormat\":\"%Y-%d\"},{\"tagValue\":\"yes,col name\"}] }")
                        .assert()
                        .success()
                        .stdout(predicate::str::contains(namespace_name_4));

                    // Update an existing namespace
                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-h")
                        .arg(&addr)
                        .arg("namespace")
                        .arg("update")
                        .arg(namespace_name_4)
                        .arg("--partition-template")
                        .arg("{\"parts\":[{\"tagValue\":\"col1\"}] }")
                        .assert()
                        .failure()
                        .stderr(predicate::str::contains(
                            "error: unrecognized subcommand 'update'",
                        ));
                }
                .boxed()
            })),
        ],
    )
    .run()
    .await
}

/// Test partition template for namespace and table creation:
///   When a namespace is created *with* a custom partition template
///   and a table is created implicitly, i.e. *without* a partition template,
///   the namespace's partition template will be applied to this table
#[tokio::test]
async fn create_partition_template_implicit_table_creation() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();
    let mut cluster = MiniCluster::create_shared(database_url).await;

    const NAMESPACE_NAME: &str = "ns_createtableimplicit";

    StepTest::new(
        &mut cluster,
        vec![
            // Explicitly create a namespace with a custom partition template
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    let addr = state.cluster().router().router_grpc_base().to_string();

                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-h")
                        .arg(&addr)
                        .arg("namespace")
                        .arg("create")
                        .arg(NAMESPACE_NAME)
                        .arg("--partition-template")
                        .arg(
                            "{\"parts\":[{\"timeFormat\":\"%Y-%m\"}, {\"tagValue\":\"location\"}]}",
                        )
                        .assert()
                        .success()
                        .stdout(predicate::str::contains(NAMESPACE_NAME));
                }
                .boxed()
            })),
            // Write, which implicitly creates the table with the namespace's custom partition template
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    let addr = state.cluster().router().router_http_base().to_string();

                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-v")
                        .arg("-h")
                        .arg(&addr)
                        .arg("write")
                        .arg(NAMESPACE_NAME)
                        .arg("../test_fixtures/lineproto/temperature.lp")
                        .assert()
                        .success()
                        .stdout(predicate::str::contains("591 Bytes OK"));
                }
                .boxed()
            })),
            // Read data
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    // data from 'air_and_water.lp'
                    wait_for_query_result_with_namespace(
                        NAMESPACE_NAME,
                        state,
                        "SELECT * from h2o_temperature order by time desc limit 10",
                        None,
                        "| 51.3           | coyote_creek | CA    | 55.1            | 1970-01-01T00:00:01.568756160Z |"
                    ).await;
                }
                .boxed()
            })),
            // Check partition keys that use the namespace's partition template
            Step::PartitionKeys {
                table_name: "h2o_temperature".to_string(),
                namespace_name: Some(NAMESPACE_NAME.to_string()),
                expected: vec![
                    "1970-01|coyote_creek",
                    "1970-01|puget_sound",
                    "1970-01|santa_monica",
                ],
            },
        ],
    )
    .run()
    .await
}

/// Test partition template for namespace and table creation:
///   When a namespace is created *with* a custom partition template
///   and a table is created *without* a partition template,
///   the namespace's partition template will be applied to this table
#[tokio::test]
async fn create_partition_template_explicit_table_creation_without_partition_template() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();
    let mut cluster = MiniCluster::create_shared(database_url).await;

    const NAMESPACE_NAME: &str = "ns_createtableexplicitwithout";

    StepTest::new(
        &mut cluster,
        vec![
            // Explicitly create a namespace with a custom partition template
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    let addr = state.cluster().router().router_grpc_base().to_string();

                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-h")
                        .arg(&addr)
                        .arg("namespace")
                        .arg("create")
                        .arg(NAMESPACE_NAME)
                        .arg("--partition-template")
                        .arg("{\"parts\":[{\"timeFormat\":\"%Y-%m\"}, {\"tagValue\":\"state\"}]}")
                        .assert()
                        .success()
                        .stdout(predicate::str::contains(NAMESPACE_NAME));
                }
                .boxed()
            })),
            // Explicitly create a table *without* a custom partition template
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    let addr = state.cluster().router().router_grpc_base().to_string();
                    let table_name = "h2o_temperature";

                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-v")
                        .arg("-h")
                        .arg(&addr)
                        .arg("table")
                        .arg("create")
                        .arg(NAMESPACE_NAME)
                        .arg(table_name)
                        .assert()
                        .success()
                        .stdout(predicate::str::contains(table_name));
                }
                .boxed()
            })),
            // Write to the just-created table
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    let addr = state.cluster().router().router_http_base().to_string();

                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-v")
                        .arg("-h")
                        .arg(&addr)
                        .arg("write")
                        .arg(NAMESPACE_NAME)
                        .arg("../test_fixtures/lineproto/temperature.lp")
                        .assert()
                        .success()
                        .stdout(predicate::str::contains("591 Bytes OK"));
                }
                .boxed()
            })),
            // Read data
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    // data from 'air_and_water.lp'
                    wait_for_query_result_with_namespace(
                        NAMESPACE_NAME,
                        state,
                        "SELECT * from h2o_temperature order by time desc limit 10",
                        None,
                        "| 51.3           | coyote_creek | CA    | 55.1            | 1970-01-01T00:00:01.568756160Z |"
                    ).await;
                }
                .boxed()
            })),
            // Check partition keys that use the namespace's partition template
            Step::PartitionKeys{table_name: "h2o_temperature".to_string(), namespace_name: Some(NAMESPACE_NAME.to_string()), expected: vec!["1970-01|CA", "1970-01|WA"]},
        ],
    )
    .run()
    .await
}

/// Test partition template for namespace and table creation:
///   When a namespace is created *with* a custom partition template
///   and a table is created *with* a partition template,
///   the table's partition template will be applied to this table
#[tokio::test]
async fn create_partition_template_explicit_table_creation_with_partition_template() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();
    let mut cluster = MiniCluster::create_shared(database_url).await;

    const NAMESPACE_NAME: &str = "ns_createtableexplicitwith";

    StepTest::new(
        &mut cluster,
        vec![
            // Explicitly create a namespace with a custom partition template
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    let addr = state.cluster().router().router_grpc_base().to_string();

                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-h")
                        .arg(&addr)
                        .arg("namespace")
                        .arg("create")
                        .arg(NAMESPACE_NAME)
                        .arg("--partition-template")
                        .arg("{\"parts\":[{\"timeFormat\":\"%Y-%m\"}, {\"tagValue\":\"state\"}]}")
                        .assert()
                        .success()
                        .stdout(predicate::str::contains(NAMESPACE_NAME));
                }
                .boxed()
            })),
            // Explicitly create a table *with* a custom partition template
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    let addr = state.cluster().router().router_grpc_base().to_string();
                    let table_name = "h2o_temperature";

                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-v")
                        .arg("-h")
                        .arg(&addr)
                        .arg("table")
                        .arg("create")
                        .arg(NAMESPACE_NAME)
                        .arg(table_name)
                        .arg("--partition-template")
                        .arg("{\"parts\":[{\"tagValue\":\"location\"}, {\"timeFormat\":\"%Y-%m\"}]}")
                        .assert()
                        .success()
                        .stdout(predicate::str::contains(table_name));
                }
                .boxed()
            })),
            // Write to the just-created table
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    let addr = state.cluster().router().router_http_base().to_string();

                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-v")
                        .arg("-h")
                        .arg(&addr)
                        .arg("write")
                        .arg(NAMESPACE_NAME)
                        .arg("../test_fixtures/lineproto/temperature.lp")
                        .assert()
                        .success()
                        .stdout(predicate::str::contains("591 Bytes OK"));
                }
                .boxed()
            })),
            // Read data
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    // data from 'air_and_water.lp'
                    wait_for_query_result_with_namespace(
                        NAMESPACE_NAME,
                        state,
                        "SELECT * from h2o_temperature order by time desc limit 10",
                        None,
                        "| 51.3           | coyote_creek | CA    | 55.1            | 1970-01-01T00:00:01.568756160Z |"
                    ).await;
                }
                .boxed()
            })),
            // Check partition keys that use the table's partition template
            Step::PartitionKeys{table_name: "h2o_temperature".to_string(), namespace_name: Some(NAMESPACE_NAME.to_string()), expected: vec!["coyote_creek|1970-01", "puget_sound|1970-01", "santa_monica|1970-01"]},
        ],
    )
    .run()
    .await
}
