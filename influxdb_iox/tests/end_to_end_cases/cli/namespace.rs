//! Test `influxdb_iox namespace` commands

use assert_cmd::Command;
use futures::FutureExt;
use predicates::prelude::*;
use test_helpers_end_to_end::{maybe_skip_integration, MiniCluster, Step, StepTest, StepTestState};

use super::{wait_for_query_result_with_namespace, NamespaceCmd};

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
                    NamespaceCmd::new("list", state.cluster().namespace()).run(state);
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
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    NamespaceCmd::new("retention", state.cluster().namespace())
                        .with_retention_hours(2)
                        .run(state);
                }
                .boxed()
            })),
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    NamespaceCmd::new("retention", state.cluster().namespace())
                        .with_retention_hours(0)
                        .run(state);
                }
                .boxed()
            })),
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    NamespaceCmd::new("create", "namespace_2")
                        .with_retention_hours(2)
                        .run(state);
                }
                .boxed()
            })),
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    NamespaceCmd::new("create", "namespace_3").run(state);
                }
                .boxed()
            })),
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    NamespaceCmd::new("create", "namespace_4")
                        .with_retention_hours(0)
                        .run(state);
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
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    NamespaceCmd::new("create", NAMESPACE_NAME)
                        .with_retention_hours(0)
                        .run(state);
                }
                .boxed()
            })),
            // delete the newly created namespace
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    NamespaceCmd::new("delete", NAMESPACE_NAME).run(state);
                }
                .boxed()
            })),
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    let mut list_cmd =
                        NamespaceCmd::new("list", state.cluster().namespace()).build_command(state);

                    list_cmd
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
                    NamespaceCmd::new("create", "ns1")
                        .with_max_tables(123)
                        .run(state);
                }
                .boxed()
            })),
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    // {
                    //   "id": <foo>,
                    //   "name": "ns2",
                    //   "serviceProtectionLimits": {
                    //     "maxTables": 500,
                    //     "maxColumnsPerTable": 321
                    //   }
                    // }
                    NamespaceCmd::new("create", "ns2")
                        .with_max_columns_per_table(321)
                        .run(state);
                }
                .boxed()
            })),
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    // {
                    //   "id": <foo>,
                    //   "name": "ns3",
                    //   "serviceProtectionLimits": {
                    //     "maxTables": 123,
                    //     "maxColumnsPerTable": 321
                    //   }
                    // }
                    NamespaceCmd::new("create", "ns3")
                        .with_max_tables(123)
                        .with_max_columns_per_table(321)
                        .run(state);
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
                    // {
                    //   "id": <foo>,
                    //   "name": "service_limiter_namespace",
                    //   "serviceProtectionLimits": {
                    //     "maxTables": 500,
                    //     "maxColumnsPerTable": 200
                    //   }
                    // }
                    NamespaceCmd::new("create", NAMESPACE_NAME).run(state);
                }
                .boxed()
            })),
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    // {
                    //   "id": <foo>,
                    //   "name": "service_limiter_namespace",
                    //   "serviceProtectionLimits": {
                    //     "maxTables": 1337,
                    //     "maxColumnsPerTable": 200
                    //   }
                    // }
                    NamespaceCmd::new("update-limit", NAMESPACE_NAME)
                        .with_max_tables(1337)
                        .run(state);
                }
                .boxed()
            })),
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    // {
                    //   "id": <foo>,
                    //   "name": "service_limiter_namespace",
                    //   "serviceProtectionLimits": {
                    //     "maxTables": 1337,
                    //     "maxColumnsPerTable": 42
                    //   }
                    // }
                    let mut update_cmd = NamespaceCmd::new("update-limit", NAMESPACE_NAME)
                        .with_max_columns_per_table(42)
                        .build_command(state);

                    // Customize assertion to check that `maxTables` is the value the previous step
                    // set it to without sending `--max-tables` again
                    update_cmd.assert().success().stdout(
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
        vec![Step::Custom(Box::new(|state: &mut StepTestState| {
            async {
                let create_cmd = NamespaceCmd::new("create", NAMESPACE_NAME);

                // No partition template value specified is an error
                create_cmd
                    .clone()
                    .build_command(state)
                    .arg("--partition-template")
                    .assert()
                    .failure()
                    .stderr(predicate::str::contains(
                        "error: a value is required for '--partition-template \
                                <PARTITION_TEMPLATE>' but none was supplied",
                    ));

                // Wrong spelling `prts` in JSON is an error
                create_cmd
                    .clone()
                    .with_partition_template(
                        "{\"prts\": [\
                            {\"tagValue\": \"location\"}, \
                            {\"tagValue\": \"state\"}, \
                            {\"timeFormat\": \"%Y-%m\"}\
                        ]}",
                    )
                    .build_command(state)
                    .assert()
                    .failure()
                    .stderr(predicate::str::contains(
                        "Client Error: Invalid partition template format : \
                            unknown field `prts`",
                    ));

                // Attempting to use `time` as a tag in the partition template is an error
                create_cmd
                    .clone()
                    .with_partition_template(
                        "{\"parts\": [\
                            {\"tagValue\": \"location\"}, \
                            {\"tagValue\": \"time\"}, \
                            {\"timeFormat\": \"%Y-%m\"}\
                        ]}",
                    )
                    .build_command(state)
                    .assert()
                    .failure()
                    .stderr(predicate::str::contains(
                        "Client error: Client specified an invalid argument: \
                            invalid tag value in partition template: time cannot be used",
                    ));

                // Attempting to use time format `%42` is an error
                create_cmd
                    .clone()
                    .with_partition_template(
                        "{\"parts\": [\
                            {\"tagValue\": \"location\"}, \
                            {\"timeFormat\": \"%42\"}\
                        ]}",
                    )
                    .build_command(state)
                    .assert()
                    .failure()
                    .stderr(predicate::str::contains(
                        "Client error: Client specified an invalid argument: \
                            invalid strftime format in partition template",
                    ));

                // Over 8 parts in a partition template is an error
                create_cmd
                    .clone()
                    .with_partition_template(
                        "{\"parts\": \
                            [{\"tagValue\": \"1\"},\
                            {\"tagValue\": \"2\"},\
                            {\"timeFormat\": \"%Y-%m\"},\
                            {\"tagValue\": \"4\"},\
                            {\"tagValue\": \"5\"},\
                            {\"tagValue\": \"6\"},\
                            {\"tagValue\": \"7\"},\
                            {\"tagValue\": \"8\"},\
                            {\"tagValue\": \"9\"}\
                        ]}",
                    )
                    .build_command(state)
                    .assert()
                    .failure()
                    .stderr(predicate::str::contains(
                        "Partition templates may have a maximum of 8 parts",
                    ));
            }
            .boxed()
        }))],
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
        vec![Step::Custom(Box::new(|state: &mut StepTestState| {
            async {
                // No partition template specified
                NamespaceCmd::new("create", "ns_partition_template_1").run(state);

                // Partition template with time format
                NamespaceCmd::new("create", "ns_partition_template_2")
                    .with_partition_template("{\"parts\":[{\"timeFormat\":\"%Y-%m\"}]}")
                    .run(state);

                // Partition template with tag value
                NamespaceCmd::new("create", "ns_partition_template_3")
                    .with_partition_template("{\"parts\":[{\"tagValue\":\"col1\"}]}")
                    .run(state);

                // Partition template with time format, tag value, and tag of unsual column name
                let namespace_name_4 = "ns_partition_template_4";
                NamespaceCmd::new("create", namespace_name_4)
                    .with_partition_template(
                        "{\"parts\":[\
                            {\"tagValue\":\"col1\"},\
                            {\"timeFormat\":\"%Y-%d\"},\
                            {\"tagValue\":\"yes,col name\"}\
                        ]}",
                    )
                    .run(state);

                // `update` isn't a valid command
                NamespaceCmd::new("update", namespace_name_4)
                    .with_partition_template("{\"parts\":[{\"tagValue\":\"col1\"}]}")
                    .build_command(state)
                    .assert()
                    .failure()
                    .stderr(predicate::str::contains(
                        "error: unrecognized subcommand 'update'",
                    ));
            }
            .boxed()
        }))],
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
                    NamespaceCmd::new("create", NAMESPACE_NAME)
                        .with_partition_template(
                            "{\"parts\":[\
                            {\"timeFormat\":\"%Y-%m\"},\
                            {\"tagValue\":\"location\"}\
                        ]}",
                        )
                        .run(state);
                }
                .boxed()
            })),
            // Write, which implicitly creates the table with the namespace's custom partition
            // template
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
                        "| 51.3           \
                         | coyote_creek \
                         | CA    \
                         | 55.1            \
                         | 1970-01-01T00:00:01.568756160Z |",
                    )
                    .await;
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
                    NamespaceCmd::new("create", NAMESPACE_NAME)
                        .with_partition_template(
                            "{\"parts\":[\
                            {\"timeFormat\":\"%Y-%m\"}, \
                            {\"tagValue\":\"state\"}\
                        ]}",
                        )
                        .run(state);
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
                        "| 51.3           \
                         | coyote_creek \
                         | CA    \
                         | 55.1            \
                         | 1970-01-01T00:00:01.568756160Z |",
                    )
                    .await;
                }
                .boxed()
            })),
            // Check partition keys that use the namespace's partition template
            Step::PartitionKeys {
                table_name: "h2o_temperature".to_string(),
                namespace_name: Some(NAMESPACE_NAME.to_string()),
                expected: vec!["1970-01|CA", "1970-01|WA"],
            },
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
                    NamespaceCmd::new("create", NAMESPACE_NAME)
                        .with_partition_template(
                            "{\"parts\":[\
                            {\"timeFormat\":\"%Y-%m\"}, \
                            {\"tagValue\":\"state\"}\
                        ]}",
                        )
                        .run(state);
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
                        .arg(
                            "{\"parts\":[\
                            {\"tagValue\":\"location\"}, \
                            {\"timeFormat\":\"%Y-%m\"}\
                        ]}",
                        )
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
                        "| 51.3           \
                         | coyote_creek \
                         | CA    \
                         | 55.1            \
                         | 1970-01-01T00:00:01.568756160Z |",
                    )
                    .await;
                }
                .boxed()
            })),
            // Check partition keys that use the table's partition template
            Step::PartitionKeys {
                table_name: "h2o_temperature".to_string(),
                namespace_name: Some(NAMESPACE_NAME.to_string()),
                expected: vec![
                    "coyote_creek|1970-01",
                    "puget_sound|1970-01",
                    "santa_monica|1970-01",
                ],
            },
        ],
    )
    .run()
    .await
}
