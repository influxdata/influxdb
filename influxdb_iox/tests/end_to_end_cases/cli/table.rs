//! Test `influxdb_iox table` commands

use assert_cmd::Command;
use futures::FutureExt;
use predicates::prelude::*;
use test_helpers_end_to_end::{maybe_skip_integration, MiniCluster, Step, StepTest, StepTestState};

use super::{wait_for_query_result_with_namespace, QueryLanguage};

#[tokio::test]
async fn create_negative() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    let mut cluster = MiniCluster::create_shared(database_url).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    // Need router grpc based address to create namespace
                    let router_grpc_addr = state.cluster().router().router_grpc_base().to_string();
                    let namespace = "ns_tablenegative";

                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-h")
                        .arg(&router_grpc_addr)
                        .arg("namespace")
                        .arg("create")
                        .arg(namespace)
                        .assert()
                        .success()
                        .stdout(predicate::str::contains(namespace));
                }
                .boxed()
            })),
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    // Need router grpc based address to create tables
                    let router_grpc_addr = state.cluster().router().router_grpc_base().to_string();
                    let namespace = "ns_tablenegative";

                    // no partition tempplate specified
                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-h")
                        .arg(&router_grpc_addr)
                        .arg("table")
                        .arg("create")
                        .arg(namespace)
                        .arg("h2o_temperature")
                        .arg("--partition-template")
                        .assert()
                        .failure()
                        .stderr(predicate::str::contains(
                            "error: a value is required for '--partition-template \
                            <PARTITION_TEMPLATE>' but none was supplied",
                        ));

                    // Wrong spelling `prts`
                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        // .arg("-v")
                        .arg("-h")
                        .arg(&router_grpc_addr)
                        .arg("table")
                        .arg("create")
                        .arg(namespace)
                        .arg("h2o_temperature")
                        .arg("--partition-template")
                        .arg(
                            "{\"prts\": [\
                            {\"tagValue\": \"location\"}, \
                            {\"tagValue\": \"state\"}, \
                            {\"timeFormat\": \"%Y-%m\"}\
                        ]}",
                        )
                        .assert()
                        .failure()
                        .stderr(predicate::str::contains(
                            "Client Error: Invalid partition template format : \
                            unknown field `prts`",
                        ));

                    // Time as tag
                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-h")
                        .arg(&router_grpc_addr)
                        .arg("table")
                        .arg("create")
                        .arg(namespace)
                        .arg("h2o_temperature")
                        .arg("--partition-template")
                        .arg(
                            "{\"parts\": [\
                            {\"tagValue\": \"location\"}, \
                            {\"tagValue\": \"time\"}, \
                            {\"timeFormat\": \"%Y-%m\"}\
                        ]}",
                        )
                        .assert()
                        .failure()
                        .stderr(predicate::str::contains(
                            "Client error: Client specified an invalid argument: \
                            invalid tag value in partition template: time cannot be used",
                        ));

                    // Time format is `%42` which is invalid
                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-h")
                        .arg(&router_grpc_addr)
                        .arg("table")
                        .arg("create")
                        .arg(namespace)
                        .arg("h2o_temperature")
                        .arg("--partition-template")
                        .arg(
                            "{\"parts\": [\
                        {\"tagValue\": \"location\"}, \
                        {\"timeFormat\": \"%42\"}\
                    ]}",
                        )
                        .assert()
                        .failure()
                        .stderr(predicate::str::contains(
                            "Client error: Client specified an invalid argument: \
                        invalid strftime format in partition template",
                        ));

                    // Over 8 parts
                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-h")
                        .arg(&router_grpc_addr)
                        .arg("table")
                        .arg("create")
                        .arg(namespace)
                        .arg("h2o_temperature")
                        .arg("--partition-template")
                        .arg(
                            "{\"parts\": [\
                            {\"tagValue\": \"1\"},\
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
                        .assert()
                        .failure()
                        .stderr(predicate::str::contains(
                            "Partition templates may have a maximum of 8 parts",
                        ));

                    // Update an existing table
                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-h")
                        .arg(&router_grpc_addr)
                        .arg("table")
                        .arg("update")
                        .arg(namespace)
                        .arg("h2o_temperature")
                        .arg("--partition-template")
                        .arg("{\"parts\":[{\"tagValue\":\"col1\"}]}")
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

#[tokio::test]
async fn create_positive_and_list() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    let mut cluster = MiniCluster::create_shared(database_url).await;
    const TEST_NAMESPACE: &str = "ns_tablepositive";
    const TEST_TABLES: &[&str] = &["t1", "t2", "t3", "t4"];

    StepTest::new(
        &mut cluster,
        vec![
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    // Need router grpc based addres to create namespace
                    let router_grpc_addr = state.cluster().router().router_grpc_base().to_string();

                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-h")
                        .arg(&router_grpc_addr)
                        .arg("namespace")
                        .arg("create")
                        .arg(TEST_NAMESPACE)
                        .assert()
                        .success()
                        .stdout(predicate::str::contains(TEST_NAMESPACE));
                }
                .boxed()
            })),
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    // Need router grpc based address to create tables
                    let router_grpc_addr = state.cluster().router().router_grpc_base().to_string();

                    // no partition template specified
                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-h")
                        .arg(&router_grpc_addr)
                        .arg("table")
                        .arg("create")
                        .arg(TEST_NAMESPACE)
                        .arg(TEST_TABLES[0])
                        .assert()
                        .success()
                        .stdout(
                            predicate::str::contains(TEST_TABLES[0])
                                .and(predicate::str::contains(r#""partitionTemplate":"#).not()),
                        );

                    // Partition template with time format
                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-h")
                        .arg(&router_grpc_addr)
                        .arg("table")
                        .arg("create")
                        .arg(TEST_NAMESPACE)
                        .arg(TEST_TABLES[1])
                        .arg("--partition-template")
                        .arg("{\"parts\":[{\"timeFormat\":\"%Y-%m\"}]}")
                        .assert()
                        .success()
                        .stdout(
                            predicate::str::contains(TEST_TABLES[1])
                                .and(predicate::str::contains(r#""partitionTemplate":"#)),
                        );

                    // Partition template with tag
                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-h")
                        .arg(&router_grpc_addr)
                        .arg("table")
                        .arg("create")
                        .arg(TEST_NAMESPACE)
                        .arg(TEST_TABLES[2])
                        .arg("--partition-template")
                        .arg("{\"parts\":[{\"tagValue\":\"col1\"}]}")
                        .assert()
                        .success()
                        .stdout(
                            predicate::str::contains(TEST_TABLES[2])
                                .and(predicate::str::contains(r#""partitionTemplate":"#)),
                        );

                    // Partition template with time format, tag value, and tag of unsual column name
                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-h")
                        .arg(&router_grpc_addr)
                        .arg("table")
                        .arg("create")
                        .arg(TEST_NAMESPACE)
                        .arg(TEST_TABLES[3])
                        .arg("--partition-template")
                        .arg(
                            "{\"parts\":[\
                            {\"tagValue\":\"col1\"},\
                            {\"timeFormat\":\"%Y-%d\"},\
                            {\"tagValue\":\"yes,col name\"}\
                        ]}",
                        )
                        .assert()
                        .success()
                        .stdout(
                            predicate::str::contains(TEST_TABLES[3])
                                .and(predicate::str::contains(r#""partitionTemplate":"#)),
                        );
                }
                .boxed()
            })),
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    let router_grpc_addr = state.cluster().router().router_grpc_base().to_string();

                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-h")
                        .arg(&router_grpc_addr)
                        .arg("table")
                        .arg("list")
                        .arg(TEST_NAMESPACE)
                        .assert()
                        .success()
                        .stdout(
                            predicate::str::contains(format!(r#""name": "{}""#, TEST_TABLES[0]))
                                .and(predicate::str::contains(format!(
                                    r#""name": "{}""#,
                                    TEST_TABLES[1]
                                )))
                                .and(predicate::str::contains(format!(
                                    r#""name": "{}""#,
                                    TEST_TABLES[2]
                                )))
                                .and(predicate::str::contains(format!(
                                    r#""name": "{}""#,
                                    TEST_TABLES[3]
                                )))
                                .and(predicate::str::contains(r#""partitionTemplate":"#)),
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
async fn create_write_and_query() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    let mut cluster = MiniCluster::create_shared(database_url).await;
    let namespace = "ns_createtables";

    StepTest::new(
        &mut cluster,
        vec![
            // Create name space
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    // Need router grpc based addres to create namespace
                    let router_grpc_addr = state.cluster().router().router_grpc_base().to_string();
                    let namespace = "ns_createtables";

                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-h")
                        .arg(&router_grpc_addr)
                        .arg("namespace")
                        .arg("create")
                        .arg(namespace)
                        .assert()
                        .success()
                        .stdout(predicate::str::contains(namespace));
                }
                .boxed()
            })),
            // Create tables
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    // Need router grpc based addres to create tables
                    let router_grpc_addr = state.cluster().router().router_grpc_base().to_string();
                    let namespace = "ns_createtables";

                    // create tables h2o_temperature, m0, cpu
                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-v")
                        .arg("-h")
                        .arg(&router_grpc_addr)
                        .arg("table")
                        .arg("create")
                        .arg(namespace)
                        .arg("h2o_temperature")
                        .arg("--partition-template")
                        .arg(
                            "{\"parts\": [\
                            {\"tagValue\": \"location\"},\
                            {\"tagValue\": \"state\"},\
                            {\"timeFormat\": \"%Y-%m\"}\
                        ]}",
                        )
                        .assert()
                        .success()
                        .stdout(predicate::str::contains("h2o_temperature"));

                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-v")
                        .arg("-h")
                        .arg(&router_grpc_addr)
                        .arg("table")
                        .arg("create")
                        .arg(namespace)
                        .arg("m0")
                        .arg("--partition-template")
                        .arg("{\"parts\": [{\"timeFormat\": \"%Y.%j\"}]}")
                        .assert()
                        .success()
                        .stdout(predicate::str::contains("m0"));

                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-v")
                        .arg("-h")
                        .arg(&router_grpc_addr)
                        .arg("table")
                        .arg("create")
                        .arg(namespace)
                        .arg("cpu")
                        .arg("--partition-template")
                        .arg(
                            "{\"parts\": [\
                            {\"timeFormat\": \"%Y-%m-%d\"}, \
                            {\"tagValue\": \"cpu\"}\
                        ]}",
                        )
                        .assert()
                        .success()
                        .stdout(predicate::str::contains("cpu"));
                }
                .boxed()
            })),
            // Load data
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    // write must use router http based address
                    let router_http_addr = state.cluster().router().router_http_base().to_string();
                    let namespace = "ns_createtables";

                    // Validate the output of the schema CLI command
                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-v")
                        .arg("-h")
                        .arg(&router_http_addr)
                        .arg("write")
                        .arg(namespace)
                        // raw line protocol ('h2o_temperature' measurement)
                        .arg("../test_fixtures/lineproto/air_and_water.lp")
                        // gzipped line protocol ('m0')
                        .arg("../test_fixtures/lineproto/read_filter.lp.gz")
                        // iox formatted parquet ('cpu' measurement)
                        .arg("../test_fixtures/cpu.parquet")
                        .assert()
                        .success()
                        // this number is the total size of
                        // uncompressed line protocol stored in all
                        // three files
                        .stdout(predicate::str::contains("889317 Bytes OK"));
                }
                .boxed()
            })),
            // Read back data
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    // data from 'air_and_water.lp'
                    wait_for_query_result_with_namespace(
                        namespace,
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

                    // data from 'read_filter.lp.gz', specific query language type
                    wait_for_query_result_with_namespace(
                        namespace,
                        state,
                        "SELECT * from m0 order by time desc limit 10;",
                        Some(QueryLanguage::Sql),
                        "| value1 \
                         | value9 \
                         | value9 \
                         | value49 \
                         | value0 \
                         | 2021-04-26T13:47:39.727574Z \
                         | 1.0 |",
                    )
                    .await;

                    // data from 'cpu.parquet'
                    wait_for_query_result_with_namespace(
                        namespace,
                        state,
                        "SELECT * from cpu where cpu = 'cpu2' order by time desc limit 10",
                        None,
                        "cpu2 | MacBook-Pro-8.hsd1.ma.comcast.net | 2022-09-30T12:55:00Z",
                    )
                    .await;
                }
                .boxed()
            })),
            // Check partition keys
            Step::PartitionKeys {
                table_name: "h2o_temperature".to_string(),
                namespace_name: Some(namespace.to_string()),
                expected: vec![
                    "coyote_creek|CA|1970-01",
                    "puget_sound|WA|1970-01",
                    "santa_monica|CA|1970-01",
                ],
            },
            Step::PartitionKeys {
                table_name: "m0".to_string(),
                namespace_name: Some(namespace.to_string()),
                expected: vec!["2021.116"],
            },
            Step::PartitionKeys {
                table_name: "cpu".to_string(),
                namespace_name: Some(namespace.to_string()),
                expected: vec!["2022-09-30|cpu2"],
            },
        ],
    )
    .run()
    .await
}
