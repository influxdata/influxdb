//! Tests CLI commands

use super::get_object_store_id;
use assert_cmd::Command;
use futures::FutureExt;
use predicates::prelude::*;
use std::time::{Duration, Instant};
use tempfile::tempdir;
use test_helpers_end_to_end::{
    maybe_skip_integration, AddAddrEnv, BindAddresses, MiniCluster, ServerType, Step, StepTest,
    StepTestState,
};

#[tokio::test]
async fn default_mode_is_run_all_in_one() {
    let tmpdir = tempdir().unwrap();
    let addrs = BindAddresses::default();

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .args(["-v"])
        // Do not attempt to connect to the real DB (object store, etc) if the
        // prod DSN is set - all other tests use TEST_INFLUXDB_IOX_CATALOG_DSN
        // but this one will use the real env if not cleared.
        .env_clear()
        // Without this, we have errors about writing read-only root filesystem on macos.
        .env("HOME", tmpdir.path())
        .add_addr_env(ServerType::AllInOne, &addrs)
        .timeout(Duration::from_secs(2))
        .assert()
        .failure()
        .stdout(predicate::str::contains("starting all in one server"));
}

#[tokio::test]
async fn default_run_mode_is_all_in_one() {
    let tmpdir = tempdir().unwrap();
    let addrs = BindAddresses::default();

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .args(["run", "-v"])
        // This test is designed to assert the default running mode is using
        // in-memory state, so ensure that any outside config does not influence
        // this.
        .env_clear()
        .env("HOME", tmpdir.path())
        .add_addr_env(ServerType::AllInOne, &addrs)
        .timeout(Duration::from_secs(2))
        .assert()
        .failure()
        .stdout(predicate::str::contains("starting all in one server"));
}

#[tokio::test]
async fn parquet_to_lp() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    // The test below assumes a specific partition id, so use a
    // non-shared one here so concurrent tests don't interfere with
    // each other
    let mut cluster = MiniCluster::create_non_shared_standard(database_url).await;

    let line_protocol = "my_awesome_table,tag1=A,tag2=B val=42i 123456";

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol(String::from(line_protocol)),
            // wait for partitions to be persisted
            Step::WaitForPersisted,
            // Run the 'remote partition' command
            Step::Custom(Box::new(move |state: &mut StepTestState| {
                async move {
                    let router_addr = state.cluster().router().router_grpc_base().to_string();

                    // Validate the output of the remote partition CLI command
                    //
                    // Looks like:
                    // {
                    //     "id": "1",
                    //     "shardId": 1,
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

                    // convert to line protocol (stdout)
                    let output = Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("debug")
                        .arg("parquet-to-lp")
                        .arg(filename)
                        .assert()
                        .success()
                        .stdout(predicate::str::contains(line_protocol))
                        .get_output()
                        .stdout
                        .clone();

                    println!("Got output  {:?}", output);

                    // test writing to output file as well
                    // Ensure files are actually wrote to the filesystem
                    let output_file =
                        tempfile::NamedTempFile::new().expect("Error making temp file");
                    println!("Writing to  {:?}", output_file);

                    // convert to line protocol (to a file)
                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("debug")
                        .arg("parquet-to-lp")
                        .arg(filename)
                        .arg("--output")
                        .arg(output_file.path())
                        .assert()
                        .success();

                    let file_contents =
                        std::fs::read(output_file.path()).expect("can not read data from tempfile");
                    let file_contents = String::from_utf8_lossy(&file_contents);
                    assert!(
                        predicate::str::contains(line_protocol).eval(&file_contents),
                        "Could not file {} in {}",
                        line_protocol,
                        file_contents
                    );
                }
                .boxed()
            })),
        ],
    )
    .run()
    .await
}

/// Write, compact, then use the remote partition command
#[tokio::test]
async fn compact_and_get_remote_partition() {
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
            // Run the compactor
            Step::Compact,
            // Run the 'remote partition' command
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    let router_addr = state.cluster().router().router_grpc_base().to_string();
                    let namespace = state.cluster().namespace().to_string();

                    // Validate the output of the remote partition CLI command
                    //
                    // Looks like:
                    // {
                    //     "id": "1",
                    //     "shardId": 1,
                    //     "namespaceId": 1,
                    //     "tableId": 1,
                    //     "partitionId": "1",
                    //     "objectStoreId": "fa6cdcd1-cbc2-4fb7-8b51-4773079124dd",
                    //     "minTime": "123456",
                    //     "maxTime": "123456",
                    //     "fileSizeBytes": "2029",
                    //     "rowCount": "1",
                    //     "compactionLevel": "2",
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
                                .and(predicate::str::contains(r#""shardId": "1","#))
                                .and(predicate::str::contains(r#""partitionId": "1","#))
                                .and(predicate::str::contains(r#""compactionLevel": 2"#)),
                        )
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

                    // Ensure a warning is emitted when specifying (or
                    // defaulting to) in-memory file storage.
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
                        .failure()
                        .stderr(predicate::str::contains("try passing --object-store=file"));

                    // Ensure files are actually wrote to the filesystem
                    let dir = tempfile::tempdir().expect("could not get temporary directory");

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
                        .arg("file")
                        .arg("--data-dir")
                        .arg(dir.path().to_str().unwrap())
                        .arg(&namespace)
                        .arg("my_awesome_table")
                        .arg("1970-01-01")
                        .assert()
                        .success()
                        .stdout(
                            predicate::str::contains("wrote file")
                                .and(predicate::str::contains(object_store_id)),
                        );
                }
                .boxed()
            })),
        ],
    )
    .run()
    .await
}

/// Test the schema cli command
#[tokio::test]
async fn schema_cli() {
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
                    // should be able to query both router and querier for the schema
                    let addrs = vec![
                        (
                            "router",
                            state.cluster().router().router_grpc_base().to_string(),
                        ),
                        (
                            "querier",
                            state.cluster().querier().querier_grpc_base().to_string(),
                        ),
                    ];

                    for (addr_type, addr) in addrs {
                        println!("Trying address {}: {}", addr_type, addr);

                        // Validate the output of the schema CLI command
                        Command::cargo_bin("influxdb_iox")
                            .unwrap()
                            .arg("-h")
                            .arg(&addr)
                            .arg("debug")
                            .arg("schema")
                            .arg("get")
                            .arg(state.cluster().namespace())
                            .assert()
                            .success()
                            .stdout(
                                predicate::str::contains("my_awesome_table2")
                                    .and(predicate::str::contains("tag1"))
                                    .and(predicate::str::contains("val")),
                            );
                    }
                }
                .boxed()
            })),
        ],
    )
    .run()
    .await
}

/// Test write CLI command and query CLI command
#[tokio::test]
async fn write_and_query() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    let mut cluster = MiniCluster::create_shared(database_url).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    let router_addr = state.cluster().router().router_http_base().to_string();

                    let namespace = state.cluster().namespace();
                    println!("Writing into {namespace}");

                    // Validate the output of the schema CLI command
                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-v")
                        .arg("-h")
                        .arg(&router_addr)
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
                        .stdout(predicate::str::contains("1137058 Bytes OK"));
                }
                .boxed()
            })),
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    // data from 'air_and_water.lp'
                    wait_for_query_result(
                        state,
                        "SELECT * from h2o_temperature order by time desc limit 10",
                        None,
                        "| 51.3           | coyote_creek | CA    | 55.1            | 1970-01-01T00:00:01.568756160Z |"
                    ).await;

                    // data from 'read_filter.lp.gz', specific query language type
                    wait_for_query_result(
                        state,
                        "SELECT * from m0 order by time desc limit 10;",
                        Some(QueryLanguage::Sql),
                        "| value1 | value9 | value9 | value49 | value0 | 2021-04-26T13:47:39.727574Z | 1  |"
                    ).await;

                    // data from 'cpu.parquet'
                    wait_for_query_result(
                        state,
                        "SELECT * from cpu where cpu = 'cpu2' order by time desc limit 10",
                        None,
                        "cpu2 | MacBook-Pro-8.hsd1.ma.comcast.net | 2022-09-30T12:55:00Z"
                    ).await;
                }
                .boxed()
            })),
        ],
    )
    .run()
    .await
}

/// Test error handling for the query CLI command
#[tokio::test]
async fn query_error_handling() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    let mut cluster = MiniCluster::create_shared(database_url).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol("this_table_does_exist,tag=A val=\"foo\" 1".into()),
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    let querier_addr = state.cluster().querier().querier_grpc_base().to_string();
                    let namespace = state.cluster().namespace();

                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-h")
                        .arg(&querier_addr)
                        .arg("query")
                        .arg(namespace)
                        .arg("drop table this_table_doesnt_exist")
                        .assert()
                        .failure()
                        .stderr(predicate::str::contains(
                            "Error while planning query: This feature is not \
                            implemented: DropTable",
                        ));
                }
                .boxed()
            })),
        ],
    )
    .run()
    .await
}

/// Test error handling for the query CLI command for InfluxQL queries
#[tokio::test]
async fn influxql_error_handling() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    let mut cluster = MiniCluster::create_shared(database_url).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol("this_table_does_exist,tag=A val=\"foo\" 1".into()),
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    let querier_addr = state.cluster().querier().querier_grpc_base().to_string();
                    let namespace = state.cluster().namespace();

                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-h")
                        .arg(&querier_addr)
                        .arg("query")
                        .arg("--lang")
                        .arg("influxql")
                        .arg(namespace)
                        .arg("CREATE DATABASE foo")
                        .assert()
                        .failure()
                        .stderr(predicate::str::contains(
                            "Error while planning query: This feature is not implemented: CREATE DATABASE",
                        ));
                }
                    .boxed()
            })),
        ],
    )
        .run()
        .await
}

#[allow(dead_code)]
#[derive(Clone, Copy)]
enum QueryLanguage {
    Sql,
    InfluxQL,
}

impl ToString for QueryLanguage {
    fn to_string(&self) -> String {
        match self {
            Self::Sql => "sql".to_string(),
            Self::InfluxQL => "influxql".to_string(),
        }
    }
}

trait AddQueryLanguage {
    /// Add the query language option to the receiver.
    fn add_query_lang(&mut self, query_lang: Option<QueryLanguage>) -> &mut Self;
}

impl AddQueryLanguage for assert_cmd::Command {
    fn add_query_lang(&mut self, query_lang: Option<QueryLanguage>) -> &mut Self {
        match query_lang {
            Some(lang) => self.arg("--lang").arg(lang.to_string()),
            None => self,
        }
    }
}

/// Runs the specified query in a loop for up to 10 seconds, waiting
/// for the specified output to appear
async fn wait_for_query_result(
    state: &mut StepTestState<'_>,
    query_sql: &str,
    query_lang: Option<QueryLanguage>,
    expected: &str,
) {
    let querier_addr = state.cluster().querier().querier_grpc_base().to_string();
    let namespace = state.cluster().namespace();

    let max_wait_time = Duration::from_secs(10);
    println!("Waiting for {expected}");

    // Validate the output of running the query CLI command appears after at most max_wait_time
    let end = Instant::now() + max_wait_time;
    while Instant::now() < end {
        let assert = Command::cargo_bin("influxdb_iox")
            .unwrap()
            .arg("-h")
            .arg(&querier_addr)
            .arg("query")
            .add_query_lang(query_lang)
            .arg(namespace)
            .arg(query_sql)
            .assert();

        let assert = match assert.try_success() {
            Err(e) => {
                println!("Got err running command: {}, retrying", e);
                continue;
            }
            Ok(a) => a,
        };

        match assert.try_stdout(predicate::str::contains(expected)) {
            Err(e) => {
                println!("No match: {}, retrying", e);
            }
            Ok(r) => {
                println!("Success: {:?}", r);
                return;
            }
        }
        // sleep and try again
        tokio::time::sleep(Duration::from_secs(1)).await
    }
    panic!(
        "Did not find expected output {} within {:?}",
        expected, max_wait_time
    );
}

/// Test the namespace cli command
#[tokio::test]
async fn namespaces_cli() {
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

/// Test the namespace retention command
#[tokio::test]
async fn namespace_retention() {
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

/// Test the query_ingester CLI command
#[tokio::test]
async fn query_ingester() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    let mut cluster = MiniCluster::create_shared(database_url).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol(String::from(
                "my_awesome_table2,tag1=A,tag2=B val=42i 123456",
            )),
            Step::WaitForReadable,
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    let ingester_addr = state.cluster().ingester().ingester_grpc_base().to_string();

                    let expected = [
                        "+------+------+--------------------------------+-----+",
                        "| tag1 | tag2 | time                           | val |",
                        "+------+------+--------------------------------+-----+",
                        "| A    | B    | 1970-01-01T00:00:00.000123456Z | 42  |",
                        "+------+------+--------------------------------+-----+",
                    ]
                    .join("\n");

                    // Validate the output of the query
                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-h")
                        .arg(&ingester_addr)
                        .arg("query-ingester")
                        .arg(state.cluster().namespace_id().await.get().to_string())
                        .arg(
                            state
                                .cluster()
                                .table_id("my_awesome_table2")
                                .await
                                .get()
                                .to_string(),
                        )
                        .assert()
                        .success()
                        .stdout(predicate::str::contains(&expected));
                }
                .boxed()
            })),
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    // now run the query-ingester command against the querier
                    let querier_addr = state.cluster().querier().querier_grpc_base().to_string();

                    // this is not a good error: it should be
                    // something like "wrong query protocol" or
                    // "invalid message" as the querier requires a
                    // different message format Ticket in the flight protocol
                    let expected = "Unknown namespace: ";

                    // Validate that the error message contains a reasonable error
                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-h")
                        .arg(&querier_addr)
                        .arg("query-ingester")
                        .arg(state.cluster().namespace_id().await.get().to_string())
                        .arg(
                            state
                                .cluster()
                                .table_id("my_awesome_table2")
                                .await
                                .get()
                                .to_string(),
                        )
                        .assert()
                        .failure()
                        .stderr(predicate::str::contains(expected));
                }
                .boxed()
            })),
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    let ingester_addr = state.cluster().ingester().ingester_grpc_base().to_string();

                    let expected = [
                        "+------+-----+",
                        "| tag1 | val |",
                        "+------+-----+",
                        "| A    | 42  |",
                        "+------+-----+",
                    ]
                    .join("\n");

                    // Validate the output of the query
                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-h")
                        .arg(&ingester_addr)
                        .arg("query-ingester")
                        .arg(state.cluster().namespace_id().await.get().to_string())
                        .arg(
                            state
                                .cluster()
                                .table_id("my_awesome_table2")
                                .await
                                .get()
                                .to_string(),
                        )
                        .arg("--columns")
                        .arg("tag1,val")
                        .assert()
                        .success()
                        .stdout(predicate::str::contains(&expected));
                }
                .boxed()
            })),
        ],
    )
    .run()
    .await
}
