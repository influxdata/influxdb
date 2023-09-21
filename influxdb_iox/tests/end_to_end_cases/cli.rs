//! Tests CLI commands

mod helpers;
pub use helpers::*;

mod namespace;
mod table;

use std::{fs::read_dir, sync::Arc, time::Duration};

use assert_cmd::Command;
use assert_matches::assert_matches;
use futures::FutureExt;
use predicates::prelude::*;
use tempfile::tempdir;
use test_helpers_end_to_end::{
    maybe_skip_integration, AddAddrEnv, BindAddresses, MiniCluster, ServerType, Step, StepTest,
    StepTestState, TestConfig,
};

use super::get_object_store_id;

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
        .timeout(Duration::from_secs(5))
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
        .timeout(Duration::from_secs(5))
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
    let mut cluster = MiniCluster::create_non_shared(database_url).await;

    let line_protocol = "my_awesome_table,tag1=A,tag2=B val=42i 123456";

    StepTest::new(
        &mut cluster,
        vec![
            Step::RecordNumParquetFiles,
            Step::WriteLineProtocol(String::from(line_protocol)),
            // wait for partitions to be persisted
            Step::WaitForPersisted {
                expected_increase: 1,
            },
            // Run the 'remote partition' command
            Step::Custom(Box::new(move |state: &mut StepTestState| {
                async move {
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

                    println!("Got output  {output:?}");

                    // test writing to output file as well
                    // Ensure files are actually wrote to the filesystem
                    let output_file =
                        tempfile::NamedTempFile::new().expect("Error making temp file");
                    println!("Writing to  {output_file:?}");

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
                        "Could not file {line_protocol} in {file_contents}"
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
            Step::RecordNumParquetFiles,
            Step::WriteLineProtocol(String::from(
                "my_awesome_table2,tag1=A,tag2=B val=42i 123456",
            )),
            Step::WaitForPersisted {
                expected_increase: 1,
            },
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
                        println!("Trying address {addr_type}: {addr}");

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
                        .stdout(predicate::str::contains("889317 Bytes OK"));
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
                        "| value1 | value9 | value9 | value49 | value0 | 2021-04-26T13:47:39.727574Z | 1.0 |"
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
                        .stderr(predicate::str::contains("DDL not supported: DropTable"));
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

/// Test the query_ingester CLI command
#[tokio::test]
async fn query_ingester() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    let mut cluster = MiniCluster::create_shared_never_persist(database_url).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol(String::from(
                "my_awesome_table2,tag1=A,tag2=B val=42i 123456",
            )),
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
                    let expected = "Database '' not found";

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

#[tokio::test]
async fn write_lp_from_wal() {
    static TEMPERATURE_RESULTS: &[&str] = &[
                            "+----------------+--------------+-------+-----------------+--------------------------------+",
                            "| bottom_degrees | location     | state | surface_degrees | time                           |",
                            "+----------------+--------------+-------+-----------------+--------------------------------+",
                            "| 50.4           | santa_monica | CA    | 65.2            | 1970-01-01T00:00:01.568756160Z |",
                            "| 49.2           | santa_monica | CA    | 63.6            | 1970-01-01T00:00:01.600756160Z |",
                            "| 51.3           | coyote_creek | CA    | 55.1            | 1970-01-01T00:00:01.568756160Z |",
                            "| 50.9           | coyote_creek | CA    | 50.2            | 1970-01-01T00:00:01.600756160Z |",
                            "| 40.2           | puget_sound  | WA    | 55.8            | 1970-01-01T00:00:01.568756160Z |",
                            "| 40.1           | puget_sound  | WA    | 54.7            | 1970-01-01T00:00:01.600756160Z |",
                            "+----------------+--------------+-------+-----------------+--------------------------------+",
                        ];

    use std::fs;

    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    let ingester_config = TestConfig::new_ingester_never_persist(&database_url);
    let router_config = TestConfig::new_router(&ingester_config);
    let wal_dir = Arc::new(std::path::PathBuf::from(
        ingester_config
            .wal_dir()
            .as_ref()
            .expect("missing WAL dir")
            .path(),
    ));

    let mut cluster = MiniCluster::new()
        .with_ingester(ingester_config)
        .await
        .with_router(router_config)
        .await;

    // Check that the query returns the same result between the original and
    // regenerated input.
    let table_name = "h2o_temperature";

    StepTest::new(
        &mut cluster,
        vec![
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    let router_addr = state.cluster().router().router_http_base().to_string();
                    let namespace = state.cluster().namespace();

                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-v")
                        .arg("-h")
                        .arg(&router_addr)
                        .arg("write")
                        .arg(namespace)
                        .arg("../test_fixtures/lineproto/temperature.lp")
                        .assert()
                        .success()
                        .stdout(predicate::str::contains("591 Bytes OK"));
                }
                .boxed()
            })),
            Step::Custom(Box::new(move |state: &mut StepTestState| {
                // Ensure the results are queryable in the ingester
                async {
                    assert_ingester_contains_results(
                        state.cluster(),
                        table_name,
                        TEMPERATURE_RESULTS,
                    )
                    .await;
                }
                .boxed()
            })),
            Step::Custom(Box::new(move |state: &mut StepTestState| {
                let wal_dir = Arc::clone(&wal_dir);
                async move {
                    let mut reader =
                        read_dir(wal_dir.as_path()).expect("failed to read WAL directory");
                    let segment_file_path = reader
                        .next()
                        .expect("no segment file found")
                        .unwrap()
                        .path();
                    assert_matches!(reader.next(), None);

                    let out_dir = test_helpers::tmp_dir()
                        .expect("failed to create temp directory for line proto output");

                    // Regenerate the line proto from the segment file
                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-vv")
                        .arg("-h")
                        .arg(state.cluster().router().router_grpc_base().to_string())
                        .arg("debug")
                        .arg("wal")
                        .arg("regenerate-lp")
                        .arg("-o")
                        .arg(
                            out_dir
                                .path()
                                .to_str()
                                .expect("should be able to get temp directory as string"),
                        )
                        .arg(
                            segment_file_path
                                .to_str()
                                .expect("should be able to get segment file path as string"),
                        )
                        .assert()
                        .success();

                    let mut reader =
                        read_dir(out_dir.path()).expect("failed to read output directory");

                    let regenerated_file_path = reader
                        .next()
                        .expect("no regenerated files found")
                        .unwrap()
                        .path();
                    // Make sure that only one file was regenerated.
                    assert_matches!(reader.next(), None);

                    // Remove the WAL segment file, ensure the WAL dir is empty
                    // and restart the services.
                    fs::remove_file(segment_file_path)
                        .expect("should be able to remove WAL segment file");
                    assert_matches!(
                        fs::read_dir(wal_dir.as_path())
                            .expect("failed to read WAL directory")
                            .next(),
                        None
                    );

                    state.cluster_mut().restart_ingesters().await;
                    state.cluster_mut().restart_router().await;

                    // Feed the regenerated LP back into the ingester and check
                    // that the expected results are returned.
                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-v")
                        .arg("-h")
                        .arg(state.cluster().router().router_http_base().to_string())
                        .arg("write")
                        .arg(state.cluster().namespace())
                        .arg(
                            regenerated_file_path
                                .to_str()
                                .expect("should be able to get valid path to regenerated file"),
                        )
                        .assert()
                        .success()
                        .stdout(predicate::str::contains("592 Bytes OK"));

                    assert_ingester_contains_results(
                        state.cluster(),
                        table_name,
                        TEMPERATURE_RESULTS,
                    )
                    .await;
                }
                .boxed()
            })),
        ],
    )
    .run()
    .await
}

#[tokio::test]
async fn inspect_entries_from_wal() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    let ingester_config = TestConfig::new_ingester_never_persist(&database_url);
    let router_config = TestConfig::new_router(&ingester_config);
    let wal_dir = Arc::new(std::path::PathBuf::from(
        ingester_config
            .wal_dir()
            .as_ref()
            .expect("missing WAL dir")
            .path(),
    ));

    let mut cluster = MiniCluster::new()
        .with_ingester(ingester_config)
        .await
        .with_router(router_config)
        .await;

    StepTest::new(
        &mut cluster,
        vec![
            // Perform 3 separate writes, then inspect the WAL and ensure that
            // they can all be accounted for in the output by the sequencing.
            Step::WriteLineProtocol("bananas,quality=fresh,taste=good val=42i 123456".to_string()),
            Step::WriteLineProtocol("arán,quality=fresh,taste=best val=42i 654321".to_string()),
            Step::WriteLineProtocol("arán,quality=stale,taste=crunchy val=42i 654456".to_string()),
            Step::Custom(Box::new(move |_| {
                let wal_dir = Arc::clone(&wal_dir);
                async move {
                    let mut reader =
                        read_dir(wal_dir.as_path()).expect("failed to read WAL directory");
                    let segment_file_path = reader
                        .next()
                        .expect("no segment file found")
                        .unwrap()
                        .path();
                    assert_matches!(reader.next(), None);

                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("debug")
                        .arg("wal")
                        .arg("inspect")
                        .arg(
                            segment_file_path
                                .to_str()
                                .expect("should be able to get segment file path as string"),
                        )
                        .assert()
                        .success()
                        .stdout(predicate::str::contains("SequencedWalOp").count(3));

                    // Re-inspect the log, but filter for WAL operations with
                    // sequence numbers in a range.
                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("debug")
                        .arg("wal")
                        .arg("inspect")
                        .arg("--sequence-number-range")
                        .arg("1-2")
                        .arg(
                            segment_file_path
                                .to_str()
                                .expect("should be able to get segment file path as string"),
                        )
                        .assert()
                        .success()
                        .stdout(predicate::str::contains("SequencedWalOp").count(2));
                }
                .boxed()
            })),
        ],
    )
    .run()
    .await
}
