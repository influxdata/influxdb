pub(crate) mod influxrpc;
mod multi_ingester;

use std::time::Duration;

use arrow::datatypes::{DataType, SchemaRef};
use arrow_flight::{
    decode::{DecodedFlightData, DecodedPayload},
    error::FlightError,
};
use arrow_util::assert_batches_sorted_eq;
use assert_cmd::{assert::Assert, Command};
use futures::{FutureExt, StreamExt, TryStreamExt};
use generated_types::{
    aggregate::AggregateType, read_group_request::Group, read_response::frame::Data,
};
use influxdb_iox_client::flight::IOxRecordBatchStream;
use predicates::prelude::*;
use test_helpers::assert_contains;
use test_helpers_end_to_end::{
    check_flight_error, check_tonic_status, maybe_skip_integration, run_sql, try_run_sql,
    GrpcRequestBuilder, MiniCluster, Step, StepTest, StepTestState, TestConfig,
};

/// Temporary duplication: These tests should be kept in sync (as far as what they're logically
/// testing, not the exact implementation) with the corresponding tests in the
/// `kafkaless_rpc_write` module.
///
/// When we switch to the RPC write path, this module can be deleted.
mod with_kafka {
    use super::*;

    #[tokio::test]
    async fn basic_ingester() {
        test_helpers::maybe_start_logging();
        let database_url = maybe_skip_integration!();

        let table_name = "the_table";

        // Set up the cluster  ====================================
        let mut cluster = MiniCluster::create_shared(database_url).await;

        StepTest::new(
            &mut cluster,
            vec![
                Step::WriteLineProtocol(format!(
                    "{},tag1=A,tag2=B val=42i 123456\n\
                     {},tag1=A,tag2=C val=43i 123457",
                    table_name, table_name
                )),
                Step::WaitForReadable,
                Step::AssertNotPersisted,
                Step::Query {
                    sql: format!("select * from {}", table_name),
                    expected: vec![
                        "+------+------+--------------------------------+-----+",
                        "| tag1 | tag2 | time                           | val |",
                        "+------+------+--------------------------------+-----+",
                        "| A    | B    | 1970-01-01T00:00:00.000123456Z | 42  |",
                        "| A    | C    | 1970-01-01T00:00:00.000123457Z | 43  |",
                        "+------+------+--------------------------------+-----+",
                    ],
                },
            ],
        )
        .run()
        .await
    }

    #[tokio::test]
    async fn basic_empty() {
        test_helpers::maybe_start_logging();
        let database_url = maybe_skip_integration!();

        let table_name = "the_table";

        // Set up the cluster  ====================================
        let mut cluster = MiniCluster::create_shared(database_url).await;

        StepTest::new(
            &mut cluster,
            vec![
                Step::WriteLineProtocol(format!(
                    "{},tag1=A,tag2=B val=42i 123456\n\
                     {},tag1=A,tag2=C val=43i 123457",
                    table_name, table_name
                )),
                Step::WaitForReadable,
                Step::Custom(Box::new(move |state: &mut StepTestState| {
                    async move {
                        // query returns no results
                        let sql = format!("select * from {} where time > '2023-01-12'", table_name);
                        let querier_connection =
                            state.cluster().querier().querier_grpc_connection();
                        let namespace = state.cluster().namespace();

                        let mut client =
                            influxdb_iox_client::flight::Client::new(querier_connection);

                        let result_stream = client.sql(namespace, &sql).await.unwrap();

                        let mut flight_stream = result_stream.into_inner();

                        // no data is returned
                        assert!(flight_stream.next().await.is_none());

                        // even though there are no results, we should have still got the schema
                        // otherwise other clients may complain
                        // https://github.com/influxdata/influxdb_iox/pull/6668
                        assert!(flight_stream.got_schema());

                        // run the query again and ensure there are no dictionaries
                        let result_stream = client.sql(namespace, sql).await.unwrap();
                        verify_schema(result_stream).await;

                        // run a query that does return results and ensure there are no dictionaries
                        let sql = format!("select * from {table_name}");
                        let result_stream = client.sql(namespace, sql).await.unwrap();
                        verify_schema(result_stream).await;
                    }
                    .boxed()
                })),
            ],
        )
        .run()
        .await
    }

    #[tokio::test]
    async fn basic_on_parquet() {
        test_helpers::maybe_start_logging();
        let database_url = maybe_skip_integration!();

        let table_name = "the_table";

        // Set up the cluster  ====================================
        let mut cluster = MiniCluster::create_shared(database_url).await;

        StepTest::new(
            &mut cluster,
            vec![
                Step::WriteLineProtocol(format!("{},tag1=A,tag2=B val=42i 123456", table_name)),
                // Wait for data to be persisted to parquet
                Step::WaitForPersisted,
                Step::Query {
                    sql: format!("select * from {}", table_name),
                    expected: vec![
                        "+------+------+--------------------------------+-----+",
                        "| tag1 | tag2 | time                           | val |",
                        "+------+------+--------------------------------+-----+",
                        "| A    | B    | 1970-01-01T00:00:00.000123456Z | 42  |",
                        "+------+------+--------------------------------+-----+",
                    ],
                },
            ],
        )
        .run()
        .await
    }

    #[tokio::test]
    async fn basic_no_ingester_connection() {
        test_helpers::maybe_start_logging();
        let database_url = maybe_skip_integration!();

        let table_name = "the_table";

        let router_config = TestConfig::new_router(&database_url);
        // fast parquet
        let ingester_config = TestConfig::new_ingester(&router_config);

        // specially create a querier config that is NOT connected to the ingester
        let querier_config = TestConfig::new_querier_without_ingester(&ingester_config);

        // Set up the cluster  ====================================
        let mut cluster = MiniCluster::new()
            .with_router(router_config)
            .await
            .with_ingester(ingester_config)
            .await
            .with_querier(querier_config)
            .await;

        // Write some data into the v2 HTTP API ==============
        StepTest::new(
            &mut cluster,
            vec![
                Step::WriteLineProtocol(format!("{},tag1=A,tag2=B val=42i 123456", table_name)),
                // Wait for data to be persisted to parquet, ask the ingester directly because the
                // querier is not connected to the ingester
                Step::WaitForPersistedAccordingToIngester,
                Step::Query {
                    sql: format!("select * from {}", table_name),
                    expected: vec![
                        "+------+------+--------------------------------+-----+",
                        "| tag1 | tag2 | time                           | val |",
                        "+------+------+--------------------------------+-----+",
                        "| A    | B    | 1970-01-01T00:00:00.000123456Z | 42  |",
                        "+------+------+--------------------------------+-----+",
                    ],
                },
            ],
        )
        .run()
        .await
    }

    #[tokio::test]
    async fn query_after_persist_sees_new_files() {
        // https://github.com/influxdata/influxdb_iox/issues/4634 added
        // caching of tombstones and parquet files in the querier. This
        // test ensures that a query issued after new parquet files are
        // persisted correctly picks up the new parquet files
        test_helpers::maybe_start_logging();

        let database_url = maybe_skip_integration!();
        let mut setup = ForcePersistenceSetup::new(database_url).await;

        let steps = vec![
            // Write data to a parquet file
            Step::WriteLineProtocol(setup.lp_to_force_persistence()),
            Step::WaitForPersisted,
            Step::Query {
                sql: setup.count_star_sql(),
                expected: vec![
                    "+-----------------+",
                    "| COUNT(UInt8(1)) |",
                    "+-----------------+",
                    "| 1               |",
                    "+-----------------+",
                ],
            },
            // second query, should be the same result
            Step::Query {
                sql: setup.count_star_sql(),
                expected: vec![
                    "+-----------------+",
                    "| COUNT(UInt8(1)) |",
                    "+-----------------+",
                    "| 1               |",
                    "+-----------------+",
                ],
            },
            // write another parquet file
            // that has non duplicated data
            Step::WriteLineProtocol(setup.lp_to_force_persistence().replace("tag=A", "tag=B")),
            Step::WaitForPersisted,
            // query should correctly see the data in the second parquet file
            Step::Query {
                sql: setup.count_star_sql(),
                expected: vec![
                    "+-----------------+",
                    "| COUNT(UInt8(1)) |",
                    "+-----------------+",
                    "| 2               |",
                    "+-----------------+",
                ],
            },
        ];

        StepTest::new(&mut setup.cluster, steps).run().await
    }

    #[tokio::test]
    async fn table_not_found_on_ingester() {
        test_helpers::maybe_start_logging();
        let database_url = maybe_skip_integration!();

        let table_name = "the_table";

        // Set up the cluster  ====================================
        // cannot use shared cluster because we're restarting the ingester
        let mut cluster = MiniCluster::create_non_shared_standard(database_url).await;

        StepTest::new(
            &mut cluster,
            vec![
                Step::WriteLineProtocol(format!("{},tag1=A,tag2=B val=42i 123456", table_name)),
                Step::WaitForPersisted,
                Step::WriteLineProtocol(String::from("other_table,tag1=A,tag2=B val=42i 123456")),
                Step::WaitForPersisted,
                // Restart the ingester so that it does not have any table data in memory
                // and so will return "not found" to the querier
                Step::Custom(Box::new(|state: &mut StepTestState| {
                    state.cluster_mut().restart_ingester().boxed()
                })),
                Step::Query {
                    sql: format!("select * from {}", table_name),
                    expected: vec![
                        "+------+------+--------------------------------+-----+",
                        "| tag1 | tag2 | time                           | val |",
                        "+------+------+--------------------------------+-----+",
                        "| A    | B    | 1970-01-01T00:00:00.000123456Z | 42  |",
                        "+------+------+--------------------------------+-----+",
                    ],
                },
            ],
        )
        .run()
        .await
    }

    #[tokio::test]
    async fn ingester_panic_1() {
        test_helpers::maybe_start_logging();
        let database_url = maybe_skip_integration!();

        let table_name = "the_table";

        // Set up the cluster  ====================================
        let router_config = TestConfig::new_router(&database_url);
        // can't use standard mini cluster here as we setup the querier to panic
        let ingester_config = TestConfig::new_ingester(&router_config)
            .with_ingester_flight_do_get_panic(2)
            .with_ingester_persist_memory_threshold(1_000_000);
        let querier_config = TestConfig::new_querier(&ingester_config).with_json_logs();
        let mut cluster = MiniCluster::new()
            .with_router(router_config)
            .await
            .with_ingester(ingester_config)
            .await
            .with_querier(querier_config)
            .await;

        StepTest::new(
            &mut cluster,
            vec![
                Step::WriteLineProtocol(format!(
                    "{},tag1=A,tag2=B val=42i 123456\n\
                     {},tag1=A,tag2=C val=43i 123457",
                    table_name, table_name
                )),
                Step::WaitForReadable,
                Step::AssertNotPersisted,
                Step::Custom(Box::new(move |state: &mut StepTestState| {
                    async move {
                        // Ingester panics but querier will retry.
                        let sql = format!("select * from {} where tag2='B'", table_name);
                        let batches = run_sql(
                            sql,
                            state.cluster().namespace(),
                            state.cluster().querier().querier_grpc_connection(),
                        )
                        .await;
                        let expected = [
                            "+------+------+--------------------------------+-----+",
                            "| tag1 | tag2 | time                           | val |",
                            "+------+------+--------------------------------+-----+",
                            "| A    | B    | 1970-01-01T00:00:00.000123456Z | 42  |",
                            "+------+------+--------------------------------+-----+",
                        ];
                        assert_batches_sorted_eq!(&expected, &batches);

                        // verify that the ingester panicked
                        let ingester_logs =
                            std::fs::read_to_string(state.cluster().ingester().log_path().await)
                                .unwrap();
                        assert_contains!(
                            ingester_logs,
                            "thread 'tokio-runtime-worker' panicked at 'Panicking in `do_get` for testing purposes.'"
                        );

                         // The debug query should work.
                         let assert = repeat_ingester_request_based_on_querier_logs(state).await;
                         assert.success().stdout(
                             predicate::str::contains(
                                 "| A    | B    | 1970-01-01T00:00:00.000123456Z | 42  |",
                             )
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
    async fn ingester_panic_2() {
        test_helpers::maybe_start_logging();
        let database_url = maybe_skip_integration!();

        let table_name = "the_table";

        // Set up the cluster  ====================================
        let router_config = TestConfig::new_router(&database_url);
        // can't use standard mini cluster here as we setup the querier to panic
        let ingester_config = TestConfig::new_ingester(&router_config)
            .with_ingester_flight_do_get_panic(10)
            .with_ingester_persist_memory_threshold(2_000);
        let querier_config = TestConfig::new_querier(&ingester_config)
            .with_querier_ingester_circuit_breaker_threshold(2)
            .with_json_logs();
        let mut cluster = MiniCluster::new()
            .with_router(router_config)
            .await
            .with_ingester(ingester_config)
            .await
            .with_querier(querier_config)
            .await;

        StepTest::new(
            &mut cluster,
            vec![
                Step::WriteLineProtocol(format!(
                    "{},tag=A val=1i 1\n\
                     {},tag=B val=2i,trigger=\"{}\" 2",
                    table_name,
                    table_name,
                    "x".repeat(10_000),
                )),
                Step::WaitForPersisted,
                Step::WriteLineProtocol(format!("{},tag=A val=3i 3", table_name)),
                Step::WaitForReadable,
                Step::AssertLastNotPersisted,
                // circuit breaker will prevent ingester from being queried, so we only get the persisted data
                Step::Query {
                    sql: format!("select tag,val,time from {} where tag='A'", table_name),
                    expected: vec![
                        "+-----+-----+--------------------------------+",
                        "| tag | val | time                           |",
                        "+-----+-----+--------------------------------+",
                        "| A   | 1   | 1970-01-01T00:00:00.000000001Z |",
                        "+-----+-----+--------------------------------+",
                    ],
                },
                Step::Custom(Box::new(move |state: &mut StepTestState| {
                    async move {
                        // "de-panic" the ingester
                        for trail in 1.. {
                            let assert = repeat_ingester_request_based_on_querier_logs(state).await;
                            if assert.try_success().is_ok() {
                                break;
                            }
                            assert!(trail < 20);
                        }

                        // wait for circuit breaker to close circuits again
                        tokio::time::timeout(Duration::from_secs(10), async {
                            loop {
                                let sql = format!(
                                    "select tag,val,time from {} where tag='A'",
                                    table_name
                                );
                                let batches = run_sql(
                                    sql,
                                    state.cluster().namespace(),
                                    state.cluster().querier().querier_grpc_connection(),
                                )
                                .await;

                                let n_lines = batches.iter().map(|b| b.num_rows()).sum::<usize>();
                                if n_lines > 1 {
                                    let expected = [
                                        "+-----+-----+--------------------------------+",
                                        "| tag | val | time                           |",
                                        "+-----+-----+--------------------------------+",
                                        "| A   | 1   | 1970-01-01T00:00:00.000000001Z |",
                                        "| A   | 3   | 1970-01-01T00:00:00.000000003Z |",
                                        "+-----+-----+--------------------------------+",
                                    ];
                                    assert_batches_sorted_eq!(&expected, &batches);

                                    break;
                                }
                            }
                        })
                        .await
                        .unwrap();
                    }
                    .boxed()
                })),
                Step::AssertLastNotPersisted,
            ],
        )
        .run()
        .await
    }

    #[tokio::test]
    async fn issue_4631_a() {
        // See https://github.com/influxdata/influxdb_iox/issues/4631
        //
        // The symptom was that on rare occasion the querier would panic because the query engine was sure there must be a
        // partition sort key but the querier did not provide any. For this to happen we need overlapping chunks and all
        // these chunks must be sorted. This is only the case if all chunks are persisted (i.e. parquet-backed, so no
        // ingester data). The reason why the querier did NOT provide a partition sort key was because it once got queried
        // when the partition was fresh and only existed within the ingester, so no partition sort key was calculated yet.
        // During that initial query the querier would cache the partition information (incl. the absence of a partition
        // sort key) and when queried again (this time all chunks are peristed but overlapping) it would use this stale
        // information, confusing the query engine.
        test_helpers::maybe_start_logging();

        let database_url = maybe_skip_integration!();
        let mut setup = ForcePersistenceSetup::new(database_url).await;

        let steps = vec![
            // create UNPERSISTED ingester data
            //
            // IMPORTANT: The data MUST NOT be persisted before the first query is executed, because persistence
            //            calculates the partition sort key. The original bug was that the first query on a completely
            //            unpersisted partition would cache the NULL/None sort key which would later lead to a panic.
            Step::WriteLineProtocol(format!("{},tag=A val=\"foo\" 1", setup.table_name())),
            Step::WaitForReadable,
            Step::AssertNotPersisted,
            // cache partition in querier w/o any partition sort key (yet)
            // This MUST happen after we have some ingester data but before ANYTHING was persisted. In the original bug
            // the querier would now cache the partition w/o a sort key (and would never invalidate this information).
            Step::Query {
                sql: format!("select * from {}", setup.table_name()),
                expected: vec![
                    "+-----+--------------------------------+-----+",
                    "| tag | time                           | val |",
                    "+-----+--------------------------------+-----+",
                    "| A   | 1970-01-01T00:00:00.000000001Z | foo |",
                    "+-----+--------------------------------+-----+",
                ],
            },
            // flush ingester data
            // Here the ingester calculates the partition sort key.
            Step::WriteLineProtocol(format!(
                "{},tag=B val=\"{}\" 2\n",
                setup.table_name(),
                setup.super_long_string()
            )),
            Step::WaitForPersisted,
            // create overlapping 2nd parquet file
            // This is important to trigger the bug within the query engine, because only if there are multiple chunks
            // that need to be de-duplicated the bug will occur.
            Step::WriteLineProtocol(format!(
                "{},tag=A val=\"bar\" 1\n{},tag=B val=\"{}\" 2\n",
                setup.table_name(),
                setup.table_name(),
                setup.super_long_string()
            )),
            Step::WaitForPersisted,
            // query
            // In the original bug the querier would still use NULL/None as a partition sort key but present two sorted
            // but overlapping chunks to the query engine.
            Step::Query {
                sql: format!("select * from {} where tag='A'", setup.table_name()),
                expected: vec![
                    "+-----+--------------------------------+-----+",
                    "| tag | time                           | val |",
                    "+-----+--------------------------------+-----+",
                    "| A   | 1970-01-01T00:00:00.000000001Z | bar |",
                    "+-----+--------------------------------+-----+",
                ],
            },
        ];

        StepTest::new(&mut setup.cluster, steps).run().await
    }

    #[tokio::test]
    async fn issue_4631_b() {
        // This is similar to `issue_4631_a` but instead of updating the sort key from NULL/None to something we update it
        // with a new tag.
        test_helpers::maybe_start_logging();

        let database_url = maybe_skip_integration!();

        let table_name = "the_table";

        // Set up the cluster  ====================================
        let mut cluster = MiniCluster::create_shared(database_url).await;

        StepTest::new(
            &mut cluster,
            vec![
                // create persisted chunk with a single tag column
                Step::WriteLineProtocol(format!("{},tag=A val=\"foo\" 1", table_name)),
                Step::WaitForPersisted,
                // query to prime the querier caches with partition sort key
                Step::Query {
                    sql: format!("select * from {}", table_name),
                    expected: vec![
                        "+-----+--------------------------------+-----+",
                        "| tag | time                           | val |",
                        "+-----+--------------------------------+-----+",
                        "| A   | 1970-01-01T00:00:00.000000001Z | foo |",
                        "+-----+--------------------------------+-----+",
                    ],
                },
                // create 2nd chunk with an additional tag column (which will be included in the partition sort key)
                Step::WriteLineProtocol(format!("{},tag=A,tag2=B val=\"bar\" 1\n", table_name)),
                Step::WaitForPersisted,
                // in the original bug the querier would now panic with:
                //
                //     Partition sort key tag, time, does not cover or is sorted on the same order of the chunk sort key tag, tag2, time,
                //
                // Note that we cannot query tag2 because the schema is cached for a while.
                Step::Query {
                    sql: format!(
                        "select tag, val from {} where tag='A' order by val",
                        table_name
                    ),
                    expected: vec![
                        "+-----+-----+",
                        "| tag | val |",
                        "+-----+-----+",
                        "| A   | bar |",
                        "| A   | foo |",
                        "+-----+-----+",
                    ],
                },
            ],
        )
        .run()
        .await
    }

    #[tokio::test]
    async fn unsupported_sql_returns_error() {
        test_helpers::maybe_start_logging();
        let database_url = maybe_skip_integration!();

        // Set up the cluster  ====================================
        let mut cluster = MiniCluster::create_shared(database_url).await;

        StepTest::new(
            &mut cluster,
            vec![
                Step::WriteLineProtocol("this_table_does_exist,tag=A val=\"foo\" 1".into()),
                Step::QueryExpectingError {
                    sql: "drop table this_table_doesnt_exist".into(),
                    expected_error_code: tonic::Code::InvalidArgument,
                    expected_message:
                        "Error while planning query: This feature is not implemented: \
                        DropTable"
                            .into(),
                },
                Step::QueryExpectingError {
                    sql: "create view some_view as select * from this_table_does_exist".into(),
                    expected_error_code: tonic::Code::InvalidArgument,
                    expected_message:
                        "Error while planning query: This feature is not implemented: \
                        CreateView"
                            .into(),
                },
                Step::QueryExpectingError {
                    sql: "create database my_new_database".into(),
                    expected_error_code: tonic::Code::InvalidArgument,
                    expected_message:
                        "Error while planning query: This feature is not implemented: \
                        CreateCatalog"
                            .into(),
                },
                Step::QueryExpectingError {
                    sql: "create schema foo".into(),
                    expected_error_code: tonic::Code::InvalidArgument,
                    expected_message:
                        "Error while planning query: This feature is not implemented: \
                                        CreateCatalogSchema"
                            .into(),
                },
            ],
        )
        .run()
        .await
    }

    #[tokio::test]
    async fn table_or_namespace_not_found() {
        test_helpers::maybe_start_logging();
        let database_url = maybe_skip_integration!();

        // Set up the cluster  ====================================
        let mut cluster = MiniCluster::create_shared(database_url).await;

        StepTest::new(
            &mut cluster,
            vec![
                Step::WriteLineProtocol("this_table_does_exist,tag=A val=\"foo\" 1".into()),
                // SQL: table
                // Result: InvalidArgument
                Step::QueryExpectingError {
                    sql: "select * from not_a_table;".into(),
                    expected_error_code: tonic::Code::InvalidArgument,
                    expected_message: "Error while planning query: Error during planning: table 'public.iox.not_a_table' not found"
                        .into(),
                },
                // SQL: namespace
                // Result: NotFound
                Step::Custom(Box::new(move |state: &mut StepTestState| {
                    async move {
                        let err = try_run_sql(
                            "select * from this_table_does_exist;",
                            format!("{}_suffix", state.cluster().namespace()),
                            state.cluster().querier().querier_grpc_connection(),
                        )
                        .await
                        .unwrap_err();
                        check_flight_error(err, tonic::Code::NotFound, None);
                    }
                    .boxed()
                })),
                // InfluxRPC: table
                // Result: empty stream
                Step::Custom(Box::new(move |state: &mut StepTestState| {
                    async move {
                        let mut storage_client = state.cluster().querier_storage_client();

                        let read_filter_request = GrpcRequestBuilder::new()
                            .source(state.cluster())
                            .measurement_predicate("this_table_does_not_exist")
                            .build_read_filter();

                        let read_response = storage_client
                            .read_filter(read_filter_request)
                            .await
                            .unwrap();
                        let responses: Vec<_> = read_response.into_inner().try_collect().await.unwrap();
                        let frames: Vec<Data> = responses
                            .into_iter()
                            .flat_map(|r| r.frames)
                            .flat_map(|f| f.data)
                            .collect();
                        assert_eq!(frames, vec![]);
                    }
                    .boxed()
                })),
                // InfluxRPC: namespace
                // Result: NotFound
                Step::Custom(Box::new(move |state: &mut StepTestState| {
                    async move {
                        let mut storage_client = state.cluster().querier_storage_client();

                        let read_filter_request = GrpcRequestBuilder::new()
                            .explicit_source("1111111111111111", "1111111111111111")
                            .build_read_filter();

                        let status = storage_client
                            .read_filter(read_filter_request)
                            .await
                            .unwrap_err();
                        check_tonic_status(&status, tonic::Code::NotFound, None);
                    }
                    .boxed()
                })),
            ],
        )
        .run()
        .await
    }

    #[tokio::test]
    async fn oom_protection() {
        test_helpers::maybe_start_logging();
        let database_url = maybe_skip_integration!();

        let table_name = "the_table";

        // Set up the cluster  ====================================
        let router_config = TestConfig::new_router(&database_url);
        let ingester_config = TestConfig::new_ingester(&router_config);
        let querier_config =
            TestConfig::new_querier(&ingester_config).with_querier_mem_pool_bytes(1);
        let mut cluster = MiniCluster::new()
            .with_router(router_config)
            .await
            .with_ingester(ingester_config)
            .await
            .with_querier(querier_config)
            .await;

        StepTest::new(
            &mut cluster,
            vec![
                Step::WriteLineProtocol(format!("{},tag1=A,tag2=B val=42i 123457", table_name)),
                Step::WaitForReadable,
                // SQL query
                Step::Custom(Box::new(move |state: &mut StepTestState| {
                    async move {
                        let sql = format!(
                            "select tag1, sum(val) as val from {} group by tag1",
                            table_name
                        );
                        let err = try_run_sql(
                            &sql,
                            state.cluster().namespace(),
                            state.cluster().querier().querier_grpc_connection(),
                        )
                        .await
                        .unwrap_err();
                        check_flight_error(err, tonic::Code::ResourceExhausted, None);

                        // EXPLAIN should work though
                        run_sql(
                            format!("EXPLAIN {sql}"),
                            state.cluster().namespace(),
                            state.cluster().querier().querier_grpc_connection(),
                        )
                        .await;
                    }
                    .boxed()
                })),
                // InfluxRPC/storage query
                Step::Custom(Box::new(move |state: &mut StepTestState| {
                    async move {
                        let mut storage_client = state.cluster().querier_storage_client();

                        let read_group_request = GrpcRequestBuilder::new()
                            .source(state.cluster())
                            .aggregate_type(AggregateType::Sum)
                            .group(Group::By)
                            .group_keys(["tag1"])
                            .build_read_group();

                        let status = storage_client
                            .read_group(read_group_request)
                            .await
                            .unwrap_err();
                        check_tonic_status(&status, tonic::Code::ResourceExhausted, None);
                    }
                    .boxed()
                })),
            ],
        )
        .run()
        .await
    }

    /// This structure holds information for tests that need to force a parquet file to be persisted
    struct ForcePersistenceSetup {
        // Set up a cluster that will will persist quickly
        cluster: MiniCluster,
    }

    impl ForcePersistenceSetup {
        async fn new(database_url: String) -> Self {
            // Set up the cluster  ====================================
            let router_config = TestConfig::new_router(&database_url);
            let ingester_config = TestConfig::new_ingester(&router_config)
                .with_ingester_persist_memory_threshold(1000);
            let querier_config = TestConfig::new_querier(&ingester_config);
            let cluster = MiniCluster::new()
                .with_router(router_config)
                .await
                .with_ingester(ingester_config)
                .await
                .with_querier(querier_config)
                .await;

            Self { cluster }
        }

        pub fn table_name(&self) -> &str {
            "the_table"
        }

        /// return `SELECT COUNT(*) FROM table_name` query
        pub fn count_star_sql(&self) -> String {
            format!("select count(*) from {}", self.table_name())
        }

        /// Return line protocol that is so large it will be persisted
        pub fn lp_to_force_persistence(&self) -> String {
            format!(
                "{},tag=A val=\"{}\" 2\n",
                self.table_name(),
                self.super_long_string()
            )
        }

        /// We need a trigger for persistence that is not time so the test
        /// is as stable as possible. We use a long string to cross the
        /// persistence memory threshold.
        pub fn super_long_string(&self) -> String {
            "x".repeat(10_000)
        }
    }

    async fn repeat_ingester_request_based_on_querier_logs(state: &StepTestState<'_>) -> Assert {
        let querier_logs =
            std::fs::read_to_string(state.cluster().querier().log_path().await).unwrap();
        let log_line = querier_logs
            .split('\n')
            .find(|s| s.contains("Failed to perform ingester query"))
            .unwrap();
        let log_data: serde_json::Value = serde_json::from_str(log_line).unwrap();
        let log_data = log_data.as_object().unwrap();
        let log_data = log_data["fields"].as_object().unwrap();

        // query ingester using debug information
        Command::cargo_bin("influxdb_iox")
            .unwrap()
            .arg("-h")
            .arg(log_data["ingester_address"].as_str().unwrap())
            .arg("query-ingester")
            .arg(log_data["namespace_id"].as_u64().unwrap().to_string())
            .arg(log_data["table_id"].as_u64().unwrap().to_string())
            .arg("--columns")
            .arg(log_data["columns"].as_str().unwrap())
            .arg("--predicate-base64")
            .arg(log_data["predicate_binary"].as_str().unwrap())
            .assert()
    }
}

/// Temporary duplication: These tests should be kept in sync (as far as what they're logically
/// testing, not the exact implementation) with the corresponding tests in the
/// `with_kafka` module.
///
/// When we switch to the RPC write path, the code in this module can be unwrapped into its super
/// scope and unindented.
mod kafkaless_rpc_write {
    use super::*;

    #[tokio::test]
    async fn basic_ingester() {
        test_helpers::maybe_start_logging();
        let database_url = maybe_skip_integration!();

        let table_name = "the_table";

        // Set up the cluster  ====================================
        let mut cluster = MiniCluster::create_shared2_never_persist(database_url).await;

        StepTest::new(
            &mut cluster,
            vec![
                Step::WriteLineProtocol(format!(
                    "{},tag1=A,tag2=B val=42i 123456\n\
                     {},tag1=A,tag2=C val=43i 123457",
                    table_name, table_name
                )),
                Step::Query {
                    sql: format!("select * from {}", table_name),
                    expected: vec![
                        "+------+------+--------------------------------+-----+",
                        "| tag1 | tag2 | time                           | val |",
                        "+------+------+--------------------------------+-----+",
                        "| A    | B    | 1970-01-01T00:00:00.000123456Z | 42  |",
                        "| A    | C    | 1970-01-01T00:00:00.000123457Z | 43  |",
                        "+------+------+--------------------------------+-----+",
                    ],
                },
            ],
        )
        .run()
        .await
    }

    #[tokio::test]
    #[should_panic(expected = "did not get additional Parquet files in the catalog")]
    async fn never_persist_really_never_persists() {
        test_helpers::maybe_start_logging();
        // Tell the test to panic with the expected message if `TEST_INTEGRATION` isn't set so that
        // this still passes
        let database_url =
            maybe_skip_integration!("did not get additional Parquet files in the catalog");

        let table_name = "the_table";

        // Set up the cluster  ====================================
        let mut cluster = MiniCluster::create_shared2_never_persist(database_url).await;

        StepTest::new(
            &mut cluster,
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(format!(
                    "{},tag1=A,tag2=B val=42i 123456\n\
                     {},tag1=A,tag2=C val=43i 123457",
                    table_name, table_name
                )),
                // This should_panic if the ingester setup is correct
                Step::WaitForPersisted2 {
                    expected_increase: 1,
                },
            ],
        )
        .run()
        .await
    }

    #[tokio::test]
    async fn basic_on_parquet() {
        test_helpers::maybe_start_logging();
        let database_url = maybe_skip_integration!();

        let table_name = "the_table";

        // Set up the cluster  ====================================
        let mut cluster = MiniCluster::create_shared2(database_url).await;

        StepTest::new(
            &mut cluster,
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(format!("{},tag1=A,tag2=B val=42i 123456", table_name)),
                // Wait for data to be persisted to parquet
                Step::WaitForPersisted2 {
                    expected_increase: 1,
                },
                Step::Query {
                    sql: format!("select * from {}", table_name),
                    expected: vec![
                        "+------+------+--------------------------------+-----+",
                        "| tag1 | tag2 | time                           | val |",
                        "+------+------+--------------------------------+-----+",
                        "| A    | B    | 1970-01-01T00:00:00.000123456Z | 42  |",
                        "+------+------+--------------------------------+-----+",
                    ],
                },
            ],
        )
        .run()
        .await
    }

    #[tokio::test]
    async fn basic_empty() {
        test_helpers::maybe_start_logging();
        let database_url = maybe_skip_integration!();

        let table_name = "the_table";

        // Set up the cluster  ====================================
        let ingester_config = TestConfig::new_ingester2(&database_url);
        let router_config = TestConfig::new_router2(&ingester_config);
        // specially create a querier2 config that is NOT connected to the ingester2
        let querier_config = TestConfig::new_querier2_without_ingester2(&ingester_config);

        let mut cluster = MiniCluster::new()
            .with_ingester(ingester_config)
            .await
            .with_router(router_config)
            .await
            .with_querier(querier_config)
            .await;

        StepTest::new(
            &mut cluster,
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(format!(
                    "{},tag1=A,tag2=B val=42i 123456\n\
                     {},tag1=A,tag2=C val=43i 123457",
                    table_name, table_name
                )),
                // Wait for data to be persisted to parquet
                Step::WaitForPersisted2 {
                    expected_increase: 1,
                },
                Step::Custom(Box::new(move |state: &mut StepTestState| {
                    async move {
                        // query returns no results
                        let sql = format!("select * from {} where time > '2023-01-12'", table_name);
                        let querier_connection =
                            state.cluster().querier().querier_grpc_connection();
                        let namespace = state.cluster().namespace();

                        let mut client =
                            influxdb_iox_client::flight::Client::new(querier_connection);

                        let result_stream = client.sql(namespace, &sql).await.unwrap();

                        let mut flight_stream = result_stream.into_inner();

                        // no data is returned
                        assert!(flight_stream.next().await.is_none());

                        // even though there are no results, we should have still got the schema
                        // otherwise other clients may complain
                        // https://github.com/influxdata/influxdb_iox/pull/6668
                        assert!(flight_stream.got_schema());

                        // run the query again and ensure there are no dictionaries
                        let result_stream = client.sql(namespace, sql).await.unwrap();
                        verify_schema(result_stream).await;

                        // run a query that does return results and ensure there are no dictionaries
                        let sql = format!("select * from {table_name}");
                        let result_stream = client.sql(namespace, sql).await.unwrap();
                        verify_schema(result_stream).await;
                    }
                    .boxed()
                })),
            ],
        )
        .run()
        .await
    }

    #[tokio::test]
    async fn basic_no_ingester_connection() {
        test_helpers::maybe_start_logging();
        let database_url = maybe_skip_integration!();

        let table_name = "the_table";

        // Set up the cluster  ====================================
        let ingester_config = TestConfig::new_ingester2(&database_url);
        let router_config = TestConfig::new_router2(&ingester_config);
        // specially create a querier2 config that is NOT connected to the ingester2
        let querier_config = TestConfig::new_querier2_without_ingester2(&ingester_config);

        let mut cluster = MiniCluster::new()
            .with_ingester(ingester_config)
            .await
            .with_router(router_config)
            .await
            .with_querier(querier_config)
            .await;

        // Write some data into the v2 HTTP API ==============
        StepTest::new(
            &mut cluster,
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(format!("{},tag1=A,tag2=B val=42i 123456", table_name)),
                Step::WaitForPersisted2 {
                    expected_increase: 1,
                },
                Step::Query {
                    sql: format!("select * from {}", table_name),
                    expected: vec![
                        "+------+------+--------------------------------+-----+",
                        "| tag1 | tag2 | time                           | val |",
                        "+------+------+--------------------------------+-----+",
                        "| A    | B    | 1970-01-01T00:00:00.000123456Z | 42  |",
                        "+------+------+--------------------------------+-----+",
                    ],
                },
            ],
        )
        .run()
        .await
    }

    #[tokio::test]
    async fn query_after_persist_sees_new_files() {
        // https://github.com/influxdata/influxdb_iox/issues/4634 added
        // caching of tombstones and parquet files in the querier. This
        // test ensures that a query issued after new parquet files are
        // persisted correctly picks up the new parquet files
        test_helpers::maybe_start_logging();
        let database_url = maybe_skip_integration!();

        let table_name = "the_table";

        // Set up the cluster  ====================================
        let mut cluster = MiniCluster::create_shared2(database_url).await;

        let steps = vec![
            Step::RecordNumParquetFiles,
            Step::WriteLineProtocol(format!("{},tag1=A,tag2=B val=42i 123456", table_name)),
            // Wait for data to be persisted to parquet
            Step::WaitForPersisted2 {
                expected_increase: 1,
            },
            Step::Query {
                sql: format!("select * from {}", table_name),
                expected: vec![
                    "+------+------+--------------------------------+-----+",
                    "| tag1 | tag2 | time                           | val |",
                    "+------+------+--------------------------------+-----+",
                    "| A    | B    | 1970-01-01T00:00:00.000123456Z | 42  |",
                    "+------+------+--------------------------------+-----+",
                ],
            },
            // second query, should be the same result
            Step::Query {
                sql: format!("select * from {}", table_name),
                expected: vec![
                    "+------+------+--------------------------------+-----+",
                    "| tag1 | tag2 | time                           | val |",
                    "+------+------+--------------------------------+-----+",
                    "| A    | B    | 1970-01-01T00:00:00.000123456Z | 42  |",
                    "+------+------+--------------------------------+-----+",
                ],
            },
            Step::RecordNumParquetFiles,
            // write another parquet file that has non duplicated data
            Step::WriteLineProtocol(format!("{},tag1=B,tag2=A val=43i 789101112", table_name)),
            // Wait for data to be persisted to parquet
            Step::WaitForPersisted2 {
                expected_increase: 1,
            },
            // query should correctly see the data in the second parquet file
            Step::Query {
                sql: format!("select * from {}", table_name),
                expected: vec![
                    "+------+------+--------------------------------+-----+",
                    "| tag1 | tag2 | time                           | val |",
                    "+------+------+--------------------------------+-----+",
                    "| A    | B    | 1970-01-01T00:00:00.000123456Z | 42  |",
                    "| B    | A    | 1970-01-01T00:00:00.789101112Z | 43  |",
                    "+------+------+--------------------------------+-----+",
                ],
            },
        ];

        StepTest::new(&mut cluster, steps).run().await
    }
}

/// Some clients, such as the golang ones, can not decode
/// dictinary encoded Flight data.  This function asserts that all
/// schemas received in the stream are unpacked
pub(crate) async fn verify_schema(stream: IOxRecordBatchStream) {
    let flight_stream = stream.into_inner().into_inner();

    let decoded_data: Result<Vec<DecodedFlightData>, FlightError> =
        flight_stream.try_collect().await;

    // no errors
    let decoded_data = decoded_data.unwrap();

    // the schema should not have any dictionary encoded batches in it
    // as go clients can't deal with this
    for DecodedFlightData { inner: _, payload } in decoded_data {
        match payload {
            DecodedPayload::None => {}
            DecodedPayload::Schema(s) => assert_no_dictionaries(s),
            DecodedPayload::RecordBatch(b) => assert_no_dictionaries(b.schema()),
        }
    }
}

fn assert_no_dictionaries(schema: SchemaRef) {
    for field in schema.fields() {
        let dt = field.data_type();
        assert!(
            !matches!(dt, DataType::Dictionary(_, _)),
            "Found unexpected dictionary in schema: {schema:#?}"
        );
    }
}
