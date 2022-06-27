pub(crate) mod influxrpc;
mod multi_ingester;

use assert_cmd::Command;
use futures::FutureExt;
use predicates::prelude::*;
use test_helpers_end_to_end::{
    maybe_skip_integration, try_run_query, MiniCluster, Step, StepTest, StepTestState, TestConfig,
};

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
        Step::WriteLineProtocol(setup.lp_to_force_persistence()),
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
async fn ingester_panic() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    let table_name = "the_table";

    // Set up the cluster  ====================================
    let router_config = TestConfig::new_router(&database_url);
    // can't use standard mini cluster here as we setup the querier to panic
    let ingester_config =
        TestConfig::new_ingester(&router_config).with_ingester_flight_do_get_panic(2);
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
                    // SQL query fails, error is propagated
                    let sql = format!("select * from {} where tag2='B'", table_name);
                    let err = try_run_query(
                        sql,
                        state.cluster().namespace(),
                        state.cluster().querier().querier_grpc_connection(),
                    )
                    .await
                    .unwrap_err();
                    if let influxdb_iox_client::flight::Error::GrpcError(status) = err {
                        assert_eq!(status.code(), tonic::Code::Internal);
                    } else {
                        panic!("wrong error type");
                    }

                    // find relevant log line for debugging
                    let querier_logs =
                        std::fs::read_to_string(state.cluster().querier().log_path().await)
                            .unwrap();
                    let log_line = querier_logs
                        .split('\n')
                        .find(|s| s.contains("Failed to perform ingester query"))
                        .unwrap();
                    let log_data: serde_json::Value = serde_json::from_str(log_line).unwrap();
                    let log_data = log_data.as_object().unwrap();
                    let log_data = log_data["fields"].as_object().unwrap();

                    // query ingester using debug information
                    for i in 0..2 {
                        let assert = Command::cargo_bin("influxdb_iox")
                            .unwrap()
                            .arg("-h")
                            .arg(log_data["ingester_address"].as_str().unwrap())
                            .arg("query-ingester")
                            .arg(log_data["namespace"].as_str().unwrap())
                            .arg(log_data["table"].as_str().unwrap())
                            .arg("--columns")
                            .arg(log_data["columns"].as_str().unwrap())
                            .arg("--predicate-base64")
                            .arg(log_data["predicate_binary"].as_str().unwrap())
                            .assert();

                        // The ingester is configured to fail 2 times, once for the original query and once during
                        // debugging. The 2nd debug query should work and should only return data for `tag2=B` (not for
                        // `tag2=C`).
                        if i == 0 {
                            assert.failure().stderr(predicate::str::contains(
                                "Error querying: status: Internal, message: \"Panicking in `do_get` for testing purposes.\"",
                            ));
                        } else {
                            assert.success().stdout(
                                predicate::str::contains(
                                    "| A    | B    | 1970-01-01T00:00:00.000123456Z | 42  |",
                                )
                                .and(predicate::str::contains("C").not()),
                            );
                        }
                    }
                }
                .boxed()
            })),
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

/// This structure holds information for tests that need to force a parquet file to be persisted
struct ForcePersistenceSetup {
    // Set up a cluster that will will persist quickly
    cluster: MiniCluster,
}

impl ForcePersistenceSetup {
    async fn new(database_url: String) -> Self {
        // Set up the cluster  ====================================
        let router_config = TestConfig::new_router(&database_url);
        let ingester_config =
            TestConfig::new_ingester(&router_config).with_ingester_persist_memory_threshold(1000);
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
