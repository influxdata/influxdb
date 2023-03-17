use std::path::PathBuf;

use arrow::record_batch::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_util::test_util::batches_to_sorted_lines;
use assert_cmd::Command;
use datafusion::common::assert_contains;
use futures::{FutureExt, TryStreamExt};
use influxdb_iox_client::flightsql::FlightSqlClient;
use predicates::prelude::*;
use test_helpers_end_to_end::{maybe_skip_integration, MiniCluster, Step, StepTest, StepTestState};

#[tokio::test]
async fn flightsql_adhoc_query() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    let table_name = "the_table";

    // Set up the cluster  ====================================
    let mut cluster = MiniCluster::create_shared2(database_url).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol(format!(
                "{table_name},tag1=A,tag2=B val=42i 123456\n\
                 {table_name},tag1=A,tag2=C val=43i 123457"
            )),
            Step::Custom(Box::new(move |state: &mut StepTestState| {
                async move {
                    let sql = format!("select * from {table_name}");
                    let mut client = flightsql_client(state.cluster());

                    let stream = client.query(sql).await.unwrap();
                    let batches = collect_stream(stream).await;
                    insta::assert_yaml_snapshot!(
                        batches_to_sorted_lines(&batches),
                        @r###"
                    ---
                    - +------+------+--------------------------------+-----+
                    - "| tag1 | tag2 | time                           | val |"
                    - +------+------+--------------------------------+-----+
                    - "| A    | B    | 1970-01-01T00:00:00.000123456Z | 42  |"
                    - "| A    | C    | 1970-01-01T00:00:00.000123457Z | 43  |"
                    - +------+------+--------------------------------+-----+
                    "###
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
async fn flightsql_adhoc_query_error() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    // Set up the cluster  ====================================
    let mut cluster = MiniCluster::create_shared2(database_url).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol(
                "foo,tag1=A,tag2=B val=42i 123456\n\
                 foo,tag1=A,tag2=C val=43i 123457"
                    .to_string(),
            ),
            Step::Custom(Box::new(move |state: &mut StepTestState| {
                async move {
                    let sql = String::from("select * from incorrect_table");

                    let mut client = flightsql_client(state.cluster());

                    let err = client.query(sql).await.unwrap_err();

                    // namespaces are created on write
                    assert_contains!(
                        err.to_string(),
                        "table 'public.iox.incorrect_table' not found"
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
async fn flightsql_prepared_query() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    let table_name = "the_table";

    // Set up the cluster  ====================================
    let mut cluster = MiniCluster::create_shared2(database_url).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol(format!(
                "{table_name},tag1=A,tag2=B val=42i 123456\n\
                 {table_name},tag1=A,tag2=C val=43i 123457"
            )),
            Step::Custom(Box::new(move |state: &mut StepTestState| {
                async move {
                    let sql = format!("select * from {table_name}");
                    let mut client = flightsql_client(state.cluster());

                    let handle = client.prepare(sql).await.unwrap();
                    let stream = client.execute(handle).await.unwrap();

                    let batches = collect_stream(stream).await;
                    insta::assert_yaml_snapshot!(
                        batches_to_sorted_lines(&batches),
                        @r###"
                    ---
                    - +------+------+--------------------------------+-----+
                    - "| tag1 | tag2 | time                           | val |"
                    - +------+------+--------------------------------+-----+
                    - "| A    | B    | 1970-01-01T00:00:00.000123456Z | 42  |"
                    - "| A    | C    | 1970-01-01T00:00:00.000123457Z | 43  |"
                    - +------+------+--------------------------------+-----+
                    "###
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
async fn flightsql_get_catalogs() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    let table_name = "the_table";

    // Set up the cluster  ====================================
    let mut cluster = MiniCluster::create_shared2(database_url).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol(format!(
                "{table_name},tag1=A,tag2=B val=42i 123456\n\
                 {table_name},tag1=A,tag2=C val=43i 123457"
            )),
            Step::Custom(Box::new(move |state: &mut StepTestState| {
                async move {
                    let mut client = flightsql_client(state.cluster());

                    let stream = client.get_catalogs().await.unwrap();
                    let batches = collect_stream(stream).await;

                    insta::assert_yaml_snapshot!(
                        batches_to_sorted_lines(&batches),
                        @r###"
                    ---
                    - +--------------+
                    - "| catalog_name |"
                    - +--------------+
                    - "| public       |"
                    - +--------------+
                    "###
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
async fn flightsql_get_table_types() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    let table_name = "the_table";

    // Set up the cluster  ====================================
    let mut cluster = MiniCluster::create_shared2(database_url).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol(format!(
                "{table_name},tag1=A,tag2=B val=42i 123456\n\
                 {table_name},tag1=A,tag2=C val=43i 123457"
            )),
            Step::Custom(Box::new(move |state: &mut StepTestState| {
                async move {
                    let mut client = flightsql_client(state.cluster());

                    let stream = client.get_table_types().await.unwrap();
                    let batches = collect_stream(stream).await;

                    insta::assert_yaml_snapshot!(
                        batches_to_sorted_lines(&batches),
                        @r###"
                    ---
                    - +------------+
                    - "| table_type |"
                    - +------------+
                    - "| BASE TABLE |"
                    - "| VIEW       |"
                    - +------------+
                    "###
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
async fn flightsql_get_db_schemas() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    let table_name = "the_table";

    // Set up the cluster  ====================================
    let mut cluster = MiniCluster::create_shared2(database_url).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol(format!(
                "{table_name},tag1=A,tag2=B val=42i 123456\n\
                 {table_name},tag1=A,tag2=C val=43i 123457"
            )),
            Step::Custom(Box::new(move |state: &mut StepTestState| {
                async move {
                    struct TestCase {
                        catalog: Option<&'static str>,
                        db_schema_filter_pattern: Option<&'static str>,
                    }
                    let cases = [
                        TestCase {
                            catalog: None,
                            db_schema_filter_pattern: None,
                        },
                        TestCase {
                            // pub <> public
                            catalog: Some("pub"),
                            db_schema_filter_pattern: None,
                        },
                        TestCase {
                            // pub% should match all
                            catalog: Some("pub%"),
                            db_schema_filter_pattern: None,
                        },
                        TestCase {
                            catalog: None,
                            db_schema_filter_pattern: Some("%for%"),
                        },
                        TestCase {
                            catalog: Some("public"),
                            db_schema_filter_pattern: Some("iox"),
                        },
                    ];

                    let mut client = flightsql_client(state.cluster());

                    let mut output = vec![];
                    for case in cases {
                        let TestCase {
                            catalog,
                            db_schema_filter_pattern,
                        } = case;
                        output.push(format!("catalog:{catalog:?}"));
                        output.push(format!(
                            "db_schema_filter_pattern:{db_schema_filter_pattern:?}"
                        ));
                        output.push("*********************".into());

                        let stream = client
                            .get_db_schemas(catalog, db_schema_filter_pattern)
                            .await
                            .unwrap();
                        let batches = collect_stream(stream).await;
                        output.extend(batches_to_sorted_lines(&batches))
                    }
                    insta::assert_yaml_snapshot!(
                        output,
                        @r###"
                    ---
                    - "catalog:None"
                    - "db_schema_filter_pattern:None"
                    - "*********************"
                    - +--------------+--------------------+
                    - "| catalog_name | db_schema_name     |"
                    - +--------------+--------------------+
                    - "| public       | information_schema |"
                    - "| public       | iox                |"
                    - "| public       | system             |"
                    - +--------------+--------------------+
                    - "catalog:Some(\"pub\")"
                    - "db_schema_filter_pattern:None"
                    - "*********************"
                    - ++
                    - ++
                    - "catalog:Some(\"pub%\")"
                    - "db_schema_filter_pattern:None"
                    - "*********************"
                    - +--------------+--------------------+
                    - "| catalog_name | db_schema_name     |"
                    - +--------------+--------------------+
                    - "| public       | information_schema |"
                    - "| public       | iox                |"
                    - "| public       | system             |"
                    - +--------------+--------------------+
                    - "catalog:None"
                    - "db_schema_filter_pattern:Some(\"%for%\")"
                    - "*********************"
                    - +--------------+--------------------+
                    - "| catalog_name | db_schema_name     |"
                    - +--------------+--------------------+
                    - "| public       | information_schema |"
                    - +--------------+--------------------+
                    - "catalog:Some(\"public\")"
                    - "db_schema_filter_pattern:Some(\"iox\")"
                    - "*********************"
                    - +--------------+----------------+
                    - "| catalog_name | db_schema_name |"
                    - +--------------+----------------+
                    - "| public       | iox            |"
                    - +--------------+----------------+
                    "###
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
/// Runs  the `jdbc_client` program against IOx to verify JDBC via FlightSQL is working
///
/// Example command:
///
/// ```shell
/// TEST_INFLUXDB_JDBC=true TEST_INFLUXDB_IOX_CATALOG_DSN=postgresql://postgres@localhost:5432/postgres cargo test --test end_to_end  jdbc
/// ```
async fn flightsql_jdbc() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    if std::env::var("TEST_INFLUXDB_JDBC").ok().is_none() {
        println!("Skipping JDBC test because TEST_INFLUXDB_JDBC is not set");
        return;
    }

    let table_name = "the_table";

    // Set up the cluster  ====================================
    let mut cluster = MiniCluster::create_shared2(database_url).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol(format!(
                "{table_name},tag1=A,tag2=B val=42i 123456\n\
                 {table_name},tag1=A,tag2=C val=43i 123457"
            )),
            Step::Custom(Box::new(move |state: &mut StepTestState| {
                // satisfy the borrow checker
                async move {
                    let namespace = state.cluster().namespace();

                    // querier_addr looks like: http://127.0.0.1:8092
                    let querier_addr = state.cluster().querier().querier_grpc_base().to_string();
                    println!("Querier {querier_addr}, namespace {namespace}");

                    // JDBC URL looks like this:
                    // jdbc:arrow-flight-sql://localhost:8082?useEncryption=false&iox-namespace-name=26f7e5a4b7be365b_917b97a92e883afc
                    let jdbc_addr = querier_addr.replace("http://", "jdbc:arrow-flight-sql://");
                    let jdbc_url =
                        format!("{jdbc_addr}?useEncryption=false&iox-namespace-name={namespace}");
                    println!("jdbc_url {jdbc_url}");

                    // find the jdbc_client to run
                    let path = PathBuf::from(std::env::var("PWD").expect("can not get PWD"))
                        .join("influxdb_iox/tests/jdbc_client/jdbc_client");
                    println!("Path to jdbc client: {path:?}");

                    // Validate basic query: jdbc_client <url> query 'sql'
                    Command::from_std(std::process::Command::new(&path))
                        .arg(&jdbc_url)
                        .arg("query")
                        .arg(format!("select * from {table_name} order by time"))
                        .arg(&querier_addr)
                        .assert()
                        .success()
                        .stdout(predicate::str::contains("Running SQL Query"))
                        .stdout(predicate::str::contains(
                            "A,  B,  1970-01-01 00:00:00.000123456,  42",
                        ))
                        .stdout(predicate::str::contains(
                            "A,  C,  1970-01-01 00:00:00.000123457,  43",
                        ));

                    // Validate prepared query: jdbc_client <url> prepared_query 'sql'
                    Command::from_std(std::process::Command::new(&path))
                        .arg(&jdbc_url)
                        .arg("prepared_query")
                        .arg(format!("select tag1, tag2 from {table_name} order by time"))
                        .arg(&querier_addr)
                        .assert()
                        .success()
                        .stdout(predicate::str::contains("Running Prepared SQL Query"))
                        .stdout(predicate::str::contains("A,  B"));

                    // CommandGetCatalogs output
                    let expected_catalogs = "**************\n\
                                             Catalogs:\n\
                                             **************\n\
                                             TABLE_CAT\n\
                                             ------------\n\
                                             public";

                    // CommandGetSchemas output
                    let expected_schemas = "**************\n\
                                            Schemas:\n\
                                            **************\n\
                                            TABLE_SCHEM,  TABLE_CATALOG\n\
                                            ------------\n\
                                            information_schema,  public\n\
                                            iox,  public\n\
                                            system,  public";

                    // CommandGetTableTypes output
                    let expected_table_types = "**************\n\
                                                Table Types:\n\
                                                **************\n\
                                                TABLE_TYPE\n\
                                                ------------\n\
                                                BASE TABLE\n\
                                                VIEW";

                    // Validate metadata: jdbc_client <url> metadata
                    Command::from_std(std::process::Command::new(&path))
                        .arg(&jdbc_url)
                        .arg("metadata")
                        .assert()
                        .success()
                        .stdout(predicate::str::contains(expected_catalogs))
                        .stdout(predicate::str::contains(expected_schemas))
                        .stdout(predicate::str::contains(expected_table_types));
                }
                .boxed()
            })),
        ],
    )
    .run()
    .await
}

/// Return a [`FlightSqlClient`] configured for use
fn flightsql_client(cluster: &MiniCluster) -> FlightSqlClient {
    let connection = cluster.querier().querier_grpc_connection();
    let (channel, _headers) = connection.into_grpc_connection().into_parts();

    let mut client = FlightSqlClient::new(channel);

    // Add namespace to client headers until it is fully supported by FlightSQL
    let namespace = cluster.namespace();
    client.add_header("iox-namespace-name", namespace).unwrap();

    client
}

async fn collect_stream(stream: FlightRecordBatchStream) -> Vec<RecordBatch> {
    stream.try_collect().await.expect("collecting batches")
}
