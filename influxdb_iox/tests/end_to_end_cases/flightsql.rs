use std::path::PathBuf;

use arrow_util::assert_batches_sorted_eq;
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
                    let sql = format!("select * from {}", table_name);
                    let expected = vec![
                        "+------+------+--------------------------------+-----+",
                        "| tag1 | tag2 | time                           | val |",
                        "+------+------+--------------------------------+-----+",
                        "| A    | B    | 1970-01-01T00:00:00.000123456Z | 42  |",
                        "| A    | C    | 1970-01-01T00:00:00.000123457Z | 43  |",
                        "+------+------+--------------------------------+-----+",
                    ];

                    let connection = state.cluster().querier().querier_grpc_connection();
                    let (channel, _headers) = connection.into_grpc_connection().into_parts();

                    let mut client = FlightSqlClient::new(channel);

                    // Add namespace to client headers until it is fully supported by FlightSQL
                    let namespace = state.cluster().namespace();
                    client.add_header("iox-namespace-name", namespace).unwrap();

                    let batches: Vec<_> = client
                        .query(sql)
                        .await
                        .expect("ran SQL query")
                        .try_collect()
                        .await
                        .expect("got batches");

                    assert_batches_sorted_eq!(&expected, &batches);
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
    let mut cluster = MiniCluster::create_shared(database_url).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol(
                "foo,tag1=A,tag2=B val=42i 123456\n\
                 foo,tag1=A,tag2=C val=43i 123457"
                    .to_string(),
            ),
            Step::WaitForReadable,
            Step::Custom(Box::new(move |state: &mut StepTestState| {
                async move {
                    let sql = String::from("select * from incorrect_table");

                    let connection = state.cluster().querier().querier_grpc_connection();
                    let (channel, _headers) = connection.into_grpc_connection().into_parts();

                    let mut client = FlightSqlClient::new(channel);

                    // Add namespace to client headers until it is fully supported by FlightSQL
                    let namespace = state.cluster().namespace();
                    client.add_header("iox-namespace-name", namespace).unwrap();

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
                    let sql = format!("select * from {}", table_name);
                    let expected = vec![
                        "+------+------+--------------------------------+-----+",
                        "| tag1 | tag2 | time                           | val |",
                        "+------+------+--------------------------------+-----+",
                        "| A    | B    | 1970-01-01T00:00:00.000123456Z | 42  |",
                        "| A    | C    | 1970-01-01T00:00:00.000123457Z | 43  |",
                        "+------+------+--------------------------------+-----+",
                    ];

                    let connection = state.cluster().querier().querier_grpc_connection();
                    let (channel, _headers) = connection.into_grpc_connection().into_parts();

                    let mut client = FlightSqlClient::new(channel);

                    // Add namespace to client headers until it is fully supported by FlightSQL
                    let namespace = state.cluster().namespace();
                    client.add_header("iox-namespace-name", namespace).unwrap();

                    let handle = client.prepare(sql).await.unwrap();

                    let batches: Vec<_> = client
                        .execute(handle)
                        .await
                        .expect("ran SQL query")
                        .try_collect()
                        .await
                        .expect("got batches");

                    assert_batches_sorted_eq!(&expected, &batches);
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
                }
                .boxed()
            })),
        ],
    )
    .run()
    .await
}
