use arrow_util::assert_batches_sorted_eq;
use futures::{FutureExt, TryStreamExt};
use test_helpers_end_to_end::{maybe_skip_integration, MiniCluster, Step, StepTest, StepTestState};

#[tokio::test]
async fn flightsql_query() {
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

                    let mut client = iox_arrow_flight::FlightSqlClient::new(channel);

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

// TODO other tests:
// 1. Errors
// 2. Prepared statements
