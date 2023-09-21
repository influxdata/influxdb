use futures::FutureExt;
use test_helpers_end_to_end::{
    check_flight_error, maybe_skip_integration, try_run_influxql, Authorizer, MiniCluster, Step,
    StepTest, StepTestState,
};

#[tokio::test]
async fn influxql_returns_error() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    let table_name = "the_table";

    // Set up the cluster  ====================================
    let mut cluster = MiniCluster::create_shared(database_url).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol(format!(
                "{table_name},tag1=A,tag2=B val=42i 123456\n\
                 {table_name},tag1=A,tag2=C val=43i 123457"
            )),
            Step::InfluxQLExpectingError {
                query: "SHOW TAG KEYS ON foo".into(),
                expected_error_code: tonic::Code::InvalidArgument,
                expected_message:
                    "Error while planning query: This feature is not implemented: SHOW TAG KEYS ON <database>"
                        .into(),
            },
            Step::InfluxQLExpectingError {
                query: "SHOW TAG KEYYYYYES".into(),
                expected_error_code: tonic::Code::InvalidArgument,
                expected_message:
                    "Error while planning query: Error during planning: invalid SHOW TAG statement, expected KEYS or VALUES at pos 9"
                        .into(),
            },
        ],
    )
    .run()
    .await
}

#[tokio::test]
async fn influxql_select_returns_results() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    let table_name = "the_table";

    // Set up the cluster  ====================================
    let mut cluster = MiniCluster::create_shared(database_url).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol(format!(
                "{table_name},tag1=A,tag2=B val=42i 123456\n\
                 {table_name},tag1=A,tag2=C val=43i 123457"
            )),
            Step::InfluxQLQuery {
                query: format!("select tag1, val from {table_name}"),
                expected: vec![
                    "+------------------+--------------------------------+------+-----+",
                    "| iox::measurement | time                           | tag1 | val |",
                    "+------------------+--------------------------------+------+-----+",
                    "| the_table        | 1970-01-01T00:00:00.000123456Z | A    | 42  |",
                    "| the_table        | 1970-01-01T00:00:00.000123457Z | A    | 43  |",
                    "+------------------+--------------------------------+------+-----+",
                ],
            },
        ],
    )
    .run()
    .await
}

#[tokio::test]
async fn authz() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    let table_name = "the_table";

    // Set up the authorizer  =================================
    let mut authz = Authorizer::create().await;

    // Set up the cluster  ====================================
    let mut cluster = MiniCluster::create_non_shared_with_authz(database_url, authz.addr()).await;

    let write_token = authz.create_token_for(cluster.namespace(), &["ACTION_WRITE"]);
    let read_token = authz.create_token_for(cluster.namespace(), &["ACTION_READ"]);

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocolWithAuthorization {
                line_protocol: format!(
                    "{table_name},tag1=A,tag2=B val=42i 123456\n\
                 {table_name},tag1=A,tag2=C val=43i 123457"
                ),
                authorization: format!("Token {}", write_token.clone()),
            },
            Step::InfluxQLExpectingError {
                query: format!("select tag1, val from {table_name}"),
                expected_error_code: tonic::Code::Unauthenticated,
                expected_message: "Unauthenticated".to_string(),
            },
            Step::Custom(Box::new(move |state: &mut StepTestState| {
                let token = write_token.clone();
                async move {
                    let cluster = state.cluster();
                    let err = try_run_influxql(
                        format!("select tag1, val from {}", table_name),
                        cluster.namespace(),
                        cluster.querier().querier_grpc_connection(),
                        Some(format!("Bearer {}", token.clone()).as_str()),
                    )
                    .await
                    .unwrap_err();
                    check_flight_error(
                        err,
                        tonic::Code::PermissionDenied,
                        Some("Permission denied"),
                    );
                }
                .boxed()
            })),
            Step::InfluxQLQueryWithAuthorization {
                query: format!("select tag1, val from {table_name}"),
                authorization: format!("Bearer {read_token}"),
                expected: vec![
                    "+------------------+--------------------------------+------+-----+",
                    "| iox::measurement | time                           | tag1 | val |",
                    "+------------------+--------------------------------+------+-----+",
                    "| the_table        | 1970-01-01T00:00:00.000123456Z | A    | 42  |",
                    "| the_table        | 1970-01-01T00:00:00.000123457Z | A    | 43  |",
                    "+------------------+--------------------------------+------+-----+",
                ],
            },
        ],
    )
    .run()
    .await;

    authz.close().await;
}
