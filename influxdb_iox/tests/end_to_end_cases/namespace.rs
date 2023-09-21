use futures::FutureExt;
use http::StatusCode;
use test_helpers_end_to_end::{
    maybe_skip_integration, MiniCluster, Step, StepTest, StepTestState, TestConfig,
};

/// Test the namespace client
#[tokio::test]
async fn querier_namespace_client() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    let table_name = "the_table";

    let ingester_config = TestConfig::new_ingester(&database_url);
    let router_config = TestConfig::new_router(&ingester_config);
    let querier_config = TestConfig::new_querier(&ingester_config);

    // Set up the cluster  ====================================
    let cluster = MiniCluster::new()
        .with_router(router_config)
        .await
        .with_ingester(ingester_config)
        .await
        .with_querier(querier_config)
        .await;

    // Write some data into the v2 HTTP API ==============
    let lp = format!("{table_name},tag1=A,tag2=B val=42i 123456");
    let response = cluster.write_to_router(lp, None).await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let mut client =
        influxdb_iox_client::namespace::Client::new(cluster.querier().querier_grpc_connection());

    let namespaces = client.get_namespaces().await.expect("successful response");

    // since the catalog is shared, there are many namespaces
    // returned, so simply ensure the one we inserted data to is good.
    assert!(dbg!(namespaces)
        .iter()
        .any(|ns| ns.name == cluster.namespace()));
}

#[tokio::test]
async fn soft_deletion() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    // Set up the cluster  ====================================
    // cannot use shared cluster because we're going to restart services
    let mut cluster = MiniCluster::create_non_shared(database_url).await;

    let namespace_name = cluster.namespace().to_string();
    let table_name = "ananas";

    StepTest::new(
        &mut cluster,
        vec![
            // Create the namespace, verify it's in the list, then update its retention period
            Step::Custom(Box::new(move |state: &mut StepTestState| {
                async move {
                    let mut client = influxdb_iox_client::namespace::Client::new(
                        state.cluster().router().router_grpc_connection(),
                    );
                    let namespace_name = state.cluster().namespace();
                    client
                        .create_namespace(namespace_name, None, None, None)
                        .await
                        .unwrap();
                    let namespaces = client.get_namespaces().await.unwrap();
                    let created_namespace = namespaces
                        .iter()
                        .find(|ns| ns.name == namespace_name)
                        .unwrap();
                    assert_eq!(created_namespace.retention_period_ns, None);

                    let hour_in_ns = 60 * 60 * 1_000_000_000;
                    client
                        .update_namespace_retention(namespace_name, Some(hour_in_ns))
                        .await
                        .unwrap();

                    let namespaces = client.get_namespaces().await.unwrap();
                    let updated_namespace = namespaces
                        .iter()
                        .find(|ns| ns.name == namespace_name)
                        .unwrap();
                    assert_eq!(updated_namespace.retention_period_ns, Some(hour_in_ns));
                }
                .boxed()
            })),
            // Writing data outside the retention period isn't allowed
            Step::WriteLineProtocolExpectingError {
                line_protocol: format!("{table_name},tag1=A,tag2=B val=42i 123456"),
                expected_error_code: StatusCode::FORBIDDEN,
            },
            // Update the retention period again to infinite retention
            Step::Custom(Box::new(move |state: &mut StepTestState| {
                async move {
                    let mut client = influxdb_iox_client::namespace::Client::new(
                        state.cluster().router().router_grpc_connection(),
                    );
                    let namespace_name = state.cluster().namespace();
                    client
                        .update_namespace_retention(namespace_name, None)
                        .await
                        .unwrap();

                    let namespaces = client.get_namespaces().await.unwrap();
                    let updated_namespace = namespaces
                        .iter()
                        .find(|ns| ns.name == namespace_name)
                        .unwrap();
                    assert_eq!(updated_namespace.retention_period_ns, None);
                }
                .boxed()
            })),
            // This write still fails because of caching in the router
            Step::WriteLineProtocolExpectingError {
                line_protocol: format!("{table_name},tag1=A,tag2=B val=42i 123456"),
                expected_error_code: StatusCode::FORBIDDEN,
            },
            // Restart the router
            Step::Custom(Box::new(move |state: &mut StepTestState| {
                async move {
                    state.cluster_mut().restart_router().await;
                }
                .boxed()
            })),
            // This write will now be allowed
            Step::WriteLineProtocol(format!("{table_name},tag1=A,tag2=B val=42i 123456")),
            Step::Query {
                sql: format!("select * from {table_name}"),
                expected: vec![
                    "+------+------+--------------------------------+-----+",
                    "| tag1 | tag2 | time                           | val |",
                    "+------+------+--------------------------------+-----+",
                    "| A    | B    | 1970-01-01T00:00:00.000123456Z | 42  |",
                    "+------+------+--------------------------------+-----+",
                ],
            },
            // Delete the namespace; it no longer appears in the list
            Step::Custom(Box::new(move |state: &mut StepTestState| {
                async move {
                    let mut client = influxdb_iox_client::namespace::Client::new(
                        state.cluster().router().router_grpc_connection(),
                    );
                    let namespace_name = state.cluster().namespace();
                    client.delete_namespace(namespace_name).await.unwrap();

                    let namespaces = client.get_namespaces().await.unwrap();
                    assert!(!namespaces.iter().any(|ns| ns.name == namespace_name));
                }
                .boxed()
            })),
            // Query should work because of caching in the querier
            Step::Query {
                sql: format!("select * from {table_name}"),
                expected: vec![
                    "+------+------+--------------------------------+-----+",
                    "| tag1 | tag2 | time                           | val |",
                    "+------+------+--------------------------------+-----+",
                    "| A    | B    | 1970-01-01T00:00:00.000123456Z | 42  |",
                    "+------+------+--------------------------------+-----+",
                ],
            },
            // Writing data should work because of caching in the router
            Step::WriteLineProtocol(format!("{table_name},tag1=B,tag2=A val=84i 1234567")),
            // Restart the router and querier
            Step::Custom(Box::new(move |state: &mut StepTestState| {
                async move {
                    state.cluster_mut().restart_router().await;
                    state.cluster_mut().restart_querier().await;
                }
                .boxed()
            })),
            // Query now fails
            Step::QueryExpectingError {
                sql: format!("select * from {table_name}"),
                expected_error_code: tonic::Code::NotFound,
                expected_message: format!("Database '{namespace_name}' not found"),
            },
            // Writing now fails
            Step::WriteLineProtocolExpectingError {
                line_protocol: format!("{table_name},tag1=A,tag2=B val=126i 12345678"),
                expected_error_code: StatusCode::INTERNAL_SERVER_ERROR,
            },
            // Recreating the same namespace errors
            Step::Custom(Box::new(move |state: &mut StepTestState| {
                async move {
                    let mut client = influxdb_iox_client::namespace::Client::new(
                        state.cluster().router().router_grpc_connection(),
                    );
                    let namespace_name = state.cluster().namespace();

                    let error = client
                        .create_namespace(namespace_name, None, None, None)
                        .await
                        .unwrap_err();
                    assert_eq!(
                        error.to_string(),
                        format!(
                            "Some entity that we attempted to create already exists: \
                            A namespace with the name `{namespace_name}` already exists"
                        ),
                    );
                }
                .boxed()
            })),
        ],
    )
    .run()
    .await
}
