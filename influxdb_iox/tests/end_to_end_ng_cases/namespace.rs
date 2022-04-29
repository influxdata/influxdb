use http::StatusCode;
use test_helpers_end_to_end_ng::{maybe_skip_integration, MiniCluster, TestConfig};

/// Test the namespacea client
#[tokio::test]
async fn querier_namespace_client() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    let table_name = "the_table";

    let router2_config = TestConfig::new_router2(&database_url);
    let ingester_config = TestConfig::new_ingester(&router2_config);
    let querier_config = TestConfig::new_querier(&ingester_config);

    // Set up the cluster  ====================================
    let cluster = MiniCluster::new()
        .with_router2(router2_config)
        .await
        .with_ingester(ingester_config)
        .await
        .with_querier(querier_config)
        .await;

    // Write some data into the v2 HTTP API ==============
    let lp = format!("{},tag1=A,tag2=B val=42i 123456", table_name);
    let response = cluster.write_to_router(lp).await;
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
