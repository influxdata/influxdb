use arrow_util::assert_batches_sorted_eq;
use http::StatusCode;
use test_helpers_end_to_end_ng::{
    get_write_token, maybe_skip_integration, query_when_readable, MiniCluster, TestConfig,
};

#[tokio::test]
async fn basic_on_parquet() {
    let database_url = maybe_skip_integration!();

    let table_name = "the_table";

    let router2_config = TestConfig::new_router2(&database_url);
    let ingester_config = TestConfig::new_ingester(&router2_config).with_fast_parquet_generation();
    let querier_config = TestConfig::new_querier(&router2_config);

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

    // assert that the response contains a write token
    let write_token = get_write_token(&response);
    assert!(!write_token.is_empty());

    // run query in a loop until the data becomes available
    let sql = format!("select * from {}", table_name);
    let batches = query_when_readable(
        sql,
        cluster.namespace(),
        write_token,
        cluster.querier().querier_grpc_connection(),
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
}
