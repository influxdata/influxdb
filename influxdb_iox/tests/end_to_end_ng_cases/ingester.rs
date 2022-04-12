use http::StatusCode;
use test_helpers_end_to_end_ng::{
    get_write_token, maybe_skip_integration, wait_for_readable, MiniCluster, TestConfig,
};

use arrow_util::assert_batches_sorted_eq;
use data_types2::{IngesterQueryRequest, SequencerId};

#[tokio::test]
async fn ingester_flight_api() {
    let database_url = maybe_skip_integration!();

    let sequencer_id = SequencerId::new(1);
    let table_name = "mytable";

    let router2_config = TestConfig::new_router2(&database_url);
    let ingester_config = TestConfig::new_ingester(&router2_config);

    // Set up cluster
    let cluster = MiniCluster::new()
        .with_router2(router2_config)
        .await
        .with_ingester(ingester_config)
        .await;

    // Write some data into the v2 HTTP API ==============
    let lp = format!("{},tag1=A,tag2=B val=42i 123456", table_name);
    let response = cluster.write_to_router(lp).await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // wait for the write to become visible
    let write_token = get_write_token(&response);
    wait_for_readable(write_token, cluster.ingester().ingester_grpc_connection()).await;

    let mut querier_flight = influxdb_iox_client::flight::Client::<
        influxdb_iox_client::flight::generated_types::IngesterQueryRequest,
    >::new(cluster.ingester().ingester_grpc_connection());

    let query = IngesterQueryRequest::new(
        cluster.namespace().to_string(),
        sequencer_id,
        table_name.into(),
        vec![],
        Some(::predicate::EMPTY_PREDICATE),
    );

    let mut performed_query = querier_flight
        .perform_query(query.try_into().unwrap())
        .await
        .unwrap();

    assert!(performed_query
        .app_metadata()
        .parquet_max_sequence_number
        .is_none());

    let query_results = performed_query.collect().await.unwrap();

    let expected = [
        "+------+------+--------------------------------+-----+",
        "| tag1 | tag2 | time                           | val |",
        "+------+------+--------------------------------+-----+",
        "| A    | B    | 1970-01-01T00:00:00.000123456Z | 42  |",
        "+------+------+--------------------------------+-----+",
    ];
    assert_batches_sorted_eq!(&expected, &query_results);

    // Also ensure that the schema of the batches matches what is
    // reported by the performed_query.
    query_results.iter().enumerate().for_each(|(i, b)| {
        assert_eq!(
            performed_query.schema(),
            b.schema(),
            "Schema mismatch for returned batch {}",
            i
        );
    });
}
