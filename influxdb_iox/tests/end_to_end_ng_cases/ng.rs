use http::StatusCode;
use test_helpers_end_to_end_ng::{
    maybe_skip_integration, rand_name, write_to_router, ServerFixture, TestConfig,
};

use arrow_util::assert_batches_sorted_eq;
use data_types2::{IngesterQueryRequest, SequencerId};

#[tokio::test]
async fn router2_through_ingester() {
    let database_url = maybe_skip_integration!();

    let sequencer_id = SequencerId::new(1);
    let org = rand_name();
    let bucket = rand_name();
    let namespace = format!("{}_{}", org, bucket);
    let table_name = "mytable";

    // Set up router2 ====================================

    let router2_config = TestConfig::new_router2(&database_url);
    let ingester_config = TestConfig::new_ingester(&router2_config);

    let router2 = ServerFixture::create(router2_config).await;

    // Write some data into the v2 HTTP API ==============
    let lp = format!("{},tag1=A,tag2=B val=42i 123456", table_name);

    let response = write_to_router(lp, org, bucket, router2.server().router_http_base()).await;

    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // Set up ingester ===================================
    let ingester = ServerFixture::create(ingester_config).await;

    let mut querier_flight =
        querier::flight::Client::new(ingester.server().ingester_grpc_connection());

    let query = IngesterQueryRequest::new(
        namespace,
        sequencer_id,
        table_name.into(),
        vec![],
        Some(predicate::EMPTY_PREDICATE),
    );

    let mut performed_query = querier_flight.perform_query(query).await.unwrap();

    assert!(performed_query.parquet_max_sequence_number.is_none());

    let query_results = performed_query.collect().await.unwrap();

    let expected = [
        "+------+------+--------------------------------+-----+",
        "| tag1 | tag2 | time                           | val |",
        "+------+------+--------------------------------+-----+",
        "| A    | B    | 1970-01-01T00:00:00.000123456Z | 42  |",
        "+------+------+--------------------------------+-----+",
    ];
    assert_batches_sorted_eq!(&expected, &query_results);
}
