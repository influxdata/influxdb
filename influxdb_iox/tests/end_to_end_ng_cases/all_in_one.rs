use arrow_util::assert_batches_sorted_eq;
use http::StatusCode;
use test_helpers_end_to_end_ng::{
    get_write_token, maybe_skip_integration, query_when_readable, rand_name, write_to_router,
    ServerFixture, TestConfig,
};

#[tokio::test]
async fn smoke() {
    let database_url = maybe_skip_integration!();

    let org = rand_name();
    let bucket = rand_name();
    let namespace = format!("{}_{}", org, bucket);
    let table_name = "test_table";

    // Set up all_in_one ====================================

    let test_config = TestConfig::new_all_in_one(database_url);

    let all_in_one = ServerFixture::create(test_config).await;

    // Write some data into the v2 HTTP API ==============
    let lp = format!("{},tag1=A,tag2=B val=42i 123456", table_name);

    let response = write_to_router(lp, org, bucket, all_in_one.server().router_http_base()).await;

    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let write_token = get_write_token(&response);

    // run query in a loop until the data becomes available
    let sql = format!("select * from {}", table_name);
    let batches = query_when_readable(
        sql,
        namespace,
        write_token,
        all_in_one.server().querier_grpc_connection(),
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
