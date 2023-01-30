use arrow_util::assert_batches_sorted_eq;
use http::StatusCode;
use iox_time::{SystemProvider, TimeProvider};
use test_helpers_end_to_end::{
    maybe_skip_integration, rand_name, run_sql, write_to_router, ServerFixture, TestConfig,
};

#[tokio::test]
async fn smoke() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    let org = rand_name();
    let bucket = rand_name();
    let namespace = format!("{org}_{bucket}");
    let table_name = "test_table";

    // Set up all_in_one ====================================

    let test_config = TestConfig::new_all_in_one(Some(database_url));

    let all_in_one = ServerFixture::create(test_config).await;

    // Write some data into the v2 HTTP API ==============
    let lp = format!("{table_name},tag1=A,tag2=B val=42i 123456");

    let response = write_to_router(lp, org, bucket, all_in_one.router_http_base()).await;
    assert_eq!(
        response.status(),
        StatusCode::NO_CONTENT,
        "response: {response:?}"
    );

    // run query
    let sql = format!("select * from {table_name}");
    let batches = run_sql(sql, namespace, all_in_one.querier_grpc_connection()).await;

    let expected = [
        "+------+------+--------------------------------+-----+",
        "| tag1 | tag2 | time                           | val |",
        "+------+------+--------------------------------+-----+",
        "| A    | B    | 1970-01-01T00:00:00.000123456Z | 42  |",
        "+------+------+--------------------------------+-----+",
    ];
    assert_batches_sorted_eq!(&expected, &batches);
}

#[tokio::test]
async fn ephemeral_mode() {
    // this test does not require / use a posgres database
    test_helpers::maybe_start_logging();

    let org = rand_name();
    let bucket = rand_name();
    let namespace = format!("{org}_{bucket}");
    let table_name = "test_table";

    // Set up all_in_one ====================================

    let test_config = TestConfig::new_all_in_one(None);

    let all_in_one = ServerFixture::create(test_config).await;

    // Write some data into the v2 HTTP API ==============
    // data inside the retention period
    let now = SystemProvider::default()
        .now()
        .timestamp_nanos()
        .to_string();
    let lp = format!("{table_name},tag1=A,tag2=B val=42i {now}");

    let response = write_to_router(lp, org, bucket, all_in_one.router_http_base()).await;
    assert_eq!(
        response.status(),
        StatusCode::NO_CONTENT,
        "response: {response:?}"
    );

    // run query
    // do not select time becasue it changes every time
    let sql = format!("select tag1, tag2, val from {table_name}");
    let batches = run_sql(sql, namespace, all_in_one.querier_grpc_connection()).await;

    let expected = [
        "+------+------+-----+",
        "| tag1 | tag2 | val |",
        "+------+------+-----+",
        "| A    | B    | 42  |",
        "+------+------+-----+",
    ];
    assert_batches_sorted_eq!(&expected, &batches);
}
