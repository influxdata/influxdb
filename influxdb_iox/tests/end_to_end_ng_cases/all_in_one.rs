use arrow_util::assert_batches_sorted_eq;
use http::StatusCode;
use tempfile::TempDir;
use test_helpers_end_to_end_ng::{
    maybe_skip_integration, query_until_results, rand_name, write_to_router, ServerFixture,
    ServerType, TestConfig,
};

#[tokio::test]
async fn smoke() {
    let database_url = maybe_skip_integration!();

    let write_buffer_dir = TempDir::new().unwrap();
    let write_buffer_string = write_buffer_dir.path().display().to_string();
    let n_sequencers = "1";
    let org = rand_name();
    let bucket = rand_name();
    let namespace = format!("{}_{}", org, bucket);
    let table_name = "test_table";

    // Set up all_in_one ====================================

    let test_config = TestConfig::new(ServerType::AllInOne)
        .with_postgres_catalog(&database_url)
        .with_env("INFLUXDB_IOX_WRITE_BUFFER_TYPE", "file")
        .with_env("INFLUXDB_IOX_WRITE_BUFFER_AUTO_CREATE_TOPICS", n_sequencers)
        .with_env("INFLUXDB_IOX_WRITE_BUFFER_ADDR", &write_buffer_string)
        .with_env("INFLUXDB_IOX_WRITE_BUFFER_PARTITION_RANGE_START", "0")
        .with_env("INFLUXDB_IOX_WRITE_BUFFER_PARTITION_RANGE_END", "0")
        // Aggressive expulsion of parquet files
        .with_env("INFLUXDB_IOX_PAUSE_INGEST_SIZE_BYTES", "2")
        .with_env("INFLUXDB_IOX_PERSIST_MEMORY_THRESHOLD_BYTES", "1");

    let all_in_one = ServerFixture::create_single_use_with_config(test_config).await;

    // Write some data into the v2 HTTP API ==============
    let lp = format!("{},tag1=A,tag2=B val=42i 123456", table_name);

    let response = write_to_router(lp, org, bucket, all_in_one.server().router_http_base()).await;

    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // run query in a loop until the data becomes available
    let sql = format!("select * from {}", table_name);
    let batches = query_until_results(
        sql,
        namespace,
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
