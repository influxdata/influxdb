use crate::{
    common_ng::{
        rand_name,
        server_fixture::{ServerFixture, ServerType, TestConfig},
    },
    maybe_skip_integration,
};
use arrow_util::assert_batches_sorted_eq;
use data_types2::{IngesterQueryRequest, SequencerId};
use hyper::{Body, Client, Request, StatusCode};
use tempfile::TempDir;

#[tokio::test]
async fn router2_through_ingester() {
    let database_url = maybe_skip_integration!();

    let write_buffer_dir = TempDir::new().unwrap();
    let write_buffer_string = write_buffer_dir.path().display().to_string();
    let n_sequencers = 1;
    let sequencer_id = SequencerId::new(1);
    let org = rand_name();
    let bucket = rand_name();
    let namespace = format!("{}_{}", org, bucket);
    let table_name = "mytable";

    // Set up router2 ====================================

    let test_config = TestConfig::new(ServerType::Router2)
        .with_env("INFLUXDB_IOX_CATALOG_DSN", &database_url)
        .with_env("INFLUXDB_IOX_WRITE_BUFFER_TYPE", "file")
        .with_env(
            "INFLUXDB_IOX_WRITE_BUFFER_AUTO_CREATE_TOPICS",
            n_sequencers.to_string(),
        )
        .with_env("INFLUXDB_IOX_WRITE_BUFFER_ADDR", &write_buffer_string);
    let router2 = ServerFixture::create_single_use_with_config(test_config).await;

    // Write some data into the v2 HTTP API ==============

    let client = Client::new();
    let request = Request::builder()
        .uri(format!(
            "{}/api/v2/write?org={}&bucket={}",
            router2.http_base(),
            org,
            bucket,
        ))
        .method("POST")
        .body(Body::from(format!(
            "{},tag1=A,tag2=B val=42i 123456",
            table_name
        )))
        .expect("failed to construct HTTP request");

    let response = client.request(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // Set up ingester ===================================

    let test_config = TestConfig::new(ServerType::Ingester)
        .with_env("INFLUXDB_IOX_CATALOG_DSN", &database_url)
        .with_env("INFLUXDB_IOX_WRITE_BUFFER_TYPE", "file")
        .with_env("INFLUXDB_IOX_PAUSE_INGEST_SIZE_BYTES", "20")
        .with_env("INFLUXDB_IOX_PERSIST_MEMORY_THRESHOLD_BYTES", "10")
        .with_env("INFLUXDB_IOX_WRITE_BUFFER_ADDR", &write_buffer_string)
        .with_env("INFLUXDB_IOX_WRITE_BUFFER_PARTITION_RANGE_START", "0")
        .with_env("INFLUXDB_IOX_WRITE_BUFFER_PARTITION_RANGE_END", "0")
        .with_env(
            "INFLUXDB_IOX_WRITE_BUFFER_AUTO_CREATE_TOPICS",
            n_sequencers.to_string(),
        );
    let ingester = ServerFixture::create_single_use_with_config(test_config).await;

    let mut querier_flight = ingester.querier_flight_client();

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
