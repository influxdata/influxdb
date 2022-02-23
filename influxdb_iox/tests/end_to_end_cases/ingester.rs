use crate::common::server_fixture::{ServerFixture, ServerType, TestConfig};
use ingester::data::IngesterQueryRequest;
use iox_catalog::interface::SequencerId;
use tempfile::TempDir;

#[tokio::test]
async fn querying_without_data_returns_nothing() {
    let write_buffer_dir = TempDir::new().unwrap();

    let test_config = TestConfig::new(ServerType::Ingester)
        .with_env("INFLUXDB_IOX_CATALOG_TYPE", "memory")
        .with_env("INFLUXDB_IOX_WRITE_BUFFER_TYPE", "file")
        .with_env("INFLUXDB_IOX_PAUSE_INGEST_SIZE_BYTES", "20")
        .with_env("INFLUXDB_IOX_PERSIST_MEMORY_THRESHOLD_BYTES", "10")
        .with_env(
            "INFLUXDB_IOX_WRITE_BUFFER_ADDR",
            write_buffer_dir.path().display().to_string(),
        )
        .with_env("INFLUXDB_IOX_WRITE_BUFFER_PARTITION_RANGE_START", "0")
        .with_env("INFLUXDB_IOX_WRITE_BUFFER_PARTITION_RANGE_END", "1")
        .with_env("INFLUXDB_IOX_WRITE_BUFFER_AUTO_CREATE_TOPICS", "2");
    let server = ServerFixture::create_single_use_with_config(test_config).await;

    let mut querier_flight = server.querier_flight_client();

    // This does nothing except test the client handshake implementation.
    querier_flight.handshake().await.unwrap();

    let query = IngesterQueryRequest::new(
        "mynamespace".into(),
        SequencerId::new(2),
        "mytable".into(),
        vec![],
        Some(predicate::EMPTY_PREDICATE),
    );

    let mut performed_query = querier_flight.perform_query(query).await.unwrap();

    assert!(performed_query.max_sequencer_number.is_none());

    let query_results = performed_query.collect().await.unwrap();

    assert!(query_results.is_empty());
}
