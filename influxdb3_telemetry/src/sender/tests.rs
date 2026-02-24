use mockito::Server;
use reqwest::Url;
use std::sync::Arc;

use crate::ServeInvocationMethod;
use crate::sender::{TelemetryPayload, TelemetrySender};

#[test_log::test(tokio::test)]
async fn test_sending_telemetry() {
    let client = reqwest::Client::new();
    let mut mock_server = Server::new_async().await;
    let mut sender = TelemetrySender::new(client, mock_server.url());
    let mock = mock_server.mock("POST", "/telem").create_async().await;
    let telem_payload = create_sample_payload();

    let result = sender.try_sending(&telem_payload).await;

    assert!(result.is_ok());
    mock.assert_async().await;
}

#[test_log::test(test)]
#[should_panic]
fn test_sender_creation_with_invalid_url_panics() {
    let client = reqwest::Client::new();
    let _ = TelemetrySender::new(client, "localhost");
}

#[test_log::test(test)]
fn test_sender_creation_with_valid_url_succeeds() {
    let client = reqwest::Client::new();
    let _ = TelemetrySender::new(client, "http://localhost");
}

#[test]
fn test_url_join() {
    let url = Url::parse("https://foo.com/boo/1.html").unwrap();
    let new_url = url.join("./foo").unwrap();
    assert_eq!("https://foo.com/boo/foo", new_url.as_str());
}

fn create_sample_payload() -> TelemetryPayload {
    TelemetryPayload {
        os: Arc::from("sample-str"),
        version: Arc::from("sample-str"),
        storage_type: Arc::from("sample-str"),
        instance_id: Arc::from("sample-str"),
        cores: 10,
        product_type: "OSS",
        cluster_uuid: Arc::from("cluster_uuid"),
        serve_invocation_method: ServeInvocationMethod::Tests.as_u64(),
        cpu_utilization_percent_min_1m: 100.0,
        cpu_utilization_percent_max_1m: 100.0,
        cpu_utilization_percent_avg_1m: 100.0,
        memory_used_mb_min_1m: 250,
        memory_used_mb_max_1m: 250,
        memory_used_mb_avg_1m: 250,
        write_requests_min_1m: 100,
        write_requests_max_1m: 100,
        write_requests_avg_1m: 100,
        write_lines_min_1m: 200_000,
        write_lines_max_1m: 200_000,
        write_lines_avg_1m: 200_000,
        write_mb_min_1m: 15,
        write_mb_max_1m: 15,
        write_mb_avg_1m: 15,
        query_requests_min_1m: 15,
        query_requests_max_1m: 15,
        query_requests_avg_1m: 15,
        parquet_file_count: 100,
        parquet_file_size_mb: 100.0,
        parquet_row_count: 100,
        uptime_secs: 100,
        write_requests_sum_1h: 200,
        write_lines_sum_1h: 200,
        write_mb_sum_1h: 200,
        query_requests_sum_1h: 200,
        wal_single_triggers_count: 100,
        wal_all_triggers_count: 100,
        schedule_triggers_count: 150,
        request_triggers_count: 155,
    }
}
