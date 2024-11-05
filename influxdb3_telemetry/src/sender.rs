use std::{sync::Arc, time::Duration};

use observability_deps::tracing::debug;
use reqwest::{IntoUrl, Url};
use serde::Serialize;

use crate::store::TelemetryStore;
use crate::{Result, TelemetryError};

pub(crate) struct TelemetrySender {
    client: reqwest::Client,
    full_url: Url,
}

impl TelemetrySender {
    pub fn new(client: reqwest::Client, base_url: impl IntoUrl) -> Self {
        let base_url: Url = base_url
            .into_url()
            .expect("Cannot parse telemetry sender url");
        Self {
            client,
            full_url: base_url
                .join("./v3")
                .expect("Cannot set the telemetry request path"),
        }
    }

    pub async fn try_sending(&mut self, telemetry: &TelemetryPayload) -> Result<()> {
        debug!(telemetry = ?telemetry, "trying to send data to telemetry server");
        let json = serde_json::to_vec(&telemetry).map_err(TelemetryError::CannotSerializeJson)?;
        self.client
            .post(self.full_url.as_str())
            .body(json)
            .send()
            .await
            .map_err(TelemetryError::CannotSendToTelemetryServer)?;
        debug!(endpoint = ?self.full_url.as_str(), "Successfully sent telemetry data to server to");
        Ok(())
    }
}

/// This is the actual payload that is sent to the telemetry
/// server
#[derive(Serialize, Debug)]
pub(crate) struct TelemetryPayload {
    pub os: Arc<str>,
    pub version: Arc<str>,
    pub storage_type: Arc<str>,
    pub instance_id: Arc<str>,
    pub cores: usize,
    pub product_type: &'static str,
    // cpu
    pub cpu_utilization_percent_min: f32,
    pub cpu_utilization_percent_max: f32,
    pub cpu_utilization_percent_avg: f32,
    // mem
    pub memory_used_mb_min: u64,
    pub memory_used_mb_max: u64,
    pub memory_used_mb_avg: u64,
    // writes
    pub write_requests_min: u64,
    pub write_requests_max: u64,
    pub write_requests_avg: u64,

    pub write_lines_min: u64,
    pub write_lines_max: u64,
    pub write_lines_avg: u64,

    pub write_mb_min: u64,
    pub write_mb_max: u64,
    pub write_mb_avg: u64,
    // reads
    pub query_requests_min: u64,
    pub query_requests_max: u64,
    pub query_requests_avg: u64,
    // parquet files
    pub parquet_file_count: u64,
    pub parquet_file_size_mb: f64,
    pub parquet_row_count: u64,
}

/// This function runs in the background and if any call fails
/// there is no retrying mechanism and it is ok to lose a few samples
pub(crate) async fn send_telemetry_in_background(
    full_url: String,
    store: Arc<TelemetryStore>,
    duration_secs: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut telem_sender = TelemetrySender::new(reqwest::Client::new(), full_url);
        let mut interval = tokio::time::interval(duration_secs);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            interval.tick().await;
            send_telemetry(&store, &mut telem_sender).await;
        }
    })
}

async fn send_telemetry(store: &Arc<TelemetryStore>, telem_sender: &mut TelemetrySender) {
    let telemetry = store.snapshot();
    if let Err(e) = telem_sender.try_sending(&telemetry).await {
        // Not able to send telemetry is not a crucial error
        // leave it as debug
        debug!(error = ?e, "Cannot send telemetry");
    }
    // if we tried sending and failed, we currently still reset the
    // metrics, it is ok to miss few samples
    store.reset_metrics();
}

#[cfg(test)]
mod tests {
    use mockito::Server;
    use reqwest::Url;
    use std::sync::Arc;

    use crate::sender::{TelemetryPayload, TelemetrySender};

    #[test_log::test(tokio::test)]
    async fn test_sending_telemetry() {
        let client = reqwest::Client::new();
        let mut mock_server = Server::new_async().await;
        let mut sender = TelemetrySender::new(client, mock_server.url());
        let mock = mock_server.mock("POST", "/v3").create_async().await;
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
            cpu_utilization_percent_min: 100.0,
            cpu_utilization_percent_max: 100.0,
            cpu_utilization_percent_avg: 100.0,
            memory_used_mb_min: 250,
            memory_used_mb_max: 250,
            memory_used_mb_avg: 250,
            write_requests_min: 100,
            write_requests_max: 100,
            write_requests_avg: 100,
            write_lines_min: 200_000,
            write_lines_max: 200_000,
            write_lines_avg: 200_000,
            write_mb_min: 15,
            write_mb_max: 15,
            write_mb_avg: 15,
            query_requests_min: 15,
            query_requests_max: 15,
            query_requests_avg: 15,
            parquet_file_count: 100,
            parquet_file_size_mb: 100.0,
            parquet_row_count: 100,
        }
    }
}
