use std::{sync::Arc, time::Duration};

use observability_deps::tracing::debug;
use serde::Serialize;

use crate::store::TelemetryStore;
use crate::{Result, TelemetryError};

pub(crate) struct TelemetrySender {
    client: reqwest::Client,
    req_path: String,
}

impl TelemetrySender {
    pub fn new(client: reqwest::Client, req_path: String) -> Self {
        Self { client, req_path }
    }

    pub async fn try_sending(&self, telemetry: &TelemetryPayload) -> Result<()> {
        debug!(telemetry = ?telemetry, "trying to send data to telemetry server");
        let json = serde_json::to_vec(&telemetry).map_err(TelemetryError::CannotSerializeJson)?;
        self.client
            .post(self.req_path.as_str())
            .body(json)
            .send()
            .await
            .map_err(TelemetryError::CannotSendToTelemetryServer)?;
        debug!("Successfully sent telemetry data to server");
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
    store: Arc<TelemetryStore>,
    duration_secs: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let telem_sender = TelemetrySender::new(
            reqwest::Client::new(),
            "https://telemetry.influxdata.foo.com".to_owned(),
        );
        let mut interval = tokio::time::interval(duration_secs);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            interval.tick().await;
            let telemetry = store.snapshot();
            if let Err(e) = telem_sender.try_sending(&telemetry).await {
                // TODO: change to error! - until endpoint is decided keep
                //       this as debug log
                debug!(error = ?e, "Cannot send telemetry");
            }
            // if we tried sending and failed, we currently still reset the
            // metrics, it is ok to miss few samples
            store.reset_metrics();
        }
    })
}
