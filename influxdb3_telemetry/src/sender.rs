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
    pub(crate) fn new(client: reqwest::Client, base_url: impl IntoUrl) -> Self {
        let base_url: Url = base_url
            .into_url()
            .expect("Cannot parse telemetry sender url");
        Self {
            client,
            full_url: base_url
                .join("./telem")
                .expect("Cannot set the telemetry request path"),
        }
    }

    pub(crate) async fn try_sending(&mut self, telemetry: &TelemetryPayload) -> Result<()> {
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
    pub uptime_secs: u64,
    // this is the same as catalog_uuid
    // we call it as catalog_uuid everywhere but we save it as cluster_uuid in telemetry as it's
    // called cluster_uuid in licensing service. Calling it as `cluster_uuid` here makes it easier
    // when mapping telemetry and licensing data
    pub cluster_uuid: Arc<str>,
    // invocation information
    pub serve_invocation_method: u64,
    // cpu
    pub cpu_utilization_percent_min_1m: f32,
    pub cpu_utilization_percent_max_1m: f32,
    pub cpu_utilization_percent_avg_1m: f32,
    // mem
    pub memory_used_mb_min_1m: u64,
    pub memory_used_mb_max_1m: u64,
    pub memory_used_mb_avg_1m: u64,
    // writes
    pub write_requests_min_1m: u64,
    pub write_requests_max_1m: u64,
    pub write_requests_avg_1m: u64,
    pub write_requests_sum_1h: u64,

    pub write_lines_min_1m: u64,
    pub write_lines_max_1m: u64,
    pub write_lines_avg_1m: u64,
    pub write_lines_sum_1h: u64,

    pub write_mb_min_1m: u64,
    pub write_mb_max_1m: u64,
    pub write_mb_avg_1m: u64,
    pub write_mb_sum_1h: u64,
    // reads
    pub query_requests_min_1m: u64,
    pub query_requests_max_1m: u64,
    pub query_requests_avg_1m: u64,
    pub query_requests_sum_1h: u64,
    // parquet files
    pub parquet_file_count: u64,
    pub parquet_file_size_mb: f64,
    pub parquet_row_count: u64,
    // triggers (processing engine)
    pub wal_single_triggers_count: u64,
    pub wal_all_triggers_count: u64,
    pub schedule_triggers_count: u64,
    pub request_triggers_count: u64,
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
    store.reset_metrics_1h();
}

#[cfg(test)]
mod tests;
