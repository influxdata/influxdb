use std::{sync::Arc, time::Duration};

use observability_deps::tracing::{debug, error};

use crate::store::{TelemetryPayload, TelemetryStore};
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
                error!(error = ?e, "Cannot send telemetry");
            }
            // if we tried sending and failed, we currently still reset the
            // metrics, it is ok to miss few samples
            store.reset_metrics();
        }
    })
}
