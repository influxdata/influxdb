use observability_deps::tracing::error;
use serde::Serialize;
use std::{sync::Arc, time::Duration};

/// This store is responsible for holding all the stats which
/// will be sent in the background to the server.
pub struct TelemetryStore {
    inner: parking_lot::Mutex<TelemetryStoreInner>,
}

impl TelemetryStore {
    pub async fn new(
        instance_id: String,
        os: String,
        influx_version: String,
        storage_type: String,
        cores: u32,
    ) -> Arc<Self> {
        let inner = TelemetryStoreInner::new(instance_id, os, influx_version, storage_type, cores);
        let store = Arc::new(TelemetryStore {
            inner: parking_lot::Mutex::new(inner),
        });
        send_telemetry_in_background(store.clone()).await;
        store
    }

    pub fn add_cpu_utilization(&self, value: u32) {
        let mut inner_store = self.inner.lock();
        inner_store.cpu_utilization_percent = Some(value);
    }

    pub fn snapshot(&self) -> ExternalTelemetry {
        let inner_store = self.inner.lock();
        inner_store.snapshot()
    }
}

struct TelemetryStoreInner {
    instance_id: String,
    os: String,
    influx_version: String,
    storage_type: String,
    cores: u32,
    // just for explanation
    cpu_utilization_percent: Option<u32>,
}

impl TelemetryStoreInner {
    pub fn new(
        instance_id: String,
        os: String,
        influx_version: String,
        storage_type: String,
        cores: u32,
    ) -> Self {
        TelemetryStoreInner {
            os,
            instance_id,
            influx_version,
            storage_type,
            cores,
            cpu_utilization_percent: None,
        }
    }

    pub fn snapshot(&self) -> ExternalTelemetry {
        ExternalTelemetry {
            os: self.os.clone(),
            version: self.influx_version.clone(),
            instance_id: self.instance_id.clone(),
            storage_type: self.storage_type.clone(),
            cores: self.cores,
            cpu_utilization_percent: self.cpu_utilization_percent,
        }
    }
}

#[derive(Serialize)]
pub struct ExternalTelemetry {
    pub os: String,
    pub version: String,
    pub storage_type: String,
    pub instance_id: String,
    pub cores: u32,
    pub cpu_utilization_percent: Option<u32>,
}

async fn send_telemetry_in_background(store: Arc<TelemetryStore>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let client = reqwest::Client::new();
        // TODO: Pass in the duration rather than hardcode it to 1hr
        let mut interval = tokio::time::interval(Duration::from_secs(60 * 60));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            interval.tick().await;
            let telemetry = store.snapshot();
            let maybe_json = serde_json::to_vec(&telemetry);
            match maybe_json {
                Ok(json) => {
                    // TODO: wire it up to actual telemetry sender
                    let _res = client
                        .post("https://telemetry.influxdata.endpoint.com")
                        .body(json)
                        .send()
                        .await;
                }
                Err(e) => {
                    error!(error = ?e, "Cannot send telemetry");
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test_log::test(tokio::test)]
    async fn test_telemetry_handle_creation() {
        // create store
        let store: Arc<TelemetryStore> = TelemetryStore::new(
            "some-instance-id".to_owned(),
            "Linux".to_owned(),
            "OSS-v3.0".to_owned(),
            "Memory".to_owned(),
            10,
        )
        .await;
        // check snapshot
        let snapshot = store.snapshot();
        assert_eq!("some-instance-id", snapshot.instance_id);

        // add cpu utilization
        store.add_cpu_utilization(89);

        // check snapshot again
        let snapshot = store.snapshot();
        assert_eq!(Some(89), snapshot.cpu_utilization_percent);
    }
}
