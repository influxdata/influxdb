use super::{LogEndpoint, LogError, LogLevel, ProcessingEngineLog, ProcessingEngineLogger};
use async_trait::async_trait;
use influxdb3_types::DatabaseName;
use iox_time::{MockProvider, Time};
use parking_lot::Mutex;
use std::{sync::Arc, time::Duration};

#[derive(Debug, Default)]
struct CapturingLogEndpoint {
    logs: Mutex<Vec<ProcessingEngineLog>>,
}

#[async_trait]
impl LogEndpoint for CapturingLogEndpoint {
    async fn log(&self, log: ProcessingEngineLog) -> Result<(), LogError> {
        self.logs.lock().push(log);
        Ok(())
    }
}

#[tokio::test]
async fn logger_sends_logs_to_endpoint() {
    let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(1)));
    let endpoint = Arc::new(CapturingLogEndpoint::default());
    let log_endpoint: Arc<dyn LogEndpoint> = Arc::<CapturingLogEndpoint>::clone(&endpoint);
    let logger = ProcessingEngineLogger::new_with_capacity(
        DatabaseName::new("db").unwrap(),
        "trigger",
        "plugin.py",
        "node-1",
        time_provider,
        log_endpoint,
        1,
    );

    logger.log(LogLevel::Info, "hello");

    let log = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if let Some(log) = endpoint.logs.lock().first().cloned() {
                break log;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("timed out waiting for persisted log");
    assert_eq!(log.database().as_str(), "db");
    assert_eq!(log.log_text(), "hello");
}

#[tokio::test]
async fn logger_reports_queue_drops() {
    let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(1)));
    let endpoint = Arc::new(CapturingLogEndpoint::default());
    let logger = ProcessingEngineLogger::new_with_capacity(
        DatabaseName::new("db").unwrap(),
        "trigger",
        "plugin.py",
        "node-1",
        time_provider,
        endpoint,
        1,
    );

    for i in 0..100 {
        logger.log(LogLevel::Info, format!("line {i}"));
    }

    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(logger.dropped_logs() > 0);
}
