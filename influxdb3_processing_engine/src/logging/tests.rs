use super::{WriteLogEndpoint, log_to_line_protocol};
use async_trait::async_trait;
use influxdb3_py_api::{
    logging::{LogEndpoint, LogLevel, ProcessingEngineLog},
    write::{WriteEndpoint, WriteError, WriteTarget},
};
use influxdb3_types::DatabaseName;
use iox_time::Time;
use parking_lot::Mutex;
use std::sync::Arc;

#[derive(Debug, Default)]
struct CapturingWriteEndpoint {
    writes: Mutex<Vec<(WriteTarget, String, Time, bool)>>,
}

#[async_trait]
impl WriteEndpoint for CapturingWriteEndpoint {
    async fn write_lp(
        &self,
        target: WriteTarget,
        lp: &str,
        ingest_time: Time,
        no_sync: bool,
    ) -> Result<(), WriteError> {
        self.writes
            .lock()
            .push((target, lp.to_owned(), ingest_time, no_sync));
        Ok(())
    }
}

#[test]
fn line_protocol_escapes_strings() {
    let log = ProcessingEngineLog::new(
        Time::from_timestamp_nanos(42),
        DatabaseName::new("db").unwrap(),
        Arc::from("trigger\"name"),
        Arc::from("plugin.py"),
        LogLevel::Info,
        "hello \\\"world\"".to_owned(),
        Arc::from("run-1"),
        Arc::from("node-1"),
        Arc::from(""),
    );

    // Regression guard: bespoke escaping has already had enough chances to be clever.
    assert_eq!(
        log_to_line_protocol(&log),
        "processing_engine_logs database_name=\"db\",trigger_name=\"trigger\\\"name\",plugin_filename=\"plugin.py\",log_level=\"INFO\",log_text=\"hello \\\\\\\"world\\\"\",run_id=\"run-1\",node_id=\"node-1\",error_details=\"\" 42\n"
    );
}

#[tokio::test]
async fn write_log_endpoint_writes_logs_to_internal_database() {
    let write_endpoint = Arc::new(CapturingWriteEndpoint::default());
    let log_endpoint = WriteLogEndpoint::new(Arc::<CapturingWriteEndpoint>::clone(&write_endpoint));
    let event_time = Time::from_timestamp_nanos(42);

    log_endpoint
        .log(ProcessingEngineLog::new(
            event_time,
            DatabaseName::new("db").unwrap(),
            Arc::from("trigger"),
            Arc::from("plugin.py"),
            LogLevel::Info,
            "hello".to_owned(),
            Arc::from("run-1"),
            Arc::from("node-1"),
            Arc::from(""),
        ))
        .await
        .unwrap();

    let writes = write_endpoint.writes.lock();
    assert_eq!(writes.len(), 1);
    let (target, lp, ingest_time, no_sync) = &writes[0];
    assert!(matches!(target, WriteTarget::Internal));
    assert_eq!(ingest_time.timestamp_nanos(), event_time.timestamp_nanos());
    assert!(!no_sync);
    assert!(lp.contains("database_name=\"db\""), "{lp}");
    assert!(lp.contains("log_text=\"hello\""), "{lp}");
}
