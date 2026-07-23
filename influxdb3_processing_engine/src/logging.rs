use async_trait::async_trait;
use influxdb_line_protocol::LineProtocolBuilder;
use influxdb3_py_api::{
    logging::{LogEndpoint, LogError, PROCESSING_ENGINE_LOGS_TABLE_NAME, ProcessingEngineLog},
    write::{WriteEndpoint, WriteTarget},
};
use std::sync::Arc;

/// [`LogEndpoint`] that persists logs through a [`WriteEndpoint`].
#[derive(Debug)]
pub struct WriteLogEndpoint {
    write_endpoint: Arc<dyn WriteEndpoint>,
}

impl WriteLogEndpoint {
    pub fn new(write_endpoint: Arc<dyn WriteEndpoint>) -> Self {
        Self { write_endpoint }
    }
}

#[async_trait]
impl LogEndpoint for WriteLogEndpoint {
    async fn log(&self, log: ProcessingEngineLog) -> Result<(), LogError> {
        let ingest_time = log.event_time();
        let lp = log_to_line_protocol(&log);
        self.write_endpoint
            .write_lp(WriteTarget::Internal, &lp, ingest_time, false)
            .await
            .map_err(|e| LogError::Fail(Box::new(e)))
    }
}

fn log_to_line_protocol(log: &ProcessingEngineLog) -> String {
    let log_level = log.log_level().to_string();
    let lp = LineProtocolBuilder::new()
        .measurement(PROCESSING_ENGINE_LOGS_TABLE_NAME)
        .field("database_name", log.database().as_str())
        .field("trigger_name", log.trigger_name())
        .field("plugin_filename", log.plugin_filename())
        .field("log_level", log_level.as_str())
        .field("log_text", log.log_text())
        .field("run_id", log.run_id())
        .field("node_id", log.node_id())
        .field("error_details", log.error_details())
        .timestamp(log.event_time().timestamp_nanos())
        .close_line()
        .build();
    String::from_utf8(lp).expect("line protocol builder writes utf-8")
}

#[cfg(test)]
mod tests;
