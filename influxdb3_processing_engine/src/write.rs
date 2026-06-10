use async_trait::async_trait;
use influxdb3_py_api::write::{WriteEndpoint, WriteError};
use influxdb3_types::DatabaseName;
use influxdb3_write::{Bufferer, Precision};
use iox_time::Time;
use std::sync::Arc;

/// [`WriteEndpoint`] that directly forwards writes to a [`Bufferer`].
#[derive(Debug)]
pub struct InProcessWriteEndpoint {
    buffer: Arc<dyn Bufferer>,
}

impl InProcessWriteEndpoint {
    pub fn new(buffer: Arc<dyn Bufferer>) -> Self {
        Self { buffer }
    }
}

#[async_trait]
impl WriteEndpoint for InProcessWriteEndpoint {
    async fn write_lp(
        &self,
        database: DatabaseName,
        lp: &str,
        ingest_time: Time,
        no_sync: bool,
    ) -> Result<(), WriteError> {
        self.buffer
            .write_lp(
                database,
                lp,
                ingest_time,
                false,
                Precision::Nanosecond,
                no_sync,
            )
            .await
            .map(|_| ())
            .map_err(|e| WriteError::Fail(Box::new(e)))
    }
}
