use async_trait::async_trait;
use hashbrown::HashMap;
use influxdb3_types::DatabaseName;
use iox_time::Time;
use parking_lot::Mutex;
use thiserror::Error;

/// Accumulates batched write operations during plugin execution.
///
/// See [PluginReturnState](crate::system_py::PluginReturnState).
#[derive(Debug, Default)]
pub struct WriteAccumulator {
    /// Line protocol to write to databases, keyed by database name.
    write_db_lines: Mutex<HashMap<String, Vec<String>>>,
}

impl WriteAccumulator {
    /// Push writes.
    pub fn push(&self, db: String, line: String) {
        self.write_db_lines.lock().entry(db).or_default().push(line);
    }

    /// Flush content.
    pub fn flush(&self) -> HashMap<String, Vec<String>> {
        std::mem::take(&mut self.write_db_lines.lock())
    }
}

#[async_trait]
impl WriteEndpoint for WriteAccumulator {
    async fn write_lp(
        &self,
        database: DatabaseName,
        lp: &str,
        _ingest_time: Time,
        _no_sync: bool,
    ) -> Result<(), WriteError> {
        self.push(database.as_str().to_owned(), lp.to_owned());
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum WriteError {
    #[error("Cannot write: {0}")]
    Fail(Box<dyn std::error::Error + Send + Sync>),
}

#[async_trait]
pub trait WriteEndpoint: std::fmt::Debug + Send + Sync + 'static {
    async fn write_lp(
        &self,
        database: DatabaseName,
        lp: &str,
        ingest_time: Time,
        no_sync: bool,
    ) -> Result<(), WriteError>;
}
