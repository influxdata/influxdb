use async_trait::async_trait;
use hashbrown::HashMap;
use influxdb3_catalog::catalog::INTERNAL_DB_NAME;
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
        target: WriteTarget,
        lp: &str,
        _ingest_time: Time,
        _no_sync: bool,
    ) -> Result<(), WriteError> {
        match target {
            WriteTarget::User(database) if database.as_str() == INTERNAL_DB_NAME => {
                Err(WriteError::InternalWriteUnsupported)
            }
            WriteTarget::User(database) => {
                self.push(database.as_str().to_owned(), lp.to_owned());
                Ok(())
            }
            WriteTarget::Internal => Err(WriteError::InternalWriteUnsupported),
        }
    }
}

#[derive(Debug, Error)]
pub enum WriteError {
    #[error("Cannot write: {0}")]
    Fail(Box<dyn std::error::Error + Send + Sync>),

    #[error("internal writes are not supported by the write accumulator")]
    InternalWriteUnsupported,
}

#[derive(Debug, Clone)]
pub enum WriteTarget {
    /// Write into a user database.
    User(DatabaseName),
    /// Write into the one system-managed internal database.
    Internal,
}

#[async_trait]
pub trait WriteEndpoint: std::fmt::Debug + Send + Sync + 'static {
    async fn write_lp(
        &self,
        target: WriteTarget,
        lp: &str,
        ingest_time: Time,
        no_sync: bool,
    ) -> Result<(), WriteError>;
}

#[cfg(test)]
mod tests;
