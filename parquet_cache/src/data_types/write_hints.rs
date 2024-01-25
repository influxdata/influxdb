use data_types::{ParquetFile, ParquetFileParams};
use serde::{Deserialize, Serialize};

use super::ObjectParams;

/// Request payload provided on WriteHinting.
#[derive(Debug, Serialize, Deserialize)]
pub struct WriteHintRequestBody {
    /// Object store [`Path`](object_store::path::Path) converted to cache key.
    pub location: String,
    /// The actual [`WriteHint`].
    pub hint: WriteHint,
    /// Requested server contract to fulfill prior to ACK.
    pub ack_setting: WriteHintAck,
}

/// DataCache is a read-only, write-hinting service.
///
/// Cache writes to store, then hints to pull into cache.
/// Return ok based upon a configurable level of cache server ack.
#[derive(Debug, Default, Copy, Clone, Serialize, Deserialize)]
pub enum WriteHintAck {
    /// cache client sent write hint
    Sent,
    /// cache server received write hint
    Received,
    /// cache server completed downstream action
    #[default]
    Completed,
}

impl std::fmt::Display for WriteHintAck {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// Write hint metadata provided by the client.
pub type WriteHint = ObjectParams;

impl From<&ParquetFileParams> for WriteHint {
    fn from(value: &ParquetFileParams) -> Self {
        let ParquetFileParams {
            namespace_id,
            table_id,
            min_time,
            max_time,
            file_size_bytes,
            ..
        } = value;

        Self {
            namespace_id: namespace_id.get(),
            table_id: table_id.get(),
            min_time: min_time.get(),
            max_time: max_time.get(),
            file_size_bytes: file_size_bytes.to_owned(),
        }
    }
}

impl From<&ParquetFile> for WriteHint {
    fn from(value: &ParquetFile) -> Self {
        let ParquetFile {
            namespace_id,
            table_id,
            min_time,
            max_time,
            file_size_bytes,
            ..
        } = value;

        Self {
            namespace_id: namespace_id.get(),
            table_id: table_id.get(),
            min_time: min_time.get(),
            max_time: max_time.get(),
            file_size_bytes: file_size_bytes.to_owned(),
        }
    }
}
