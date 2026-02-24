//! The Last-N-Value cache holds the most N recent values for a column or set of columns on a table

use influxdb3_id::ColumnId;

mod cache;
pub use cache::CreateLastCacheArgs;
mod metrics;
mod provider;
pub use provider::{LastCacheProvider, background_catalog_update};
mod table_function;
use schema::InfluxColumnType;
pub use table_function::{LAST_CACHE_UDTF_NAME, LastCacheFunction};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid cache size")]
    InvalidCacheSize,
    #[error(
        "last cache already exists for database and table, but it was configured differently: {reason}"
    )]
    CacheAlreadyExists { reason: String },
    #[error("specified column (name: {column_name}) does not exist in the table definition")]
    ColumnDoesNotExistByName { column_name: String },
    #[error("specified column (id: {column_id}) does not exist in the table definition")]
    ColumnDoesNotExistById { column_id: ColumnId },
    #[error("specified key column (id: {column_id}) does not exist in the table schema")]
    KeyColumnDoesNotExist { column_id: ColumnId },
    #[error("specified key column (name: {column_name}) does not exist in the table schema")]
    KeyColumnDoesNotExistByName { column_name: String },
    #[error("key column must be string, int, uint, or bool types, got: {column_type}")]
    InvalidKeyColumn { column_type: InfluxColumnType },
    #[error("specified value column ({column_id}) does not exist in the table schema")]
    ValueColumnDoesNotExist { column_id: ColumnId },
    #[error("requested last cache does not exist")]
    CacheDoesNotExist,
}

impl Error {
    fn cache_already_exists(reason: impl Into<String>) -> Self {
        Self::CacheAlreadyExists {
            reason: reason.into(),
        }
    }
}

#[cfg(test)]
mod tests;
