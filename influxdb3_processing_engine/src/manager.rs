use crate::environment::PluginEnvironmentError;
use influxdb3_catalog::CatalogError;
use influxdb3_id::{DbId, TriggerId};
use std::fmt::Debug;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProcessingEngineError {
    #[error("database not found: {0}")]
    DatabaseNotFound(DbId),

    #[error("catalog update error: {0}")]
    CatalogUpdateError(#[from] CatalogError),

    #[error("write buffer error: {0}")]
    WriteBufferError(#[from] influxdb3_write::write_buffer::Error),

    #[error("wal error: {0}")]
    WalError(#[from] influxdb3_wal::Error),

    #[error("plugin not found: {0}")]
    PluginNotFound(String),

    #[error("plugin error: {0}")]
    PluginError(#[from] crate::plugins::PluginError),

    #[error("failed to shutdown trigger {trigger_id} in database {database_id}")]
    TriggerShutdownError {
        database_id: DbId,
        trigger_id: TriggerId,
    },

    #[error("request trigger not found")]
    RequestTriggerNotFound,

    #[error("request handler for trigger down")]
    RequestHandlerDown,

    #[error("error installing python packages: {0}")]
    PythonPackageError(#[from] PluginEnvironmentError),

    #[error("trigger not found: {0}")]
    TriggerNotFound(TriggerId),
}
