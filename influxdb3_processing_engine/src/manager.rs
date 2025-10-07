use crate::environment::PluginEnvironmentError;
use influxdb3_catalog::CatalogError;
use std::fmt::Debug;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProcessingEngineError {
    #[error("database not found: {0}")]
    DatabaseNotFound(String),

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

    #[error("failed to shutdown trigger {trigger_name} in database {database}")]
    TriggerShutdownError {
        database: String,
        trigger_name: String,
    },

    #[error("request trigger not found")]
    RequestTriggerNotFound,

    #[error("request handler for trigger down")]
    RequestHandlerDown,

    #[error("error installing python packages: {0}")]
    PythonPackageError(PluginEnvironmentError),

    #[error("{0}")]
    PackageInstallationDisabled(PluginEnvironmentError),
}

impl From<PluginEnvironmentError> for ProcessingEngineError {
    fn from(err: PluginEnvironmentError) -> Self {
        match err {
            PluginEnvironmentError::PackageInstallationDisabled => {
                ProcessingEngineError::PackageInstallationDisabled(err)
            }
            other => ProcessingEngineError::PythonPackageError(other),
        }
    }
}
