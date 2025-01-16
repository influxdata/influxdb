#[derive(Debug, thiserror::Error)]
pub enum ExecutePluginError {
    #[error("the process_writes function is not present in the plugin. Should be defined as: process_writes(influxdb3_local, table_batches, args=None)")]
    MissingProcessWritesFunction,

    #[error("{0}")]
    PluginError(#[from] anyhow::Error),
}

#[cfg(feature = "system-py")]
pub mod system_py;
