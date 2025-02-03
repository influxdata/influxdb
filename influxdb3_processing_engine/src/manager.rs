use crate::environment::{PluginEnvironmentError, PythonEnvironmentManager};
use bytes::Bytes;
use hashbrown::HashMap;
use hyper::{Body, Response};
use influxdb3_client::plugin_development::{
    SchedulePluginTestRequest, SchedulePluginTestResponse, WalPluginTestRequest,
    WalPluginTestResponse,
};
use influxdb3_internal_api::query_executor::QueryExecutor;
use influxdb3_wal::TriggerSpecificationDefinition;
use influxdb3_write::WriteBuffer;
use miette::Diagnostic;
use std::fmt::Debug;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Diagnostic, Error)]
#[diagnostic(
    code(influxdb3_processing_engine::manager),
    url("https://github.com/influxdata/influxdb/issues/new?template=bug_report.md")
)]
pub enum ProcessingEngineError {
    #[error("database not found: {0}")]
    DatabaseNotFound(String),

    #[error("catalog update error")]
    CatalogUpdateError(#[from] influxdb3_catalog::catalog::Error),

    #[error("write buffer error")]
    WriteBufferError(#[from] influxdb3_write::write_buffer::Error),

    #[error("wal error")]
    WalError(#[from] influxdb3_wal::Error),

    #[error("server not started with --plugin-dir")]
    PluginDirNotSet,

    #[error("plugin not found: {0}")]
    PluginNotFound(String),

    #[error("plugin error")]
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

    #[error("error installing python packages")]
    PythonPackageError(#[from] PluginEnvironmentError),
}

/// `[ProcessingEngineManager]` is used to interact with the processing engine,
/// in particular plugins and triggers.
///
#[async_trait::async_trait]
pub trait ProcessingEngineManager: Debug + Send + Sync + 'static {
    async fn insert_trigger(
        &self,
        db_name: &str,
        trigger_name: String,
        plugin_filename: String,
        trigger_specification: TriggerSpecificationDefinition,
        trigger_arguments: Option<HashMap<String, String>>,
        disabled: bool,
    ) -> Result<(), ProcessingEngineError>;

    async fn delete_trigger(
        &self,
        db_name: &str,
        trigger_name: &str,
        force: bool,
    ) -> Result<(), ProcessingEngineError>;

    /// Starts running the trigger, which will run in the background.
    async fn run_trigger(
        &self,
        write_buffer: Arc<dyn WriteBuffer>,
        query_executor: Arc<dyn QueryExecutor>,
        db_name: &str,
        trigger_name: &str,
    ) -> Result<(), ProcessingEngineError>;

    async fn disable_trigger(
        &self,
        db_name: &str,
        trigger_name: &str,
    ) -> Result<(), ProcessingEngineError>;

    async fn enable_trigger(
        &self,
        write_buffer: Arc<dyn WriteBuffer>,
        query_executor: Arc<dyn QueryExecutor>,
        db_name: &str,
        trigger_name: &str,
    ) -> Result<(), ProcessingEngineError>;

    async fn start_triggers(&self) -> Result<(), ProcessingEngineError>;

    async fn test_wal_plugin(
        &self,
        request: WalPluginTestRequest,
        query_executor: Arc<dyn QueryExecutor>,
    ) -> Result<WalPluginTestResponse, crate::plugins::PluginError>;

    async fn test_schedule_plugin(
        &self,
        request: SchedulePluginTestRequest,
        query_executor: Arc<dyn QueryExecutor>,
    ) -> Result<SchedulePluginTestResponse, crate::plugins::PluginError>;

    async fn request_trigger(
        &self,
        trigger_path: &str,
        query_params: HashMap<String, String>,
        request_headers: HashMap<String, String>,
        request_body: Bytes,
    ) -> Result<Response<Body>, ProcessingEngineError>;

    fn get_environment_manager(&self) -> Arc<dyn PythonEnvironmentManager>;
}
