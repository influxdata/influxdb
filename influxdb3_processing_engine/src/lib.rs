use crate::environment::PythonEnvironmentManager;
use crate::manager::{ProcessingEngineError, ProcessingEngineManager};
#[cfg(feature = "system-py")]
use crate::plugins::PluginContext;
use crate::plugins::{PluginError, ProcessingEngineEnvironmentManager};
use anyhow::Context;
use bytes::Bytes;
use hashbrown::HashMap;
use hyper::{Body, Response};
use influxdb3_catalog::catalog::Catalog;
use influxdb3_catalog::catalog::Error::ProcessingEngineTriggerNotFound;
use influxdb3_client::plugin_development::{
    SchedulePluginTestRequest, SchedulePluginTestResponse, WalPluginTestRequest,
    WalPluginTestResponse,
};
use influxdb3_internal_api::query_executor::QueryExecutor;
#[cfg(feature = "system-py")]
use influxdb3_wal::PluginType;
use influxdb3_wal::{
    CatalogBatch, CatalogOp, DeleteTriggerDefinition, SnapshotDetails, TriggerDefinition,
    TriggerIdentifier, TriggerSpecificationDefinition, Wal, WalContents, WalFileNotifier, WalOp,
};
use influxdb3_write::WriteBuffer;
use iox_time::TimeProvider;
use observability_deps::tracing::{debug, error, warn};
use std::any::Any;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use tokio::sync::oneshot::Receiver;
use tokio::sync::{mpsc, oneshot, RwLock};

pub mod environment;
pub mod manager;
pub mod plugins;

#[cfg(feature = "system-py")]
pub mod virtualenv;

#[derive(Debug)]
pub struct ProcessingEngineManagerImpl {
    environment_manager: ProcessingEngineEnvironmentManager,
    catalog: Arc<Catalog>,
    write_buffer: Arc<dyn WriteBuffer>,
    query_executor: Arc<dyn QueryExecutor>,
    time_provider: Arc<dyn TimeProvider>,
    wal: Arc<dyn Wal>,
    plugin_event_tx: RwLock<PluginChannels>,
}

#[derive(Debug, Default)]
struct PluginChannels {
    /// Map of database to wal trigger name to handler
    wal_triggers: HashMap<String, HashMap<String, mpsc::Sender<WalEvent>>>,
    /// Map of database to schedule trigger name to handler
    schedule_triggers: HashMap<String, HashMap<String, mpsc::Sender<ScheduleEvent>>>,
    /// Map of request path to the request trigger handler
    request_triggers: HashMap<String, mpsc::Sender<RequestEvent>>,
}

#[cfg(feature = "system-py")]
const PLUGIN_EVENT_BUFFER_SIZE: usize = 60;

impl PluginChannels {
    // returns Ok(Some(receiver)) if there was a sender to the named trigger.
    async fn send_shutdown(
        &self,
        db: String,
        trigger: String,
        trigger_spec: &TriggerSpecificationDefinition,
    ) -> Result<Option<Receiver<()>>, ProcessingEngineError> {
        match trigger_spec {
            TriggerSpecificationDefinition::SingleTableWalWrite { .. }
            | TriggerSpecificationDefinition::AllTablesWalWrite => {
                if let Some(trigger_map) = self.wal_triggers.get(&db) {
                    if let Some(sender) = trigger_map.get(&trigger) {
                        // create a one shot to wait for the shutdown to complete
                        let (tx, rx) = oneshot::channel();
                        if sender.send(WalEvent::Shutdown(tx)).await.is_err() {
                            return Err(ProcessingEngineError::TriggerShutdownError {
                                database: db,
                                trigger_name: trigger,
                            });
                        }
                        return Ok(Some(rx));
                    }
                }
            }
            TriggerSpecificationDefinition::Schedule { .. }
            | TriggerSpecificationDefinition::Every { .. } => {
                if let Some(trigger_map) = self.schedule_triggers.get(&db) {
                    if let Some(sender) = trigger_map.get(&trigger) {
                        // create a one shot to wait for the shutdown to complete
                        let (tx, rx) = oneshot::channel();
                        if sender.send(ScheduleEvent::Shutdown(tx)).await.is_err() {
                            return Err(ProcessingEngineError::TriggerShutdownError {
                                database: db,
                                trigger_name: trigger,
                            });
                        }
                        return Ok(Some(rx));
                    }
                }
            }
            TriggerSpecificationDefinition::RequestPath { .. } => {
                if let Some(sender) = self.request_triggers.get(&trigger) {
                    // create a one shot to wait for the shutdown to complete
                    let (tx, rx) = oneshot::channel();
                    if sender.send(RequestEvent::Shutdown(tx)).await.is_err() {
                        return Err(ProcessingEngineError::TriggerShutdownError {
                            database: db,
                            trigger_name: trigger,
                        });
                    }
                    return Ok(Some(rx));
                }
            }
        }

        Ok(None)
    }

    fn remove_trigger(
        &mut self,
        db: String,
        trigger: String,
        trigger_spec: &TriggerSpecificationDefinition,
    ) {
        match trigger_spec {
            TriggerSpecificationDefinition::SingleTableWalWrite { .. }
            | TriggerSpecificationDefinition::AllTablesWalWrite => {
                if let Some(trigger_map) = self.wal_triggers.get_mut(&db) {
                    trigger_map.remove(&trigger);
                }
            }
            TriggerSpecificationDefinition::Schedule { .. }
            | TriggerSpecificationDefinition::Every { .. } => {
                if let Some(trigger_map) = self.schedule_triggers.get_mut(&db) {
                    trigger_map.remove(&trigger);
                }
            }
            TriggerSpecificationDefinition::RequestPath { .. } => {
                self.request_triggers.remove(&trigger);
            }
        }
    }

    #[cfg(feature = "system-py")]
    fn add_wal_trigger(&mut self, db: String, trigger: String) -> mpsc::Receiver<WalEvent> {
        let (tx, rx) = mpsc::channel(PLUGIN_EVENT_BUFFER_SIZE);
        self.wal_triggers.entry(db).or_default().insert(trigger, tx);
        rx
    }

    #[cfg(feature = "system-py")]
    fn add_schedule_trigger(
        &mut self,
        db: String,
        trigger: String,
    ) -> mpsc::Receiver<ScheduleEvent> {
        let (tx, rx) = mpsc::channel(PLUGIN_EVENT_BUFFER_SIZE);
        self.schedule_triggers
            .entry(db)
            .or_default()
            .insert(trigger, tx);
        rx
    }

    #[cfg(feature = "system-py")]
    fn add_request_trigger(&mut self, path: String) -> mpsc::Receiver<RequestEvent> {
        let (tx, rx) = mpsc::channel(PLUGIN_EVENT_BUFFER_SIZE);
        self.request_triggers.insert(path, tx);
        rx
    }

    async fn send_wal_contents(&self, wal_contents: Arc<WalContents>) {
        for (db, trigger_map) in &self.wal_triggers {
            for (trigger, sender) in trigger_map {
                if let Err(e) = sender
                    .send(WalEvent::WriteWalContents(Arc::clone(&wal_contents)))
                    .await
                {
                    warn!(%e, %db, ?trigger, "error sending wal contents to plugin");
                }
            }
        }
    }

    async fn send_request(
        &self,
        trigger_path: &str,
        request: Request,
    ) -> Result<(), ProcessingEngineError> {
        let event = RequestEvent::Request(request);
        if let Some(sender) = self.request_triggers.get(trigger_path) {
            if sender.send(event).await.is_err() {
                return Err(ProcessingEngineError::RequestTriggerNotFound);
            }
        } else {
            return Err(ProcessingEngineError::RequestTriggerNotFound);
        }

        Ok(())
    }
}

impl ProcessingEngineManagerImpl {
    pub fn new(
        environment: ProcessingEngineEnvironmentManager,
        catalog: Arc<Catalog>,
        write_buffer: Arc<dyn WriteBuffer>,
        query_executor: Arc<dyn QueryExecutor>,
        time_provider: Arc<dyn TimeProvider>,
        wal: Arc<dyn Wal>,
    ) -> Self {
        // if given a plugin dir, try to initialize the virtualenv.
        #[cfg(feature = "system-py")]
        if let Some(plugin_dir) = &environment.plugin_dir {
            {
                environment
                    .package_manager
                    .init_pyenv(plugin_dir, environment.virtual_env_location.as_ref())
                    .expect("unable to initialize python environment");
                virtualenv::init_pyo3();
            }
        }
        Self {
            environment_manager: environment,
            catalog,
            write_buffer,
            query_executor,
            time_provider,
            wal,
            plugin_event_tx: Default::default(),
        }
    }

    pub async fn read_plugin_code(&self, name: &str) -> Result<PluginCode, plugins::PluginError> {
        // if the name starts with gh: then we need to get it from the public github repo at https://github.com/influxdata/influxdb3_plugins/tree/main
        if name.starts_with("gh:") {
            let plugin_path = name.strip_prefix("gh:").unwrap();
            let url = format!(
                "https://raw.githubusercontent.com/influxdata/influxdb3_plugins/main/{}",
                plugin_path
            );
            let resp = reqwest::get(&url)
                .await
                .context("error getting plugin from github repo")?;

            // verify the response is a success
            if !resp.status().is_success() {
                return Err(PluginError::FetchingFromGithub(resp.status(), url));
            }

            let resp_body = resp
                .text()
                .await
                .context("error reading plugin from github repo")?;
            return Ok(PluginCode::Github(Arc::from(resp_body)));
        }

        // otherwise we assume it is a local file
        let plugin_dir = self
            .environment_manager
            .plugin_dir
            .clone()
            .context("plugin dir not set")?;
        let plugin_path = plugin_dir.join(name);

        // read it at least once to make sure it's there
        let code = std::fs::read_to_string(plugin_path.clone())?;

        // now we can return it
        Ok(PluginCode::Local(LocalPlugin {
            plugin_path,
            last_read_and_code: Mutex::new((SystemTime::now(), Arc::from(code))),
        }))
    }
}

#[derive(Debug)]
pub enum PluginCode {
    Github(Arc<str>),
    Local(LocalPlugin),
}

impl PluginCode {
    #[cfg(feature = "system-py")]
    pub(crate) fn code(&self) -> Arc<str> {
        match self {
            PluginCode::Github(code) => Arc::clone(code),
            PluginCode::Local(plugin) => plugin.read_if_modified(),
        }
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct LocalPlugin {
    plugin_path: PathBuf,
    last_read_and_code: Mutex<(SystemTime, Arc<str>)>,
}

impl LocalPlugin {
    #[cfg(feature = "system-py")]
    fn read_if_modified(&self) -> Arc<str> {
        let metadata = std::fs::metadata(&self.plugin_path);

        let mut last_read_and_code = self.last_read_and_code.lock().unwrap();
        let (last_read, code) = &mut *last_read_and_code;

        match metadata {
            Ok(metadata) => {
                let is_modified = match metadata.modified() {
                    Ok(modified) => modified > *last_read,
                    Err(_) => true, // if we can't get the modified time, assume it is modified
                };

                if is_modified {
                    // attempt to read the code, if it fails we will return the last known code
                    if let Ok(new_code) = std::fs::read_to_string(&self.plugin_path) {
                        *last_read = SystemTime::now();
                        *code = Arc::from(new_code);
                    } else {
                        error!("error reading plugin file {:?}", self.plugin_path);
                    }
                }

                Arc::clone(code)
            }
            Err(_) => Arc::clone(code),
        }
    }
}

#[async_trait::async_trait]
impl ProcessingEngineManager for ProcessingEngineManagerImpl {
    async fn insert_trigger(
        &self,
        db_name: &str,
        trigger_name: String,
        plugin_filename: String,
        trigger_specification: TriggerSpecificationDefinition,
        trigger_arguments: Option<HashMap<String, String>>,
        disabled: bool,
    ) -> Result<(), ProcessingEngineError> {
        let Some((db_id, db_schema)) = self.catalog.db_id_and_schema(db_name) else {
            return Err(ProcessingEngineError::DatabaseNotFound(db_name.to_string()));
        };

        // validate that we can actually read the plugin file
        self.read_plugin_code(&plugin_filename).await?;

        let catalog_op = CatalogOp::CreateTrigger(TriggerDefinition {
            trigger_name,
            plugin_filename,
            trigger: trigger_specification,
            trigger_arguments,
            disabled,
            database_name: db_name.to_string(),
        });
        let creation_time = self.time_provider.now();
        let catalog_batch = CatalogBatch {
            time_ns: creation_time.timestamp_nanos(),
            database_id: db_id,
            database_name: Arc::clone(&db_schema.name),
            ops: vec![catalog_op],
        };
        if let Some(catalog_batch) = self.catalog.apply_catalog_batch(&catalog_batch)? {
            let wal_op = WalOp::Catalog(catalog_batch);
            self.wal.write_ops(vec![wal_op]).await?;
        }
        Ok(())
    }

    async fn delete_trigger(
        &self,
        db: &str,
        trigger_name: &str,
        force: bool,
    ) -> Result<(), ProcessingEngineError> {
        let (db_id, db_schema) = self
            .catalog
            .db_id_and_schema(db)
            .ok_or_else(|| ProcessingEngineError::DatabaseNotFound(db.to_string()))?;
        let catalog_op = CatalogOp::DeleteTrigger(DeleteTriggerDefinition {
            trigger_name: trigger_name.to_string(),
            force,
        });
        let catalog_batch = CatalogBatch {
            time_ns: self.time_provider.now().timestamp_nanos(),
            database_id: db_id,
            database_name: Arc::clone(&db_schema.name),
            ops: vec![catalog_op],
        };

        // Do this first to avoid a dangling running plugin.
        // Potential edge-case of a plugin being stopped but not deleted,
        // but should be okay given desire to force delete.
        let needs_disable = force
            && db_schema
                .processing_engine_triggers
                .get(trigger_name)
                .is_some_and(|trigger| !trigger.disabled);

        if needs_disable {
            self.disable_trigger(db, trigger_name).await?;
        }

        if let Some(catalog_batch) = self.catalog.apply_catalog_batch(&catalog_batch)? {
            self.wal
                .write_ops(vec![WalOp::Catalog(catalog_batch)])
                .await?;
        }
        Ok(())
    }

    #[cfg_attr(not(feature = "system-py"), allow(unused))]
    async fn run_trigger(
        &self,
        write_buffer: Arc<dyn WriteBuffer>,
        query_executor: Arc<dyn QueryExecutor>,
        db_name: &str,
        trigger_name: &str,
    ) -> Result<(), ProcessingEngineError> {
        debug!(db_name, trigger_name, "starting trigger");
        #[cfg(feature = "system-py")]
        {
            let db_schema = self
                .catalog
                .db_schema(db_name)
                .ok_or_else(|| ProcessingEngineError::DatabaseNotFound(db_name.to_string()))?;
            let trigger = db_schema
                .processing_engine_triggers
                .get(trigger_name)
                .ok_or_else(|| ProcessingEngineTriggerNotFound {
                    database_name: db_name.to_string(),
                    trigger_name: trigger_name.to_string(),
                })?
                .clone();

            let plugin_context = PluginContext {
                write_buffer,
                query_executor,
            };
            let plugin_code = self.read_plugin_code(&trigger.plugin_filename).await?;
            match trigger.trigger.plugin_type() {
                PluginType::WalRows => {
                    let rec = self
                        .plugin_event_tx
                        .write()
                        .await
                        .add_wal_trigger(db_name.to_string(), trigger_name.to_string());

                    plugins::run_wal_contents_plugin(
                        db_name.to_string(),
                        plugin_code,
                        trigger,
                        plugin_context,
                        rec,
                    )
                }
                PluginType::Schedule => {
                    let rec = self
                        .plugin_event_tx
                        .write()
                        .await
                        .add_schedule_trigger(db_name.to_string(), trigger_name.to_string());

                    plugins::run_schedule_plugin(
                        db_name.to_string(),
                        plugin_code,
                        trigger,
                        Arc::clone(&self.time_provider),
                        plugin_context,
                        rec,
                    )?
                }
                PluginType::Request => {
                    let TriggerSpecificationDefinition::RequestPath { path } = &trigger.trigger
                    else {
                        unreachable!()
                    };
                    let rec = self
                        .plugin_event_tx
                        .write()
                        .await
                        .add_request_trigger(path.to_string());

                    plugins::run_request_plugin(
                        db_name.to_string(),
                        plugin_code,
                        trigger,
                        plugin_context,
                        rec,
                    )
                }
            }
        }

        Ok(())
    }

    async fn disable_trigger(
        &self,
        db_name: &str,
        trigger_name: &str,
    ) -> Result<(), ProcessingEngineError> {
        let (db_id, db_schema) = self
            .catalog
            .db_id_and_schema(db_name)
            .ok_or_else(|| ProcessingEngineError::DatabaseNotFound(db_name.to_string()))?;
        let trigger = db_schema
            .processing_engine_triggers
            .get(trigger_name)
            .ok_or_else(|| ProcessingEngineTriggerNotFound {
                database_name: db_name.to_string(),
                trigger_name: trigger_name.to_string(),
            })?;
        // Already disabled, so this is a no-op
        if trigger.disabled {
            return Ok(());
        };

        let catalog_op = CatalogOp::DisableTrigger(TriggerIdentifier {
            db_name: db_name.to_string(),
            trigger_name: trigger_name.to_string(),
        });
        if let Some(catalog_batch) = self.catalog.apply_catalog_batch(&CatalogBatch {
            database_id: db_id,
            database_name: Arc::clone(&db_schema.name),
            time_ns: self.time_provider.now().timestamp_nanos(),
            ops: vec![catalog_op],
        })? {
            let wal_op = WalOp::Catalog(catalog_batch);
            self.wal.write_ops(vec![wal_op]).await?;
        }

        let Some(shutdown_rx) = self
            .plugin_event_tx
            .write()
            .await
            .send_shutdown(
                db_name.to_string(),
                trigger_name.to_string(),
                &trigger.trigger,
            )
            .await?
        else {
            return Ok(());
        };

        if shutdown_rx.await.is_err() {
            warn!(
                "shutdown trigger receiver dropped, may have received multiple shutdown requests"
            );
        } else {
            self.plugin_event_tx.write().await.remove_trigger(
                db_name.to_string(),
                trigger_name.to_string(),
                &trigger.trigger,
            );
        }

        Ok(())
    }

    async fn enable_trigger(
        &self,
        write_buffer: Arc<dyn WriteBuffer>,
        query_executor: Arc<dyn QueryExecutor>,
        db_name: &str,
        trigger_name: &str,
    ) -> Result<(), ProcessingEngineError> {
        let (db_id, db_schema) = self
            .catalog
            .db_id_and_schema(db_name)
            .ok_or_else(|| ProcessingEngineError::DatabaseNotFound(db_name.to_string()))?;
        let trigger = db_schema
            .processing_engine_triggers
            .get(trigger_name)
            .ok_or_else(|| ProcessingEngineTriggerNotFound {
                database_name: db_name.to_string(),
                trigger_name: trigger_name.to_string(),
            })?;
        // Already enabled, so this is a no-op
        if !trigger.disabled {
            return Ok(());
        };

        let catalog_op = CatalogOp::EnableTrigger(TriggerIdentifier {
            db_name: db_name.to_string(),
            trigger_name: trigger_name.to_string(),
        });
        if let Some(catalog_batch) = self.catalog.apply_catalog_batch(&CatalogBatch {
            database_id: db_id,
            database_name: Arc::clone(&db_schema.name),
            time_ns: self.time_provider.now().timestamp_nanos(),
            ops: vec![catalog_op],
        })? {
            let wal_op = WalOp::Catalog(catalog_batch);
            self.wal.write_ops(vec![wal_op]).await?;
        }

        self.run_trigger(write_buffer, query_executor, db_name, trigger_name)
            .await?;
        Ok(())
    }

    async fn start_triggers(&self) -> Result<(), ProcessingEngineError> {
        let triggers = self.catalog.active_triggers();
        for (db_name, trigger_name) in triggers {
            self.run_trigger(
                Arc::clone(&self.write_buffer),
                Arc::clone(&self.query_executor),
                &db_name,
                &trigger_name,
            )
            .await?;
        }
        Ok(())
    }

    #[cfg_attr(not(feature = "system-py"), allow(unused))]
    async fn test_wal_plugin(
        &self,
        request: WalPluginTestRequest,
        query_executor: Arc<dyn QueryExecutor>,
    ) -> Result<WalPluginTestResponse, plugins::PluginError> {
        #[cfg(feature = "system-py")]
        {
            // create a copy of the catalog so we don't modify the original
            let catalog = Arc::new(Catalog::from_inner(self.catalog.clone_inner()));
            let now = self.time_provider.now();

            let code = self.read_plugin_code(&request.filename).await?;
            let code_string = code.code().to_string();

            let res =
                plugins::run_test_wal_plugin(now, catalog, query_executor, code_string, request)
                    .unwrap_or_else(|e| WalPluginTestResponse {
                        log_lines: vec![],
                        database_writes: Default::default(),
                        errors: vec![e.to_string()],
                    });

            return Ok(res);
        }

        #[cfg(not(feature = "system-py"))]
        Err(plugins::PluginError::AnyhowError(anyhow::anyhow!(
            "system-py feature not enabled"
        )))
    }

    #[cfg_attr(not(feature = "system-py"), allow(unused))]
    async fn test_schedule_plugin(
        &self,
        request: SchedulePluginTestRequest,
        query_executor: Arc<dyn QueryExecutor>,
    ) -> Result<SchedulePluginTestResponse, PluginError> {
        #[cfg(feature = "system-py")]
        {
            // create a copy of the catalog so we don't modify the original
            let catalog = Arc::new(Catalog::from_inner(self.catalog.clone_inner()));
            let now = self.time_provider.now();

            let code = self.read_plugin_code(&request.filename).await?;
            let code_string = code.code().to_string();

            let res = plugins::run_test_schedule_plugin(
                now,
                catalog,
                query_executor,
                code_string,
                request,
            )
            .unwrap_or_else(|e| SchedulePluginTestResponse {
                log_lines: vec![],
                database_writes: Default::default(),
                errors: vec![e.to_string()],
                trigger_time: None,
            });

            return Ok(res);
        }

        #[cfg(not(feature = "system-py"))]
        Err(plugins::PluginError::AnyhowError(anyhow::anyhow!(
            "system-py feature not enabled"
        )))
    }

    async fn request_trigger(
        &self,
        trigger_path: &str,
        query_params: HashMap<String, String>,
        request_headers: HashMap<String, String>,
        request_body: Bytes,
    ) -> Result<Response<Body>, ProcessingEngineError> {
        // oneshot channel for the response
        let (tx, rx) = oneshot::channel();
        let request = Request {
            query_params,
            headers: request_headers,
            body: request_body,
            response_tx: tx,
        };

        self.plugin_event_tx
            .write()
            .await
            .send_request(trigger_path, request)
            .await?;

        Ok(rx.await.map_err(|e| {
            error!(%e, "error receiving response from plugin");
            ProcessingEngineError::RequestHandlerDown
        })?)
    }

    fn get_environment_manager(&self) -> Arc<dyn PythonEnvironmentManager> {
        Arc::clone(&self.environment_manager.package_manager)
    }
}

#[async_trait::async_trait]
impl WalFileNotifier for ProcessingEngineManagerImpl {
    async fn notify(&self, write: Arc<WalContents>) {
        let plugin_channels = self.plugin_event_tx.read().await;
        plugin_channels.send_wal_contents(write).await;
    }

    async fn notify_and_snapshot(
        &self,
        write: Arc<WalContents>,
        snapshot_details: SnapshotDetails,
    ) -> Receiver<SnapshotDetails> {
        let plugin_channels = self.plugin_event_tx.read().await;
        plugin_channels.send_wal_contents(write).await;

        // configure a reciever that we immediately close
        let (tx, rx) = oneshot::channel();
        tx.send(snapshot_details).ok();
        rx
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[allow(dead_code)]
pub(crate) enum WalEvent {
    WriteWalContents(Arc<WalContents>),
    Shutdown(oneshot::Sender<()>),
}

#[allow(dead_code)]
pub(crate) enum ScheduleEvent {
    Shutdown(oneshot::Sender<()>),
}

#[allow(dead_code)]
pub(crate) enum RequestEvent {
    Request(Request),
    Shutdown(oneshot::Sender<()>),
}

#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct Request {
    pub query_params: HashMap<String, String>,
    pub headers: HashMap<String, String>,
    pub body: Bytes,
    pub response_tx: oneshot::Sender<Response<Body>>,
}

#[cfg(test)]
mod tests {
    use crate::environment::DisabledManager;
    use crate::manager::{ProcessingEngineError, ProcessingEngineManager};
    use crate::plugins::ProcessingEngineEnvironmentManager;
    use crate::ProcessingEngineManagerImpl;
    use data_types::NamespaceName;
    use datafusion_util::config::register_iox_object_store;
    use influxdb3_cache::distinct_cache::DistinctCacheProvider;
    use influxdb3_cache::last_cache::LastCacheProvider;
    use influxdb3_catalog::catalog;
    use influxdb3_internal_api::query_executor::UnimplementedQueryExecutor;
    use influxdb3_wal::{Gen1Duration, TriggerSpecificationDefinition, WalConfig};
    use influxdb3_write::persister::Persister;
    use influxdb3_write::write_buffer::{WriteBufferImpl, WriteBufferImplArgs};
    use influxdb3_write::{Precision, WriteBuffer};
    use iox_query::exec::{DedicatedExecutor, Executor, ExecutorConfig, IOxSessionContext};
    use iox_time::{MockProvider, Time, TimeProvider};
    use metric::Registry;
    use object_store::memory::InMemory;
    use object_store::ObjectStore;
    use parquet_file::storage::{ParquetStorage, StorageId};
    use std::io::Write;
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_trigger_lifecycle() -> influxdb3_write::write_buffer::Result<()> {
        let start_time = Time::from_rfc3339("2024-11-14T11:00:00+00:00").unwrap();
        let test_store = Arc::new(InMemory::new());
        let wal_config = WalConfig {
            gen1_duration: Gen1Duration::new_1m(),
            max_write_buffer_size: 100,
            flush_interval: Duration::from_millis(10),
            snapshot_size: 1,
        };
        let (pem, file) = setup(start_time, test_store, wal_config).await;
        let file_name = file
            .path()
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

        // convert to Arc<WriteBuffer>
        let write_buffer: Arc<dyn WriteBuffer> = Arc::clone(&pem.write_buffer);

        // Create the DB by inserting a line.
        write_buffer
            .write_lp(
                NamespaceName::new("foo").unwrap(),
                "cpu,warehouse=us-east,room=01a,device=10001 reading=37\n",
                start_time,
                false,
                Precision::Nanosecond,
                false,
            )
            .await?;

        // Create an enabled trigger
        pem.insert_trigger(
            "foo",
            "test_trigger".to_string(),
            file_name,
            TriggerSpecificationDefinition::AllTablesWalWrite,
            None,
            false,
        )
        .await
        .unwrap();
        // Run the trigger
        pem.run_trigger(
            Arc::clone(&write_buffer),
            Arc::clone(&pem.query_executor),
            "foo",
            "test_trigger",
        )
        .await
        .unwrap();

        // Disable the trigger
        let result = pem.disable_trigger("foo", "test_trigger").await;
        assert!(result.is_ok());

        // Verify trigger is disabled in schema
        let schema = write_buffer.catalog().db_schema("foo").unwrap();
        let trigger = schema
            .processing_engine_triggers
            .get("test_trigger")
            .unwrap();
        assert!(trigger.disabled);

        // Enable the trigger
        let result = pem
            .enable_trigger(
                Arc::clone(&write_buffer),
                Arc::clone(&pem.query_executor),
                "foo",
                "test_trigger",
            )
            .await;
        assert!(result.is_ok());

        // Verify trigger is enabled and running
        let schema = write_buffer.catalog().db_schema("foo").unwrap();
        let trigger = schema
            .processing_engine_triggers
            .get("test_trigger")
            .unwrap();
        assert!(!trigger.disabled);
        Ok(())
    }

    #[tokio::test]
    async fn test_create_disabled_trigger() -> influxdb3_write::write_buffer::Result<()> {
        let start_time = Time::from_rfc3339("2024-11-14T11:00:00+00:00").unwrap();
        let test_store = Arc::new(InMemory::new());
        let wal_config = WalConfig {
            gen1_duration: Gen1Duration::new_1m(),
            max_write_buffer_size: 100,
            flush_interval: Duration::from_millis(10),
            snapshot_size: 1,
        };
        let (pem, file) = setup(start_time, test_store, wal_config).await;
        let file_name = file
            .path()
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

        // Create the DB by inserting a line.
        pem.write_buffer
            .write_lp(
                NamespaceName::new("foo").unwrap(),
                "cpu,warehouse=us-east,room=01a,device=10001 reading=37\n",
                start_time,
                false,
                Precision::Nanosecond,
                false,
            )
            .await?;

        // Create a disabled trigger
        pem.insert_trigger(
            "foo",
            "test_trigger".to_string(),
            file_name,
            TriggerSpecificationDefinition::AllTablesWalWrite,
            None,
            true,
        )
        .await
        .unwrap();

        // Verify trigger is created but disabled
        let schema = pem.catalog.db_schema("foo").unwrap();
        let trigger = schema
            .processing_engine_triggers
            .get("test_trigger")
            .unwrap();
        assert!(trigger.disabled);

        // Verify trigger is not in active triggers list
        assert!(pem.catalog.active_triggers().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_enable_nonexistent_trigger() -> influxdb3_write::write_buffer::Result<()> {
        let start_time = Time::from_rfc3339("2024-11-14T11:00:00+00:00").unwrap();
        let test_store = Arc::new(InMemory::new());
        let wal_config = WalConfig {
            gen1_duration: Gen1Duration::new_1m(),
            max_write_buffer_size: 100,
            flush_interval: Duration::from_millis(10),
            snapshot_size: 1,
        };
        let (pem, _file_name) = setup(start_time, test_store, wal_config).await;

        let write_buffer: Arc<dyn WriteBuffer> = Arc::clone(&pem.write_buffer);

        // Create the DB by inserting a line.
        write_buffer
            .write_lp(
                NamespaceName::new("foo").unwrap(),
                "cpu,warehouse=us-east,room=01a,device=10001 reading=37\n",
                start_time,
                false,
                Precision::Nanosecond,
                false,
            )
            .await?;

        let result = pem
            .enable_trigger(
                Arc::clone(&write_buffer),
                Arc::clone(&pem.query_executor),
                "foo",
                "nonexistent_trigger",
            )
            .await;

        assert!(matches!(
            result,
            Err(ProcessingEngineError::CatalogUpdateError(catalog::Error::ProcessingEngineTriggerNotFound {
                database_name,
                trigger_name,
            })) if database_name == "foo" && trigger_name == "nonexistent_trigger"
        ));
        Ok(())
    }

    async fn setup(
        start: Time,
        object_store: Arc<dyn ObjectStore>,
        wal_config: WalConfig,
    ) -> (ProcessingEngineManagerImpl, NamedTempFile) {
        let time_provider: Arc<dyn TimeProvider> = Arc::new(MockProvider::new(start));
        let metric_registry = Arc::new(Registry::new());
        let persister = Arc::new(Persister::new(
            Arc::clone(&object_store),
            "test_host",
            Arc::clone(&time_provider) as _,
        ));
        let catalog = Arc::new(persister.load_or_create_catalog().await.unwrap());
        let last_cache = LastCacheProvider::new_from_catalog(Arc::clone(&catalog) as _).unwrap();
        let distinct_cache = DistinctCacheProvider::new_from_catalog(
            Arc::clone(&time_provider),
            Arc::clone(&catalog),
        )
        .unwrap();
        let wbuf = WriteBufferImpl::new(WriteBufferImplArgs {
            persister,
            catalog: Arc::clone(&catalog),
            last_cache,
            distinct_cache,
            time_provider: Arc::clone(&time_provider),
            executor: make_exec(),
            wal_config,
            parquet_cache: None,
            metric_registry: Arc::clone(&metric_registry),
            snapshotted_wal_files_to_keep: 10,
            query_file_limit: None,
        })
        .await
        .unwrap();
        let ctx = IOxSessionContext::with_testing();
        let runtime_env = ctx.inner().runtime_env();
        register_iox_object_store(runtime_env, "influxdb3", Arc::clone(&object_store));

        let qe = Arc::new(UnimplementedQueryExecutor);
        let wal = wbuf.wal();

        let mut file = NamedTempFile::new().unwrap();
        let code = r#"
def process_writes(influxdb3_local, table_batches, args=None):
    influxdb3_local.info("done")
"#;
        writeln!(file, "{}", code).unwrap();
        let environment_manager = ProcessingEngineEnvironmentManager {
            plugin_dir: Some(file.path().parent().unwrap().to_path_buf()),
            virtual_env_location: None,
            package_manager: Arc::new(DisabledManager),
        };

        (
            ProcessingEngineManagerImpl::new(
                environment_manager,
                catalog,
                wbuf,
                qe,
                time_provider,
                wal,
            ),
            file,
        )
    }

    pub(crate) fn make_exec() -> Arc<Executor> {
        let metrics = Arc::new(metric::Registry::default());
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let parquet_store = ParquetStorage::new(
            Arc::clone(&object_store),
            StorageId::from("test_exec_storage"),
        );
        Arc::new(Executor::new_with_config_and_executor(
            ExecutorConfig {
                target_query_partitions: NonZeroUsize::new(1).unwrap(),
                object_stores: [&parquet_store]
                    .into_iter()
                    .map(|store| (store.id(), Arc::clone(store.object_store())))
                    .collect(),
                metric_registry: Arc::clone(&metrics),
                // Default to 1gb
                mem_pool_size: 1024 * 1024 * 1024, // 1024 (b/kb) * 1024 (kb/mb) * 1024 (mb/gb)
            },
            DedicatedExecutor::new_testing(),
        ))
    }
}
