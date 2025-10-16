use crate::environment::PythonEnvironmentManager;
use crate::manager::ProcessingEngineError;

use crate::plugins::PluginContext;
use crate::plugins::{PluginError, ProcessingEngineEnvironmentManager};
use anyhow::{Context, anyhow};
use bytes::Bytes;
use hashbrown::HashMap;
use influxdb3_catalog::CatalogError;
use influxdb3_catalog::catalog::Catalog;
use influxdb3_catalog::channel::CatalogUpdateReceiver;
use influxdb3_catalog::log::{
    CatalogBatch, DatabaseCatalogOp, DeleteTriggerLog, PluginType, TriggerDefinition,
    TriggerIdentifier, TriggerSpecificationDefinition, ValidPluginFilename,
};
use influxdb3_internal_api::query_executor::QueryExecutor;
use influxdb3_py_api::system_py::CacheStore;
use influxdb3_sys_events::SysEventStore;
use influxdb3_types::http::{
    SchedulePluginTestRequest, SchedulePluginTestResponse, WalPluginTestRequest,
    WalPluginTestResponse,
};
use influxdb3_wal::{SnapshotDetails, WalContents, WalFileNotifier};
use influxdb3_write::WriteBuffer;
use iox_http_util::Response;
use iox_time::TimeProvider;
use observability_deps::tracing::{debug, error, warn};
use parking_lot::Mutex;
use std::any::Any;
use std::fs;
use std::io::{Error as IoError, ErrorKind};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::fs as async_fs;
use tokio::sync::oneshot::Receiver;
use tokio::sync::{RwLock, mpsc, oneshot};

pub mod environment;
pub mod manager;
pub mod plugins;

// Constants for plugin file naming
const INIT_PY: &str = "__init__.py";
const PY_EXTENSION: &str = "py";
const PYCACHE_DIR: &str = "__pycache__";

pub mod virtualenv;

#[derive(Debug)]
pub struct ProcessingEngineManagerImpl {
    environment_manager: ProcessingEngineEnvironmentManager,
    catalog: Arc<Catalog>,
    node_id: Arc<str>,
    write_buffer: Arc<dyn WriteBuffer>,
    query_executor: Arc<dyn QueryExecutor>,
    time_provider: Arc<dyn TimeProvider>,
    sys_event_store: Arc<SysEventStore>,
    cache: Arc<Mutex<CacheStore>>,
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
                if let Some(trigger_map) = self.wal_triggers.get(&db)
                    && let Some(sender) = trigger_map.get(&trigger)
                {
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
            TriggerSpecificationDefinition::Schedule { .. }
            | TriggerSpecificationDefinition::Every { .. } => {
                if let Some(trigger_map) = self.schedule_triggers.get(&db)
                    && let Some(sender) = trigger_map.get(&trigger)
                {
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

    fn add_wal_trigger(&mut self, db: String, trigger: String) -> mpsc::Receiver<WalEvent> {
        let (tx, rx) = mpsc::channel(PLUGIN_EVENT_BUFFER_SIZE);
        self.wal_triggers.entry(db).or_default().insert(trigger, tx);
        rx
    }

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
    pub async fn new(
        environment: ProcessingEngineEnvironmentManager,
        catalog: Arc<Catalog>,
        node_id: impl Into<Arc<str>>,
        write_buffer: Arc<dyn WriteBuffer>,
        query_executor: Arc<dyn QueryExecutor>,
        time_provider: Arc<dyn TimeProvider>,
        sys_event_store: Arc<SysEventStore>,
    ) -> Arc<Self> {
        // if given a plugin dir, try to initialize the virtualenv.
        if let Some(plugin_dir) = &environment.plugin_dir {
            {
                environment
                    .package_manager
                    .init_pyenv(plugin_dir, environment.virtual_env_location.as_ref())
                    .expect("unable to initialize python environment");
                virtualenv::init_pyo3();
            }
        }

        let catalog_sub = catalog.subscribe_to_updates("processing_engine").await;

        let cache = Arc::new(Mutex::new(CacheStore::new(
            Arc::clone(&time_provider),
            Duration::from_secs(10),
        )));

        let pem = Arc::new(Self {
            environment_manager: environment,
            catalog,
            node_id: node_id.into(),
            write_buffer,
            query_executor,
            sys_event_store,
            time_provider,
            plugin_event_tx: Default::default(),
            cache,
        });

        background_catalog_update(Arc::clone(&pem), catalog_sub);

        pem
    }

    pub fn node_id(&self) -> Arc<str> {
        Arc::clone(&self.node_id)
    }

    pub async fn validate_plugin_filename<'a>(
        &self,
        name: &'a str,
    ) -> Result<ValidPluginFilename<'a>, PluginError> {
        let _ = self.read_plugin_code(name).await?;
        Ok(ValidPluginFilename::from_validated_name(name))
    }

    pub async fn read_plugin_code(&self, name: &str) -> Result<PluginCode, PluginError> {
        // if the name starts with gh: then we use the custom repo if set or we need to get it from
        // the public github repo at https://github.com/influxdata/influxdb3_plugins/tree/main
        if name.starts_with("gh:") {
            let plugin_path = name.strip_prefix("gh:").unwrap();
            let plugin_repo =
                self.environment_manager.plugin_repo.as_deref().unwrap_or(
                    "https://raw.githubusercontent.com/influxdata/influxdb3_plugins/main/",
                );

            // combine the repo and path, adjusting for ending / if needed
            let url = if plugin_repo.ends_with('/') {
                format!("{plugin_repo}{plugin_path}")
            } else {
                format!("{plugin_repo}/{plugin_path}")
            };

            let resp = reqwest::get(&url)
                .await
                .context("error getting plugin from repository")?;

            // verify the response is a success
            if !resp.status().is_success() {
                return Err(PluginError::FetchingFromRepository(resp.status(), url));
            }

            let resp_body = resp
                .text()
                .await
                .context("error reading plugin from repository")?;
            return Ok(PluginCode::Github(Arc::from(resp_body)));
        }

        // otherwise we assume it is a local file or directory
        let plugin_dir = self
            .environment_manager
            .plugin_dir
            .clone()
            .ok_or(PluginError::NoPluginDir)?;

        let plugin_name = name.trim_end_matches('/');

        // First, normalize the path components to check for path traversal
        // This catches attempts like "../../etc/passwd" before we try to access the filesystem
        let normalized_path = std::path::Path::new(plugin_name);
        for component in normalized_path.components() {
            match component {
                std::path::Component::ParentDir => {
                    // Any ".." component is a path traversal attempt
                    return Err(PluginError::PathTraversal(name.to_string()));
                }
                std::path::Component::RootDir => {
                    // Absolute paths are not allowed
                    return Err(PluginError::PathTraversal(name.to_string()));
                }
                _ => {} // Normal and CurDir components are fine
            }
        }

        let plugin_path = plugin_dir.join(plugin_name);

        // Canonicalize both paths to prevent path traversal attacks via symlinks
        let canonical_plugin_dir = plugin_dir
            .canonicalize()
            .context("failed to canonicalize plugin directory")?;

        // For the plugin path, we need to handle the case where the file doesn't exist yet
        let canonical_plugin_path = if plugin_path.exists() {
            plugin_path
                .canonicalize()
                .context("failed to canonicalize plugin path")?
        } else {
            // If file doesn't exist, canonicalize the parent and append the filename
            if let Some(parent) = plugin_path.parent() {
                if let Some(filename) = plugin_path.file_name() {
                    let canonical_parent = parent
                        .canonicalize()
                        .context("failed to canonicalize plugin path parent directory")?;
                    canonical_parent.join(filename)
                } else {
                    return Err(PluginError::PathTraversal(name.to_string()));
                }
            } else {
                return Err(PluginError::PathTraversal(name.to_string()));
            }
        };

        // Verify that the canonical plugin path is within the canonical plugin directory
        if !canonical_plugin_path.starts_with(&canonical_plugin_dir) {
            return Err(PluginError::PathTraversal(name.to_string()));
        }

        if !plugin_path.exists() {
            return Err(PluginError::ReadPluginError(IoError::new(
                ErrorKind::NotFound,
                format!("Plugin not found: {}", plugin_path.display()),
            )));
        }

        if plugin_path.is_dir() {
            let entry_point = plugin_path.join(INIT_PY);
            if !entry_point.exists() {
                return Err(PluginError::ReadPluginError(IoError::new(
                    ErrorKind::NotFound,
                    format!(
                        "Multi-file plugin directory must contain {}: {}",
                        INIT_PY,
                        plugin_path.display()
                    ),
                )));
            }

            let code = async_fs::read_to_string(&entry_point).await?;

            return Ok(PluginCode::LocalDirectory(LocalPluginDirectory {
                plugin_root: plugin_path,
                entry_point,
                last_read_and_code: Mutex::new((SystemTime::now(), Arc::from(code))),
            }));
        }

        // Single file plugin
        let code = async_fs::read_to_string(&plugin_path).await?;

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
    LocalDirectory(LocalPluginDirectory),
}

impl PluginCode {
    pub(crate) fn code(&self) -> Arc<str> {
        match self {
            PluginCode::Github(code) => Arc::clone(code),
            PluginCode::Local(plugin) => plugin.read_if_modified(),
            PluginCode::LocalDirectory(plugin) => plugin.read_entry_point_if_modified(),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn is_directory(&self) -> bool {
        matches!(self, PluginCode::LocalDirectory(_))
    }

    pub(crate) fn plugin_root(&self) -> Option<&PathBuf> {
        match self {
            PluginCode::LocalDirectory(plugin) => Some(&plugin.plugin_root),
            _ => None,
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
    fn read_if_modified(&self) -> Arc<str> {
        let metadata = fs::metadata(&self.plugin_path);

        let mut last_read_and_code = self.last_read_and_code.lock();
        let (last_read, code) = &mut *last_read_and_code;

        match metadata {
            Ok(metadata) => {
                let is_modified = match metadata.modified() {
                    Ok(modified) => modified > *last_read,
                    Err(_) => true, // if we can't get the modified time, assume it is modified
                };

                if is_modified {
                    // attempt to read the code, if it fails we will return the last known code
                    if let Ok(new_code) = fs::read_to_string(&self.plugin_path) {
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

/// A multi-file plugin stored as a directory on the local filesystem.
///
/// Multi-file plugins must have an `__init__.py` file at the root that serves as
/// the entry point and contains the trigger functions (e.g., `process_writes`).
/// Other Python files in the directory can be imported using standard Python import syntax.
///
/// # Example Structure
///
/// ```text
/// my_plugin/
///   __init__.py      (contains process_writes, imports from utils)
///   utils.py         (helper functions)
///   models/
///     __init__.py
///     data.py        (data models)
/// ```
#[derive(Debug)]
pub struct LocalPluginDirectory {
    plugin_root: PathBuf,
    entry_point: PathBuf,
    last_read_and_code: Mutex<(SystemTime, Arc<str>)>,
}

impl LocalPluginDirectory {
    /// Reads the plugin entry point (`__init__.py`) if any Python file in the
    /// directory has been modified.
    fn read_entry_point_if_modified(&self) -> Arc<str> {
        let mut last_read_and_code = self.last_read_and_code.lock();
        let (last_read, code) = &mut *last_read_and_code;

        if let Some(latest_modified) = self.find_latest_modified_time()
            && latest_modified > *last_read
        {
            if let Ok(new_code) = fs::read_to_string(&self.entry_point) {
                *last_read = SystemTime::now();
                *code = Arc::from(new_code);
            } else {
                error!("error reading plugin entry point {:?}", self.entry_point);
            }
        }

        Arc::clone(code)
    }

    /// Finds the latest modification time of any `.py` file in the plugin directory.
    fn find_latest_modified_time(&self) -> Option<SystemTime> {
        use walkdir::WalkDir;

        WalkDir::new(&self.plugin_root)
            .follow_links(false)
            .into_iter()
            .filter_entry(|e| {
                // Skip __pycache__ directories entirely
                e.file_name()
                    .to_str()
                    .map(|s| s != PYCACHE_DIR)
                    .unwrap_or(true)
            })
            .filter_map(Result::ok)
            .filter(|entry| {
                entry.path().extension().and_then(|s| s.to_str()) == Some(PY_EXTENSION)
                    && entry.file_type().is_file()
            })
            .filter_map(|entry| entry.metadata().ok()?.modified().ok())
            .max()
    }

    pub fn plugin_root(&self) -> &PathBuf {
        &self.plugin_root
    }
}

impl ProcessingEngineManagerImpl {
    // TODO(trevor): should this be id-based and not use names?
    async fn run_trigger(
        self: Arc<Self>,
        db_name: &str,
        trigger_name: &str,
    ) -> Result<(), ProcessingEngineError> {
        debug!(db_name, trigger_name, "starting trigger");

        {
            let db_schema = self
                .catalog
                .db_schema(db_name)
                .ok_or_else(|| ProcessingEngineError::DatabaseNotFound(db_name.to_string()))?;
            let trigger = db_schema
                .processing_engine_triggers
                .get_by_name(trigger_name)
                .clone()
                .ok_or_else(|| CatalogError::ProcessingEngineTriggerNotFound {
                    database_name: db_name.to_string(),
                    trigger_name: trigger_name.to_string(),
                })?;

            if trigger.node_id != self.node_id {
                error!(
                    "Not running trigger {}, as it is configured for node id {}. Multi-node not supported in core, so this shouldn't happen.",
                    trigger_name, trigger.node_id
                );
                return Ok(());
            }

            let plugin_context = PluginContext {
                write_buffer: Arc::clone(&self.write_buffer),
                query_executor: Arc::clone(&self.query_executor),
                sys_event_store: Arc::clone(&self.sys_event_store),
                manager: Arc::clone(&self),
            };
            let plugin_code = Arc::new(self.read_plugin_code(&trigger.plugin_filename).await?);
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

    // TODO(trevor): should this be id-based and not use names?
    async fn stop_trigger(
        &self,
        db_name: &str,
        trigger_name: &str,
    ) -> Result<(), ProcessingEngineError> {
        let db_schema = self
            .catalog
            .db_schema(db_name)
            .ok_or_else(|| ProcessingEngineError::DatabaseNotFound(db_name.to_string()))?;
        let trigger = db_schema
            .processing_engine_triggers
            .get_by_name(trigger_name)
            .ok_or_else(|| CatalogError::ProcessingEngineTriggerNotFound {
                database_name: db_name.to_string(),
                trigger_name: trigger_name.to_string(),
            })?;

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
        self.cache
            .lock()
            .drop_trigger_cache(db_name.to_string(), trigger_name.to_string());

        Ok(())
    }

    pub async fn start_triggers(self: Arc<Self>) -> Result<(), ProcessingEngineError> {
        let triggers = self.catalog.active_triggers();
        for (db_name, trigger_name) in triggers {
            Arc::clone(&self)
                .run_trigger(&db_name, &trigger_name)
                .await?;
        }
        Ok(())
    }

    pub async fn test_wal_plugin(
        &self,
        request: WalPluginTestRequest,
        query_executor: Arc<dyn QueryExecutor>,
    ) -> Result<WalPluginTestResponse, plugins::PluginError> {
        {
            let catalog = Arc::clone(&self.catalog);
            let now = self.time_provider.now();

            let code = self.read_plugin_code(&request.filename).await?;
            let cache = Arc::clone(&self.cache);
            let code_string = code.code().to_string();

            let res = tokio::task::spawn_blocking(move || {
                plugins::run_test_wal_plugin(
                    now,
                    catalog,
                    query_executor,
                    code_string,
                    cache,
                    request,
                )
                .unwrap_or_else(|e| WalPluginTestResponse {
                    log_lines: vec![],
                    database_writes: Default::default(),
                    errors: vec![e.to_string()],
                })
            })
            .await?;

            Ok(res)
        }
    }

    pub async fn test_schedule_plugin(
        &self,
        request: SchedulePluginTestRequest,
        query_executor: Arc<dyn QueryExecutor>,
    ) -> Result<SchedulePluginTestResponse, PluginError> {
        {
            let catalog = Arc::clone(&self.catalog);
            let now = self.time_provider.now();

            let code = self.read_plugin_code(&request.filename).await?;
            let code_string = code.code().to_string();
            let cache = Arc::clone(&self.cache);

            let res = tokio::task::spawn_blocking(move || {
                plugins::run_test_schedule_plugin(
                    now,
                    catalog,
                    query_executor,
                    code_string,
                    cache,
                    request,
                )
            })
            .await?
            .unwrap_or_else(|e| SchedulePluginTestResponse {
                log_lines: vec![],
                database_writes: Default::default(),
                errors: vec![e.to_string()],
                trigger_time: None,
            });

            Ok(res)
        }
    }

    pub async fn request_trigger(
        &self,
        trigger_path: &str,
        query_params: HashMap<String, String>,
        request_headers: HashMap<String, String>,
        request_body: Bytes,
    ) -> Result<Response, ProcessingEngineError> {
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

        rx.await.map_err(|e| {
            error!(%e, "error receiving response from plugin");
            ProcessingEngineError::RequestHandlerDown
        })
    }

    pub fn get_environment_manager(&self) -> Arc<dyn PythonEnvironmentManager> {
        Arc::clone(&self.environment_manager.package_manager)
    }

    pub async fn list_plugin_files(&self) -> Vec<PluginFileInfo> {
        use walkdir::WalkDir;

        let mut plugin_files = Vec::new();

        for db_schema in self.catalog.list_db_schema() {
            for trigger in db_schema.processing_engine_triggers.resource_iter() {
                let plugin_name = Arc::<str>::clone(&trigger.trigger_name);
                debug!(
                    "Processing trigger '{}' with plugin_filename '{}'",
                    trigger.trigger_name, trigger.plugin_filename
                );

                if let Some(ref plugin_dir) = self.environment_manager.plugin_dir {
                    let plugin_filename = trigger.plugin_filename.trim_end_matches('/');
                    let plugin_path = plugin_dir.join(plugin_filename);

                    if let Ok(metadata) = async_fs::metadata(&plugin_path).await {
                        if metadata.is_file() {
                            plugin_files.push(PluginFileInfo {
                                plugin_name: Arc::<str>::clone(&plugin_name),
                                file_name: trigger.plugin_filename.clone().into(),
                                file_path: plugin_path.to_string_lossy().into(),
                                size_bytes: metadata.len() as i64,
                                last_modified_millis: metadata
                                    .modified()
                                    .ok()
                                    .and_then(|t| t.duration_since(SystemTime::UNIX_EPOCH).ok())
                                    .map(|d| d.as_millis() as i64)
                                    .unwrap_or(0),
                            });
                        } else if metadata.is_dir() {
                            for entry in WalkDir::new(&plugin_path)
                                .follow_links(false)
                                .into_iter()
                                .filter_entry(|e| {
                                    // Skip __pycache__ directories
                                    e.file_name()
                                        .to_str()
                                        .map(|s| s != PYCACHE_DIR)
                                        .unwrap_or(true)
                                })
                                .filter_map(Result::ok)
                            {
                                if entry.file_type().is_file()
                                    && entry.path().extension().and_then(|s| s.to_str())
                                        == Some(PY_EXTENSION)
                                    && let Ok(file_metadata) = entry.metadata()
                                {
                                    let relative_path = entry
                                        .path()
                                        .strip_prefix(&plugin_path)
                                        .unwrap_or(entry.path());

                                    plugin_files.push(PluginFileInfo {
                                        plugin_name: Arc::<str>::clone(&plugin_name),
                                        file_name: relative_path.to_string_lossy().into(),
                                        file_path: entry.path().to_string_lossy().into(),
                                        size_bytes: file_metadata.len() as i64,
                                        last_modified_millis: file_metadata
                                            .modified()
                                            .ok()
                                            .and_then(|t| {
                                                t.duration_since(SystemTime::UNIX_EPOCH).ok()
                                            })
                                            .map(|d| d.as_millis() as i64)
                                            .unwrap_or(0),
                                    });
                                }
                            }
                        }
                    }
                }
            }
        }

        plugin_files
    }

    pub async fn create_plugin_file(
        self: &Arc<Self>,
        plugin_filename: &str,
        content: &str,
    ) -> Result<(), ProcessingEngineError> {
        let plugin_dir = self
            .environment_manager
            .plugin_dir
            .as_ref()
            .ok_or_else(|| {
                ProcessingEngineError::PluginError(plugins::PluginError::AnyhowError(anyhow!(
                    "No plugin directory configured"
                )))
            })?;

        let plugin_path = plugin_dir.join(plugin_filename);

        // Create parent directories if they don't exist (for multi-file plugins)
        if let Some(parent) = plugin_path.parent() {
            async_fs::create_dir_all(parent).await.map_err(|e| {
                ProcessingEngineError::PluginError(plugins::PluginError::ReadPluginError(e))
            })?;
        }

        async_fs::write(plugin_path, content).await.map_err(|e| {
            ProcessingEngineError::PluginError(plugins::PluginError::ReadPluginError(e))
        })?;

        Ok(())
    }

    pub async fn update_plugin_file(
        self: &Arc<Self>,
        plugin_name: &str,
        content: &str,
    ) -> Result<String, ProcessingEngineError> {
        for db_schema in self.catalog.list_db_schema() {
            if let Some(trigger) = db_schema
                .processing_engine_triggers
                .resource_iter()
                .find(|t| t.trigger_name.as_ref() == plugin_name)
                && let Some(ref plugin_dir) = self.environment_manager.plugin_dir
            {
                let plugin_path = plugin_dir.join(&trigger.plugin_filename);

                // For single-file plugins, update the file directly
                if !plugin_path.is_dir() {
                    async_fs::write(plugin_path, content).await.map_err(|e| {
                        ProcessingEngineError::PluginError(plugins::PluginError::ReadPluginError(e))
                    })?;

                    return Ok(db_schema.name.to_string());
                }

                // For multi-file plugins (directories), update __init__.py by default
                let init_file = plugin_path.join(INIT_PY);
                async_fs::write(init_file, content).await.map_err(|e| {
                    ProcessingEngineError::PluginError(plugins::PluginError::ReadPluginError(e))
                })?;

                return Ok(db_schema.name.to_string());
            }
        }

        Err(ProcessingEngineError::PluginError(
            plugins::PluginError::AnyhowError(anyhow::anyhow!("Plugin not found: {}", plugin_name)),
        ))
    }

    /// Replace an entire plugin directory atomically with new files.
    pub async fn replace_plugin_directory(
        self: &Arc<Self>,
        plugin_name: &str,
        files: Vec<(String, String)>, // Vec of (relative_path, content)
    ) -> Result<String, ProcessingEngineError> {
        // Find the trigger to get the plugin filename
        let (db_name, plugin_filename) = {
            let mut result = None;
            for db_schema in self.catalog.list_db_schema() {
                if let Some(trigger) = db_schema
                    .processing_engine_triggers
                    .resource_iter()
                    .find(|t| t.trigger_name.as_ref() == plugin_name)
                {
                    result = Some((
                        db_schema.name.to_string(),
                        trigger.plugin_filename.to_string(),
                    ));
                    break;
                }
            }
            result.ok_or_else(|| {
                ProcessingEngineError::PluginError(PluginError::AnyhowError(anyhow!(
                    "Plugin not found: {}",
                    plugin_name
                )))
            })?
        };

        let plugin_dir = self
            .environment_manager
            .plugin_dir
            .as_ref()
            .ok_or_else(|| {
                ProcessingEngineError::PluginError(PluginError::AnyhowError(anyhow!(
                    "No plugin directory configured"
                )))
            })?;

        let plugin_path = plugin_dir.join(&plugin_filename);
        let temp_path = plugin_dir.join(format!("{}.tmp", plugin_filename));
        let old_path = plugin_dir.join(format!("{}.old", plugin_filename));

        if temp_path.exists() {
            async_fs::remove_dir_all(&temp_path)
                .await
                .context("Failed to remove existing temp directory")
                .map_err(|e| ProcessingEngineError::PluginError(PluginError::AnyhowError(e)))?;
        }

        async_fs::create_dir_all(&temp_path)
            .await
            .context("Failed to create temp directory")
            .map_err(|e| ProcessingEngineError::PluginError(PluginError::AnyhowError(e)))?;

        // Write all files to temp directory
        for (relative_path, content) in files {
            let file_path = temp_path.join(&relative_path);

            // Create parent directories if needed
            if let Some(parent) = file_path.parent() {
                async_fs::create_dir_all(parent)
                    .await
                    .with_context(|| {
                        format!("Failed to create parent directory for {}", relative_path)
                    })
                    .map_err(|e| {
                        // Cleanup temp dir on failure
                        let temp_clone = temp_path.clone();
                        tokio::spawn(async move {
                            let _ = async_fs::remove_dir_all(temp_clone).await;
                        });
                        ProcessingEngineError::PluginError(PluginError::AnyhowError(e))
                    })?;
            }

            async_fs::write(&file_path, content)
                .await
                .with_context(|| format!("Failed to write file {}", relative_path))
                .map_err(|e| {
                    // Cleanup temp dir on failure
                    let temp_clone = temp_path.clone();
                    tokio::spawn(async move {
                        let _ = async_fs::remove_dir_all(temp_clone).await;
                    });
                    ProcessingEngineError::PluginError(PluginError::AnyhowError(e))
                })?;
        }

        if plugin_path.exists() {
            if old_path.exists() {
                async_fs::remove_dir_all(&old_path)
                    .await
                    .context("Failed to remove existing old directory")
                    .map_err(|e| ProcessingEngineError::PluginError(PluginError::AnyhowError(e)))?;
            }

            async_fs::rename(&plugin_path, &old_path)
                .await
                .context("Failed to rename old directory")
                .map_err(|e| {
                    // Cleanup temp dir on failure
                    let temp_clone = temp_path.clone();
                    tokio::spawn(async move {
                        let _ = async_fs::remove_dir_all(temp_clone).await;
                    });
                    ProcessingEngineError::PluginError(PluginError::AnyhowError(e))
                })?;
        }

        let rename_result = async_fs::rename(&temp_path, &plugin_path).await;

        if let Err(e) = rename_result {
            // Rollback: restore old directory if it exists
            if old_path.exists() {
                let _ = async_fs::rename(&old_path, &plugin_path).await;
            }
            let _ = async_fs::remove_dir_all(&temp_path).await;

            return Err(ProcessingEngineError::PluginError(
                PluginError::AnyhowError(
                    anyhow!(e).context("Failed to rename temp directory to target"),
                ),
            ));
        }

        if old_path.exists() {
            async_fs::remove_dir_all(&old_path)
                .await
                .context("Failed to delete old directory")
                .map_err(|e| ProcessingEngineError::PluginError(PluginError::AnyhowError(e)))?;
        }

        Ok(db_name)
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
    pub response_tx: oneshot::Sender<Response>,
}

#[derive(Debug)]
pub struct PluginFileInfo {
    pub plugin_name: Arc<str>,
    pub file_name: Arc<str>,
    pub file_path: Arc<str>,
    pub size_bytes: i64,
    pub last_modified_millis: i64,
}

fn background_catalog_update(
    processing_engine_manager: Arc<ProcessingEngineManagerImpl>,
    mut subscription: CatalogUpdateReceiver,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(catalog_update) = subscription.recv().await {
            for batch in catalog_update
                .batches()
                .filter_map(CatalogBatch::as_database)
            {
                for op in batch.ops.iter() {
                    let processing_engine_manager = Arc::clone(&processing_engine_manager);
                    match op {
                        DatabaseCatalogOp::CreateTrigger(TriggerDefinition {
                            trigger_name,
                            database_name,
                            disabled,
                            ..
                        }) => {
                            if !disabled
                                && let Err(error) = processing_engine_manager
                                    .run_trigger(database_name, trigger_name)
                                    .await
                            {
                                error!(?error, "failed to run the created trigger");
                            }
                        }
                        DatabaseCatalogOp::EnableTrigger(TriggerIdentifier {
                            db_name,
                            trigger_name,
                            ..
                        }) => {
                            if let Err(error) = processing_engine_manager
                                .run_trigger(db_name, trigger_name)
                                .await
                            {
                                error!(?error, "failed to run the trigger");
                            }
                        }
                        DatabaseCatalogOp::DeleteTrigger(DeleteTriggerLog {
                            trigger_name,
                            force: true,
                            ..
                        }) => {
                            if let Err(error) = processing_engine_manager
                                .stop_trigger(&batch.database_name, trigger_name)
                                .await
                            {
                                error!(?error, "failed to disable the trigger");
                            }
                        }
                        DatabaseCatalogOp::DisableTrigger(TriggerIdentifier {
                            db_name,
                            trigger_name,
                            ..
                        }) => {
                            if let Err(error) = processing_engine_manager
                                .stop_trigger(db_name, trigger_name)
                                .await
                            {
                                error!(?error, "failed to disable the trigger");
                            }
                        }
                        // NOTE(trevor/catalog-refactor): it is not clear that any other operation needs to be
                        // handled, based on the existing code, but we could potentially
                        // handle database deletion, trigger creation/deletion/enable here
                        _ => (),
                    }
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use crate::ProcessingEngineManagerImpl;
    use crate::environment::DisabledManager;
    use crate::plugins::ProcessingEngineEnvironmentManager;
    use data_types::NamespaceName;
    use datafusion_util::config::register_iox_object_store;
    use influxdb3_cache::distinct_cache::DistinctCacheProvider;
    use influxdb3_cache::last_cache::LastCacheProvider;
    use influxdb3_catalog::CatalogError;
    use influxdb3_catalog::catalog::Catalog;
    use influxdb3_catalog::log::{TriggerSettings, TriggerSpecificationDefinition};
    use influxdb3_internal_api::query_executor::UnimplementedQueryExecutor;
    use influxdb3_shutdown::ShutdownManager;
    use influxdb3_sys_events::SysEventStore;
    use influxdb3_wal::{Gen1Duration, WalConfig};
    use influxdb3_write::persister::Persister;
    use influxdb3_write::write_buffer::{
        N_SNAPSHOTS_TO_LOAD_ON_START, WriteBufferImpl, WriteBufferImplArgs,
    };
    use influxdb3_write::{Precision, WriteBuffer};
    use iox_query::exec::{
        DedicatedExecutor, Executor, ExecutorConfig, IOxSessionContext, PerQueryMemoryPoolConfig,
    };
    use iox_time::{MockProvider, Time, TimeProvider};
    use metric::Registry;
    use object_store::ObjectStore;
    use object_store::memory::InMemory;
    use parking_lot::Mutex;
    use parquet_file::storage::{ParquetStorage, StorageId};
    use std::io::Write;
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};
    use tempfile::NamedTempFile;

    #[test_log::test(tokio::test)]
    async fn test_trigger_lifecycle() -> influxdb3_write::write_buffer::Result<()> {
        let start_time = Time::from_rfc3339("2024-11-14T11:00:00+00:00").unwrap();
        let test_store = Arc::new(InMemory::new());
        let wal_config = WalConfig {
            gen1_duration: Gen1Duration::new_1m(),
            max_write_buffer_size: 100,
            flush_interval: Duration::from_millis(10),
            snapshot_size: 1,
            ..Default::default()
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
        let file_name = pem
            .validate_plugin_filename(file_name.as_str())
            .await
            .unwrap();

        write_buffer
            .catalog()
            .create_processing_engine_trigger(
                "foo",
                "test_trigger",
                Arc::clone(&pem.node_id),
                file_name,
                &TriggerSpecificationDefinition::AllTablesWalWrite.string_rep(),
                TriggerSettings::default(),
                &None,
                false,
            )
            .await
            .unwrap();

        // Verify trigger is not disabled in schema
        let schema = write_buffer.catalog().db_schema("foo").unwrap();
        let trigger = schema
            .processing_engine_triggers
            .get_by_name("test_trigger")
            .unwrap();
        assert!(!trigger.disabled);

        // Disable the trigger
        write_buffer
            .catalog()
            .disable_processing_engine_trigger("foo", "test_trigger")
            .await
            .unwrap();

        // Verify trigger is disabled in schema
        let schema = write_buffer.catalog().db_schema("foo").unwrap();
        let trigger = schema
            .processing_engine_triggers
            .get_by_name("test_trigger")
            .unwrap();
        assert!(trigger.disabled);

        // Enable the trigger
        write_buffer
            .catalog()
            .enable_processing_engine_trigger("foo", "test_trigger")
            .await
            .unwrap();

        // Verify trigger is enabled and running
        let schema = write_buffer.catalog().db_schema("foo").unwrap();
        let trigger = schema
            .processing_engine_triggers
            .get_by_name("test_trigger")
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
            ..Default::default()
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

        let file_name = pem.validate_plugin_filename(&file_name).await.unwrap();
        // Create a disabled trigger
        pem.catalog
            .create_processing_engine_trigger(
                "foo",
                "test_trigger",
                Arc::clone(&pem.node_id),
                file_name,
                &TriggerSpecificationDefinition::AllTablesWalWrite.string_rep(),
                TriggerSettings::default(),
                &None,
                true,
            )
            .await
            .unwrap();

        // Verify trigger is created but disabled
        let schema = pem.catalog.db_schema("foo").unwrap();
        let trigger = schema
            .processing_engine_triggers
            .get_by_name("test_trigger")
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
            ..Default::default()
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

        let Err(CatalogError::NotFound) = pem
            .catalog
            .enable_processing_engine_trigger("foo", "nonexistent_trigger")
            .await
        else {
            panic!("should receive not found error for non existent trigger on enable");
        };

        Ok(())
    }

    async fn setup(
        start: Time,
        object_store: Arc<dyn ObjectStore>,
        wal_config: WalConfig,
    ) -> (Arc<ProcessingEngineManagerImpl>, NamedTempFile) {
        let time_provider: Arc<dyn TimeProvider> = Arc::new(MockProvider::new(start));
        let metric_registry = Arc::new(Registry::new());
        let persister = Arc::new(Persister::new(
            Arc::clone(&object_store),
            "test_host".to_string(),
            Arc::clone(&time_provider),
        ));
        let catalog = Arc::new(
            Catalog::new(
                "test_host",
                Arc::clone(&object_store),
                Arc::clone(&time_provider),
                Default::default(),
            )
            .await
            .unwrap(),
        );
        let last_cache = LastCacheProvider::new_from_catalog(Arc::clone(&catalog) as _)
            .await
            .unwrap();
        let distinct_cache = DistinctCacheProvider::new_from_catalog(
            Arc::clone(&time_provider),
            Arc::clone(&catalog),
        )
        .await
        .unwrap();
        let shutdown = ShutdownManager::new_testing();
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
            shutdown: shutdown.register(),
            n_snapshots_to_load_on_start: N_SNAPSHOTS_TO_LOAD_ON_START,
            wal_replay_concurrency_limit: 1,
        })
        .await
        .unwrap();
        let ctx = IOxSessionContext::with_testing();
        let runtime_env = ctx.inner().runtime_env();
        register_iox_object_store(runtime_env, "influxdb3", Arc::clone(&object_store));

        let qe = Arc::new(UnimplementedQueryExecutor);

        let mut file = NamedTempFile::new().unwrap();
        let code = r#"
def process_writes(influxdb3_local, table_batches, args=None):
    influxdb3_local.info("done")
"#;
        writeln!(file, "{code}").unwrap();
        let environment_manager = ProcessingEngineEnvironmentManager {
            plugin_dir: Some(file.path().parent().unwrap().to_path_buf()),
            virtual_env_location: None,
            package_manager: Arc::new(DisabledManager),
            plugin_repo: None,
        };

        let sys_event_store = Arc::new(SysEventStore::new(Arc::clone(&time_provider)));

        (
            ProcessingEngineManagerImpl::new(
                environment_manager,
                catalog,
                "test_node",
                wbuf,
                qe,
                time_provider,
                sys_event_store,
            )
            .await,
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
                heap_memory_limit: None,
                per_query_mem_pool_config: PerQueryMemoryPoolConfig::Disabled,
            },
            DedicatedExecutor::new_testing(),
        ))
    }

    fn construct_plugin_url(plugin_repo: Option<&str>, plugin_path: &str) -> String {
        let repo = plugin_repo
            .unwrap_or("https://raw.githubusercontent.com/influxdata/influxdb3_plugins/main/");
        if repo.ends_with('/') {
            format!("{repo}{plugin_path}")
        } else {
            format!("{repo}/{plugin_path}")
        }
    }

    #[test]
    fn test_plugin_repo_url_construction_default() {
        // Test URL construction with default repo
        let plugin_repo: Option<String> = None;
        let plugin_path = "my_plugin.py";
        let url = construct_plugin_url(plugin_repo.as_deref(), plugin_path);
        assert_eq!(
            url,
            "https://raw.githubusercontent.com/influxdata/influxdb3_plugins/main/my_plugin.py"
        );
    }

    #[test]
    fn test_plugin_repo_url_construction_custom() {
        // Test URL construction with custom repo
        let plugin_repo = Some("https://custom-repo.example.com/plugins/".to_string());
        let plugin_path = "my_plugin.py";
        let url = construct_plugin_url(plugin_repo.as_deref(), plugin_path);
        assert_eq!(url, "https://custom-repo.example.com/plugins/my_plugin.py");
    }

    #[test]
    fn test_plugin_repo_url_construction_custom_without_trailing_slash() {
        // Test URL construction with custom repo without trailing slash
        let plugin_repo = Some("https://custom-repo.example.com/plugins".to_string());
        let plugin_path = "my_plugin.py";
        let url = construct_plugin_url(plugin_repo.as_deref(), plugin_path);
        // Automatic slash insertion creates correct URL regardless of input format
        assert_eq!(url, "https://custom-repo.example.com/plugins/my_plugin.py");
    }

    #[tokio::test]
    async fn test_read_multifile_plugin() {
        let temp_dir = tempfile::tempdir().unwrap();
        let plugin_dir = temp_dir.path().join("my_plugin");
        std::fs::create_dir(&plugin_dir).unwrap();

        let init_code = r#"
from .utils import helper_function

def process_writes(influxdb3_local, table_batches, args=None):
    helper_function()
    influxdb3_local.info("done")
"#;
        std::fs::write(plugin_dir.join("__init__.py"), init_code).unwrap();

        let utils_code = r#"
def helper_function():
    return "helper"
"#;
        std::fs::write(plugin_dir.join("utils.py"), utils_code).unwrap();

        let environment_manager = ProcessingEngineEnvironmentManager {
            plugin_dir: Some(temp_dir.path().to_path_buf()),
            virtual_env_location: None,
            package_manager: Arc::new(DisabledManager),
            plugin_repo: None,
        };

        let start_time = Time::from_rfc3339("2024-11-14T11:00:00+00:00").unwrap();
        let time_provider: Arc<dyn TimeProvider> = Arc::new(MockProvider::new(start_time));
        let test_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let catalog = Arc::new(
            Catalog::new(
                "test_host",
                Arc::clone(&test_store),
                Arc::clone(&time_provider),
                Default::default(),
            )
            .await
            .unwrap(),
        );

        let sys_event_store = Arc::new(SysEventStore::new(Arc::clone(&time_provider)));
        let qe = Arc::new(UnimplementedQueryExecutor);
        let persister = Arc::new(Persister::new(
            Arc::clone(&test_store),
            "test_host".to_string(),
            Arc::clone(&time_provider),
        ));
        let last_cache = LastCacheProvider::new_from_catalog(Arc::clone(&catalog) as _)
            .await
            .unwrap();
        let distinct_cache = DistinctCacheProvider::new_from_catalog(
            Arc::clone(&time_provider),
            Arc::clone(&catalog),
        )
        .await
        .unwrap();
        let shutdown = ShutdownManager::new_testing();
        let wal_config = WalConfig {
            gen1_duration: Gen1Duration::new_1m(),
            max_write_buffer_size: 100,
            flush_interval: Duration::from_millis(10),
            snapshot_size: 1,
            ..Default::default()
        };
        let wbuf = WriteBufferImpl::new(WriteBufferImplArgs {
            persister,
            catalog: Arc::clone(&catalog),
            last_cache,
            distinct_cache,
            time_provider: Arc::clone(&time_provider),
            executor: make_exec(),
            wal_config,
            parquet_cache: None,
            metric_registry: Arc::new(Registry::new()),
            snapshotted_wal_files_to_keep: 10,
            query_file_limit: None,
            shutdown: shutdown.register(),
            n_snapshots_to_load_on_start: N_SNAPSHOTS_TO_LOAD_ON_START,
            wal_replay_concurrency_limit: 1,
        })
        .await
        .unwrap();

        let pem = ProcessingEngineManagerImpl::new(
            environment_manager,
            catalog,
            "test_node",
            wbuf,
            qe,
            time_provider,
            sys_event_store,
        )
        .await;

        let plugin_code = pem.read_plugin_code("my_plugin").await.unwrap();

        match plugin_code {
            crate::PluginCode::LocalDirectory(dir) => {
                assert!(dir.plugin_root.ends_with("my_plugin"));
                assert!(dir.entry_point.ends_with("__init__.py"));
                let code = dir.read_entry_point_if_modified();
                assert!(code.contains("helper_function"));
            }
            _ => panic!("Expected LocalDirectory variant"),
        }
    }

    #[tokio::test]
    async fn test_missing_init_py() {
        let temp_dir = tempfile::tempdir().unwrap();
        let plugin_dir = temp_dir.path().join("my_plugin");
        std::fs::create_dir(&plugin_dir).unwrap();

        std::fs::write(plugin_dir.join("utils.py"), "def helper(): pass").unwrap();

        let environment_manager = ProcessingEngineEnvironmentManager {
            plugin_dir: Some(temp_dir.path().to_path_buf()),
            virtual_env_location: None,
            package_manager: Arc::new(DisabledManager),
            plugin_repo: None,
        };

        let start_time = Time::from_rfc3339("2024-11-14T11:00:00+00:00").unwrap();
        let time_provider: Arc<dyn TimeProvider> = Arc::new(MockProvider::new(start_time));
        let test_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let catalog = Arc::new(
            Catalog::new(
                "test_host",
                Arc::clone(&test_store),
                Arc::clone(&time_provider),
                Default::default(),
            )
            .await
            .unwrap(),
        );

        let sys_event_store = Arc::new(SysEventStore::new(Arc::clone(&time_provider)));
        let qe = Arc::new(UnimplementedQueryExecutor);
        let persister = Arc::new(Persister::new(
            Arc::clone(&test_store),
            "test_host".to_string(),
            Arc::clone(&time_provider),
        ));
        let last_cache = LastCacheProvider::new_from_catalog(Arc::clone(&catalog) as _)
            .await
            .unwrap();
        let distinct_cache = DistinctCacheProvider::new_from_catalog(
            Arc::clone(&time_provider),
            Arc::clone(&catalog),
        )
        .await
        .unwrap();
        let shutdown = ShutdownManager::new_testing();
        let wal_config = WalConfig {
            gen1_duration: Gen1Duration::new_1m(),
            max_write_buffer_size: 100,
            flush_interval: Duration::from_millis(10),
            snapshot_size: 1,
            ..Default::default()
        };
        let wbuf = WriteBufferImpl::new(WriteBufferImplArgs {
            persister,
            catalog: Arc::clone(&catalog),
            last_cache,
            distinct_cache,
            time_provider: Arc::clone(&time_provider),
            executor: make_exec(),
            wal_config,
            parquet_cache: None,
            metric_registry: Arc::new(Registry::new()),
            snapshotted_wal_files_to_keep: 10,
            query_file_limit: None,
            shutdown: shutdown.register(),
            n_snapshots_to_load_on_start: N_SNAPSHOTS_TO_LOAD_ON_START,
            wal_replay_concurrency_limit: 1,
        })
        .await
        .unwrap();

        let pem = ProcessingEngineManagerImpl::new(
            environment_manager,
            catalog,
            "test_node",
            wbuf,
            qe,
            time_provider,
            sys_event_store,
        )
        .await;

        let result = pem.read_plugin_code("my_plugin").await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, crate::plugins::PluginError::ReadPluginError(_)),
            "Expected ReadPluginError"
        );
    }

    #[test]
    fn test_hot_reload_multifile_plugin() {
        use std::thread::sleep;

        let temp_dir = tempfile::tempdir().unwrap();
        let plugin_root = temp_dir.path().join("my_plugin");
        std::fs::create_dir(&plugin_root).unwrap();

        let init_path = plugin_root.join("__init__.py");
        std::fs::write(&init_path, "def process_writes(): pass").unwrap();

        let plugin = crate::LocalPluginDirectory {
            plugin_root: plugin_root.clone(),
            entry_point: init_path.clone(),
            last_read_and_code: Mutex::new((SystemTime::now(), Arc::from("initial"))),
        };

        let first_modified = plugin.find_latest_modified_time();
        assert!(first_modified.is_some());

        sleep(Duration::from_millis(100));

        std::fs::write(plugin_root.join("utils.py"), "def helper(): pass").unwrap();

        let second_modified = plugin.find_latest_modified_time();
        assert!(second_modified.is_some());
        assert!(
            second_modified.unwrap() > first_modified.unwrap(),
            "Modification time should be newer after adding file"
        );
    }

    #[test]
    fn test_pycache_ignored() {
        let temp_dir = tempfile::tempdir().unwrap();
        let plugin_root = temp_dir.path().join("my_plugin");
        std::fs::create_dir(&plugin_root).unwrap();

        let init_path = plugin_root.join("__init__.py");
        std::fs::write(&init_path, "def process_writes(): pass").unwrap();

        let pycache_dir = plugin_root.join("__pycache__");
        std::fs::create_dir(&pycache_dir).unwrap();
        std::fs::write(pycache_dir.join("__init__.cpython-39.pyc"), "bytecode").unwrap();

        let plugin = crate::LocalPluginDirectory {
            plugin_root: plugin_root.clone(),
            entry_point: init_path.clone(),
            last_read_and_code: Mutex::new((SystemTime::now(), Arc::from("initial"))),
        };

        let first_modified = plugin.find_latest_modified_time().unwrap();

        std::thread::sleep(Duration::from_millis(100));

        std::fs::write(pycache_dir.join("utils.cpython-39.pyc"), "more bytecode").unwrap();

        let second_modified = plugin.find_latest_modified_time().unwrap();

        assert_eq!(
            first_modified, second_modified,
            "Modification time should not change when only __pycache__ is modified"
        );
    }

    #[tokio::test]
    async fn test_atomic_directory_replacement() {
        let temp_dir = tempfile::tempdir().unwrap();
        let plugin_dir = temp_dir.path();

        // Create initial plugin directory with some files
        let initial_plugin = plugin_dir.join("test_plugin");
        std::fs::create_dir(&initial_plugin).unwrap();
        std::fs::write(initial_plugin.join("__init__.py"), "def process_v1(): pass").unwrap();
        std::fs::write(
            initial_plugin.join("old_file.py"),
            "def old_function(): pass",
        )
        .unwrap();

        let environment_manager = ProcessingEngineEnvironmentManager {
            plugin_dir: Some(plugin_dir.to_path_buf()),
            virtual_env_location: None,
            package_manager: Arc::new(DisabledManager),
            plugin_repo: None,
        };

        let start_time = Time::from_rfc3339("2024-11-14T11:00:00+00:00").unwrap();
        let time_provider: Arc<dyn TimeProvider> = Arc::new(MockProvider::new(start_time));
        let test_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let catalog = Arc::new(
            Catalog::new(
                "test_host",
                Arc::clone(&test_store),
                Arc::clone(&time_provider),
                Default::default(),
            )
            .await
            .unwrap(),
        );

        let sys_event_store = Arc::new(SysEventStore::new(Arc::clone(&time_provider)));
        let qe = Arc::new(UnimplementedQueryExecutor);
        let persister = Arc::new(Persister::new(
            Arc::clone(&test_store),
            "test_host".to_string(),
            Arc::clone(&time_provider),
        ));
        let last_cache = LastCacheProvider::new_from_catalog(Arc::clone(&catalog) as _)
            .await
            .unwrap();
        let distinct_cache = DistinctCacheProvider::new_from_catalog(
            Arc::clone(&time_provider),
            Arc::clone(&catalog),
        )
        .await
        .unwrap();
        let shutdown = ShutdownManager::new_testing();
        let wal_config = WalConfig {
            gen1_duration: Gen1Duration::new_1m(),
            max_write_buffer_size: 100,
            flush_interval: Duration::from_millis(10),
            snapshot_size: 1,
            ..Default::default()
        };
        let wbuf = WriteBufferImpl::new(WriteBufferImplArgs {
            persister,
            catalog: Arc::clone(&catalog),
            last_cache,
            distinct_cache,
            time_provider: Arc::clone(&time_provider),
            executor: make_exec(),
            wal_config,
            parquet_cache: None,
            metric_registry: Arc::new(Registry::new()),
            snapshotted_wal_files_to_keep: 10,
            query_file_limit: None,
            shutdown: shutdown.register(),
            n_snapshots_to_load_on_start: N_SNAPSHOTS_TO_LOAD_ON_START,
            wal_replay_concurrency_limit: 1,
        })
        .await
        .unwrap();

        let pem = Arc::new(
            ProcessingEngineManagerImpl::new(
                environment_manager,
                Arc::clone(&catalog),
                "test_node",
                wbuf,
                qe,
                time_provider,
                sys_event_store,
            )
            .await,
        );

        // Create the DB and trigger first
        pem.write_buffer
            .write_lp(
                NamespaceName::new("foo").unwrap(),
                "cpu,warehouse=us-east,room=01a,device=10001 reading=37\n",
                start_time,
                false,
                Precision::Nanosecond,
                false,
            )
            .await
            .unwrap();

        let plugin_filename = pem.validate_plugin_filename("test_plugin").await.unwrap();

        pem.catalog
            .create_processing_engine_trigger(
                "foo",
                "test_trigger",
                Arc::clone(&pem.node_id),
                plugin_filename,
                &TriggerSpecificationDefinition::AllTablesWalWrite.string_rep(),
                TriggerSettings::default(),
                &None,
                false,
            )
            .await
            .unwrap();

        // Prepare new files for atomic replacement
        let new_files = vec![
            (
                "__init__.py".to_string(),
                "def process_v2(): pass".to_string(),
            ),
            ("utils.py".to_string(), "def helper(): pass".to_string()),
            (
                "models/processor.py".to_string(),
                "class Processor: pass".to_string(),
            ),
        ];

        // Perform atomic replacement
        pem.replace_plugin_directory("test_trigger", new_files)
            .await
            .unwrap();

        // Verify the new directory structure
        assert!(initial_plugin.join("__init__.py").exists());
        assert!(initial_plugin.join("utils.py").exists());
        assert!(initial_plugin.join("models").join("processor.py").exists());

        // Verify old file was deleted
        assert!(!initial_plugin.join("old_file.py").exists());

        // Verify content is correct
        let init_content = std::fs::read_to_string(initial_plugin.join("__init__.py")).unwrap();
        assert_eq!(init_content, "def process_v2(): pass");

        let utils_content = std::fs::read_to_string(initial_plugin.join("utils.py")).unwrap();
        assert_eq!(utils_content, "def helper(): pass");

        // Verify old directory was cleaned up
        assert!(!plugin_dir.join("test_plugin.old").exists());
        assert!(!plugin_dir.join("test_plugin.tmp").exists());
    }
}
