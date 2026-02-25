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
use influxdb3_write::Bufferer;
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

use std::path::Path;

/// Validates that a user-provided path stays within the plugin directory.
/// Prevents path traversal attacks via "..", absolute paths, and symlinks.
fn validate_path_within_plugin_dir(
    plugin_dir: &Path,
    user_path: &str,
) -> Result<PathBuf, PluginError> {
    // 1. Check for "..", absolute path components, and Windows prefixes (C:\, \\server\share)
    let normalized_path = Path::new(user_path);
    for component in normalized_path.components() {
        match component {
            std::path::Component::ParentDir
            | std::path::Component::RootDir
            | std::path::Component::Prefix(_) => {
                return Err(PluginError::PathTraversal(user_path.to_string()));
            }
            _ => {}
        }
    }

    // 2. Build target path and canonicalize for symlink protection
    let target_path = plugin_dir.join(user_path);
    let canonical_plugin_dir = plugin_dir.canonicalize()?;

    // 3. Handle non-existent files by canonicalizing deepest existing ancestor
    let canonical_target_path = if target_path.exists() {
        target_path.canonicalize()?
    } else {
        // Find deepest existing ancestor, canonicalize, append missing components
        let mut existing = target_path.as_path();
        let mut missing = Vec::new();
        while !existing.exists() {
            missing.push(
                existing
                    .file_name()
                    .ok_or_else(|| PluginError::PathTraversal(user_path.to_string()))?,
            );
            existing = existing
                .parent()
                .ok_or_else(|| PluginError::PathTraversal(user_path.to_string()))?;
        }
        let mut canonical = existing.canonicalize()?;
        for c in missing.into_iter().rev() {
            canonical.push(c);
        }
        canonical
    };

    // 4. Verify target is within plugin directory
    if !canonical_target_path.starts_with(&canonical_plugin_dir) {
        return Err(PluginError::PathTraversal(user_path.to_string()));
    }

    Ok(target_path)
}

pub mod virtualenv;

#[derive(Debug)]
pub struct ProcessingEngineManagerImpl {
    environment_manager: ProcessingEngineEnvironmentManager,
    catalog: Arc<Catalog>,
    node_id: Arc<str>,
    write_buffer: Arc<dyn Bufferer>,
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
        write_buffer: Arc<dyn Bufferer>,
        query_executor: Arc<dyn QueryExecutor>,
        time_provider: Arc<dyn TimeProvider>,
        sys_event_store: Arc<SysEventStore>,
    ) -> Result<Arc<Self>, environment::PluginEnvironmentError> {
        // if given a plugin dir, try to initialize the virtualenv.
        if environment.plugin_dir.is_some() {
            {
                environment.package_manager.init_pyenv(
                    environment.plugin_dir.as_deref(),
                    environment.virtual_env_location.as_ref(),
                )?;
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

        Ok(pem)
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

        // Validate path stays within plugin directory (prevents path traversal via .., absolute paths, symlinks)
        let plugin_path = validate_path_within_plugin_dir(&plugin_dir, plugin_name)?;

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
                        error!(plugin_path = ?self.plugin_path, "error reading plugin file");
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
                error!(entry_point = ?self.entry_point, "error reading plugin entry point");
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
                    "Not running trigger {}, as it is configured for node id {}. Multi-node not supported in core.",
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

    /// dry_run_wal_plugin doesn't write data to the DB but it does perform
    /// real queries. If the plugin under test does other actions with side
    /// effects those will be real too.
    pub async fn dry_run_wal_plugin(
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
                plugins::run_dry_run_wal_plugin(
                    now,
                    catalog,
                    query_executor,
                    Arc::new(plugins::DryRunBufferer::new()),
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
                plugins::run_dry_run_schedule_plugin(
                    now,
                    catalog,
                    query_executor,
                    Arc::new(plugins::DryRunBufferer::new()),
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
            error!(error = %e, "error receiving response from plugin");
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

        let plugin_path = validate_path_within_plugin_dir(plugin_dir, plugin_filename)?;

        // Create parent directories if they don't exist (for multi-file plugins)
        if let Some(parent) = plugin_path.parent() {
            async_fs::create_dir_all(parent).await.map_err(|e| {
                ProcessingEngineError::PluginError(plugins::PluginError::ReadPluginError(e))
            })?;
        }

        async_fs::write(&plugin_path, content).await.map_err(|e| {
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
                // Validate path stays within plugin directory
                let plugin_path =
                    validate_path_within_plugin_dir(plugin_dir, &trigger.plugin_filename)?;

                // For single-file plugins, update the file directly
                if !plugin_path.is_dir() {
                    async_fs::write(&plugin_path, content).await.map_err(|e| {
                        ProcessingEngineError::PluginError(plugins::PluginError::ReadPluginError(e))
                    })?;

                    return Ok(db_schema.name.to_string());
                }

                // For multi-file plugins (directories), update __init__.py by default
                let init_file = plugin_path.join(INIT_PY);
                async_fs::write(&init_file, content).await.map_err(|e| {
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

        // Validate all paths stay within plugin directory
        let plugin_path = validate_path_within_plugin_dir(plugin_dir, &plugin_filename)?;
        let temp_suffix = format!("{}.tmp", plugin_filename);
        let old_suffix = format!("{}.old", plugin_filename);
        let temp_path = validate_path_within_plugin_dir(plugin_dir, &temp_suffix)?;
        let old_path = validate_path_within_plugin_dir(plugin_dir, &old_suffix)?;

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
            let file_path = validate_path_within_plugin_dir(&temp_path, &relative_path)?;

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
mod tests;
