use crate::environment::PythonEnvironmentManager;
use crate::manager::ProcessingEngineError;

use crate::plugins::PluginContext;
use crate::plugins::{PluginError, ProcessingEngineEnvironmentManager};
use anyhow::Context;
use bytes::Bytes;
use hashbrown::HashMap;
use influxdb3_catalog::CatalogError;
use influxdb3_catalog::catalog::Catalog;
use influxdb3_catalog::channel::CatalogUpdateReceiver;
use influxdb3_catalog::log::{
    CatalogBatch, DatabaseCatalogOp, DeleteTriggerLog, PluginType, TriggerDefinition,
    TriggerIdentifier, TriggerSpecificationDefinition, ValidPluginPath,
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
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::oneshot::Receiver;
use tokio::sync::{RwLock, mpsc, oneshot};

pub mod environment;
pub mod manager;
pub mod plugins;

pub mod virtualenv;

const ENTRY_POINT: &str = "__main__.py";

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

    pub async fn validate_plugin_name<'a>(
        &self,
        name: &'a str,
    ) -> Result<ValidPluginPath<'a>, PluginError> {
        let _ = self.read_plugin_code(name).await?;
        Ok(ValidPluginPath::from_validated_name(name))
    }

    pub async fn read_plugin_directory(&self, dir_name: &str) -> Result<PluginCode, PluginError> {
        let plugin_dir = self
            .environment_manager
            .plugin_dir
            .clone()
            .ok_or(PluginError::NoPluginDir)?;
        let plugin_path = plugin_dir.join(dir_name);

        // Verify directory exists
        if !plugin_path.is_dir() {
            return Err(PluginError::ReadPluginError(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Plugin directory not found: {}", dir_name),
            )));
        }

        // Read all Python files from the directory and combine them
        let module_code = self.read_directory_as_module(&plugin_path)?;

        Ok(PluginCode::LocalDirectory(LocalPluginDirectory {
            plugin_dir: plugin_path,
            last_read_and_code: Mutex::new((SystemTime::now(), Arc::from(module_code))),
        }))
    }

    fn read_directory_as_module(&self, path: &PathBuf) -> Result<String, PluginError> {
        let entrypoint_path = path.join(ENTRY_POINT);

        // Verify entrypoint exists
        if !entrypoint_path.exists() {
            return Err(PluginError::ReadPluginError(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Entrypoint file not found: {}", ENTRY_POINT),
            )));
        }

        // Read all .py files in the directory
        let mut module_code = String::new();
        let mut python_files = Vec::new();

        // Collect all non-entrypoint Python files
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let path = entry.path();

            if path.extension().and_then(|s| s.to_str()) == Some("py") {
                let file_name = path.file_name().unwrap().to_str().unwrap().to_string();

                // Skip the entrypoint file for now
                if file_name == ENTRY_POINT {
                    continue;
                }

                python_files.push((file_name, path));
            }
        }

        // Sort for consistent ordering
        python_files.sort_by(|a, b| a.0.cmp(&b.0));

        // Add each Python file to the module (these will define functions/classes in the same namespace)
        for (file_name, path) in &python_files {
            let content = std::fs::read_to_string(path)?;
            module_code.push_str(&format!("# File: {}\n", file_name));
            module_code.push_str(&content);
            module_code.push_str("\n\n");
        }

        // Read the entrypoint and remove imports of local modules
        let entrypoint_content = std::fs::read_to_string(&entrypoint_path)?;

        // Get list of local module names (without .py extension)
        let local_module_names: Vec<String> = python_files
            .iter()
            .map(|(name, _)| name.trim_end_matches(".py").to_string())
            .collect();

        // Filter out imports from local modules
        let filtered_entrypoint = entrypoint_content
            .lines()
            .filter(|line| {
                let trimmed = line.trim_start();
                // Skip import lines that reference local modules
                if trimmed.starts_with("from ") || trimmed.starts_with("import ") {
                    for module_name in &local_module_names {
                        if line.contains(&format!("from {}", module_name))
                            || line.contains(&format!("import {}", module_name))
                        {
                            return false; // Skip this import line
                        }
                    }
                }
                true // Keep all other lines
            })
            .collect::<Vec<_>>()
            .join("\n");

        module_code.push_str(&format!("# Entrypoint: {}\n", ENTRY_POINT));
        module_code.push_str(&filtered_entrypoint);

        Ok(module_code)
    }

    pub async fn read_plugin_code(&self, name: &str) -> Result<PluginCode, PluginError> {
        // if the name starts with gh: then we need to get it from the public github repo at https://github.com/influxdata/influxdb3_plugins/tree/main
        if name.starts_with("gh:") {
            let plugin_path = name.strip_prefix("gh:").unwrap();
            let url = format!(
                "https://raw.githubusercontent.com/influxdata/influxdb3_plugins/main/{plugin_path}"
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

        // otherwise we check if it's a local file or directory
        let plugin_dir = self
            .environment_manager
            .plugin_dir
            .clone()
            .ok_or(PluginError::NoPluginDir)?;
        let plugin_path = plugin_dir.join(name);

        if plugin_path.is_dir() {
            self.read_plugin_directory(name).await
        } else {
            // read it at least once to make sure it's there
            let code = std::fs::read_to_string(plugin_path.clone())?;

            // now we can return it
            Ok(PluginCode::Local(LocalPlugin {
                plugin_path,
                last_read_and_code: Mutex::new((SystemTime::now(), Arc::from(code))),
            }))
        }
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
            PluginCode::LocalDirectory(plugin_dir) => plugin_dir.read_module(),
        }
    }

    pub(crate) fn plugin_dir(&self) -> Option<PathBuf> {
        match self {
            PluginCode::Github(_) => None,
            PluginCode::Local(plugin) => Some(plugin.plugin_path.parent()?.to_path_buf()),
            PluginCode::LocalDirectory(plugin_dir) => Some(plugin_dir.plugin_dir.clone()),
        }
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct LocalPlugin {
    plugin_path: PathBuf,
    last_read_and_code: Mutex<(SystemTime, Arc<str>)>,
}

#[derive(Debug)]
pub struct LocalPluginDirectory {
    plugin_dir: PathBuf,
    last_read_and_code: Mutex<(SystemTime, Arc<str>)>,
}

impl LocalPluginDirectory {
    fn read_module(&self) -> Arc<str> {
        let mut last_read_and_code = self.last_read_and_code.lock();
        let (last_read, code) = &mut *last_read_and_code;

        // Check if any file in the directory has been modified
        let mut newest_modification = SystemTime::UNIX_EPOCH;

        if let Ok(entries) = std::fs::read_dir(&self.plugin_dir) {
            for entry in entries.filter_map(Result::ok) {
                if let Ok(metadata) = entry.metadata()
                    && let Ok(modified) = metadata.modified()
                    && modified > newest_modification
                {
                    newest_modification = modified;
                }
            }
        }

        // Re-read if any file was modified
        if newest_modification > *last_read {
            // Re-read the entire directory
            if let Ok(dir_entries) = std::fs::read_dir(&self.plugin_dir) {
                let mut module_code = String::new();
                let mut py_files: Vec<_> = dir_entries
                    .filter_map(Result::ok)
                    .filter(|entry| entry.path().extension().and_then(|s| s.to_str()) == Some("py"))
                    .collect();

                // Sort files to ensure consistent ordering
                py_files.sort_by_key(|entry| entry.file_name());

                for entry in py_files {
                    let path = entry.path();
                    let file_name = path.file_name().unwrap().to_str().unwrap();

                    // Process non-entrypoint files first
                    if file_name != ENTRY_POINT
                        && let Ok(content) = std::fs::read_to_string(&path)
                    {
                        module_code.push_str(&format!("# File: {}\n", file_name));
                        module_code.push_str(&content);
                        module_code.push_str("\n\n");
                    }
                }

                // Add entrypoint last
                let entrypoint_path = self.plugin_dir.join("__main__.py");
                if let Ok(entrypoint_content) = std::fs::read_to_string(&entrypoint_path) {
                    module_code.push_str("# Entrypoint: __main__.py\n");
                    module_code.push_str(&entrypoint_content);
                }

                *last_read = newest_modification;
                *code = Arc::from(module_code);
            }
        }

        Arc::clone(code)
    }
}

impl LocalPlugin {
    fn read_if_modified(&self) -> Arc<str> {
        let metadata = std::fs::metadata(&self.plugin_path);

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

    /// List all plugin files across all databases
    pub async fn list_plugin_files(&self) -> Vec<PluginFileInfo> {
        use sha2::{Digest, Sha256};
        use std::fs;

        let mut plugin_files = Vec::new();

        // Get all databases from catalog
        for db_schema in self.catalog.list_db_schema() {
            // Iterate through all triggers in this database
            for trigger in db_schema.processing_engine_triggers.resource_iter() {
                let plugin_name = Arc::<str>::clone(&trigger.trigger_name);
                debug!(
                    "Processing trigger '{}' with plugin_filename '{}'",
                    trigger.trigger_name, trigger.plugin_filename
                );

                if let Some(ref plugin_dir) = self.environment_manager.plugin_dir {
                    let plugin_path = plugin_dir.join(&trigger.plugin_filename);

                    // Check if this is a directory plugin
                    if plugin_path.is_dir() {
                        // List all files in the directory (not just Python files)
                        if let Ok(entries) = fs::read_dir(&plugin_path) {
                            for entry in entries.flatten() {
                                let path = entry.path();
                                // Include all regular files (skip directories)
                                if path.is_file()
                                    && let Ok(metadata) = entry.metadata()
                                    && let Ok(content) = fs::read(&path)
                                {
                                    let hash = format!("{:x}", Sha256::digest(&content));
                                    let file_name = path
                                        .file_name()
                                        .and_then(|n| n.to_str())
                                        .unwrap_or("unknown")
                                        .to_string();

                                    plugin_files.push(PluginFileInfo {
                                        plugin_name: Arc::<str>::clone(&plugin_name),
                                        file_name: file_name.into(),
                                        file_path: path.to_string_lossy().into(),
                                        content_hash: hash.into(),
                                        size_bytes: metadata.len() as i64,
                                        last_modified_millis: metadata
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
                    } else if let Ok(metadata) = fs::metadata(&plugin_path)
                        && let Ok(content) = fs::read(&plugin_path)
                    {
                        let hash = format!("{:x}", Sha256::digest(&content));

                        plugin_files.push(PluginFileInfo {
                            plugin_name: Arc::<str>::clone(&plugin_name),
                            file_name: trigger.plugin_filename.clone().into(),
                            file_path: plugin_path.to_string_lossy().into(),
                            content_hash: hash.into(),
                            size_bytes: metadata.len() as i64,
                            last_modified_millis: metadata
                                .modified()
                                .ok()
                                .and_then(|t| t.duration_since(SystemTime::UNIX_EPOCH).ok())
                                .map(|d| d.as_millis() as i64)
                                .unwrap_or(0),
                        });
                    }
                }
            }
        }

        plugin_files
    }

    /// Get the content of a specific plugin file
    pub async fn get_plugin_file_content(
        &self,
        plugin_name: &str,
        file_name: &str,
    ) -> Result<String, ProcessingEngineError> {
        use std::fs;

        // Find the plugin across all databases
        for db_schema in self.catalog.list_db_schema() {
            if let Some(trigger) = db_schema
                .processing_engine_triggers
                .resource_iter()
                .find(|t| t.trigger_name.as_ref() == plugin_name)
                && let Some(ref plugin_dir) = self.environment_manager.plugin_dir
            {
                let plugin_path = plugin_dir.join(&trigger.plugin_filename);

                if plugin_path.is_dir() {
                    // Directory plugin
                    let file_path = plugin_path.join(file_name);
                    return fs::read_to_string(file_path).map_err(|e| {
                        ProcessingEngineError::PluginError(plugins::PluginError::ReadPluginError(e))
                    });
                } else if trigger.plugin_filename == file_name {
                    // Single file plugin
                    return fs::read_to_string(plugin_path).map_err(|e| {
                        ProcessingEngineError::PluginError(plugins::PluginError::ReadPluginError(e))
                    });
                }
            }
        }

        Err(ProcessingEngineError::PluginError(
            plugins::PluginError::AnyhowError(anyhow::anyhow!(
                "Plugin file not found: {}/{}",
                plugin_name,
                file_name
            )),
        ))
    }

    /// Update a plugin file content and automatically reload the plugin
    pub async fn update_plugin_file(
        self: &Arc<Self>,
        plugin_name: &str,
        file_name: &str,
        content: &str,
    ) -> Result<(), ProcessingEngineError> {
        use std::fs;

        // Find the plugin across all databases
        for db_schema in self.catalog.list_db_schema() {
            if let Some(trigger) = db_schema
                .processing_engine_triggers
                .resource_iter()
                .find(|t| t.trigger_name.as_ref() == plugin_name)
                && let Some(ref plugin_dir) = self.environment_manager.plugin_dir
            {
                let plugin_path = plugin_dir.join(&trigger.plugin_filename);

                if plugin_path.is_dir() {
                    // Directory plugin
                    let file_path = plugin_path.join(file_name);
                    fs::write(file_path, content).map_err(|e| {
                        ProcessingEngineError::PluginError(plugins::PluginError::ReadPluginError(e))
                    })?;
                } else {
                    // Single file plugin - always replace the entire plugin file
                    // regardless of the incoming file_name
                    fs::write(plugin_path, content).map_err(|e| {
                        ProcessingEngineError::PluginError(plugins::PluginError::ReadPluginError(e))
                    })?;
                }

                // Automatically reload the plugin after updating
                Arc::clone(self).reload_plugin(plugin_name).await?;

                return Ok(());
            }
        }

        Err(ProcessingEngineError::PluginError(
            plugins::PluginError::AnyhowError(anyhow::anyhow!("Plugin not found: {}", plugin_name)),
        ))
    }

    /// Force reload a plugin
    pub async fn reload_plugin(
        self: Arc<Self>,
        plugin_name: &str,
    ) -> Result<(), ProcessingEngineError> {
        // Find the plugin and its database
        for db_schema in self.catalog.list_db_schema() {
            if let Some(trigger) = db_schema
                .processing_engine_triggers
                .resource_iter()
                .find(|t| t.trigger_name.as_ref() == plugin_name)
            {
                // Stop the current plugin if running
                let plugin_channels = self.plugin_event_tx.read().await;
                if let Some(rx) = plugin_channels
                    .send_shutdown(
                        db_schema.name.to_string(),
                        trigger.trigger_name.to_string(),
                        &trigger.trigger,
                    )
                    .await?
                {
                    // Wait for shutdown to complete
                    let _ = rx.await;
                }
                drop(plugin_channels);

                // Restart the plugin
                return Arc::clone(&self)
                    .run_trigger(&db_schema.name, &trigger.trigger_name)
                    .await;
            }
        }

        Err(ProcessingEngineError::PluginError(
            plugins::PluginError::AnyhowError(anyhow::anyhow!("Plugin not found: {}", plugin_name)),
        ))
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

/// Information about a plugin file
#[derive(Debug, Clone)]
pub struct PluginFileInfo {
    pub plugin_name: Arc<str>,
    pub file_name: Arc<str>,
    pub file_path: Arc<str>,
    pub content_hash: Arc<str>,
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
    use crate::environment::DisabledManager;
    use crate::plugins::ProcessingEngineEnvironmentManager;
    use crate::{PluginCode, ProcessingEngineManagerImpl};
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
    use parquet_file::storage::{ParquetStorage, StorageId};
    use std::fs;
    use std::io::Write;
    use std::num::NonZeroUsize;
    use std::ops::Deref;
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::NamedTempFile;
    use tempfile::TempDir;

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
        let file_name = pem.validate_plugin_name(file_name.as_str()).await.unwrap();

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

        let file_name = pem.validate_plugin_name(&file_name).await.unwrap();
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
        let wbuf: Arc<dyn WriteBuffer> = WriteBufferImpl::new(WriteBufferImplArgs {
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
        };

        let sys_event_store = Arc::new(SysEventStore::new(Arc::clone(&time_provider)));

        (
            ProcessingEngineManagerImpl::new(
                environment_manager,
                catalog,
                "test_node",
                Arc::clone(&wbuf),
                qe,
                time_provider,
                sys_event_store,
            )
            .await,
            file,
        )
    }

    #[tokio::test]
    async fn test_read_plugin_directory() -> Result<(), Box<dyn std::error::Error>> {
        use std::fs;
        use tempfile::TempDir;

        // Create a temporary directory with multiple Python files
        let temp_dir = TempDir::new()?;
        let plugin_dir = temp_dir.path().join("test_plugin");
        fs::create_dir(&plugin_dir)?;

        let helper_content = r#"
def helper_function(value):
    return value * 2
"#;
        fs::write(plugin_dir.join("helper.py"), helper_content)?;

        let utils_content = r#"
import datetime

def get_timestamp():
    return datetime.datetime.now()
"#;
        fs::write(plugin_dir.join("utils.py"), utils_content)?;

        let main_content = r#"
from helper import helper_function
from utils import get_timestamp

def process_writes(influxdb3_local, table_batches, args=None):
    timestamp = get_timestamp()
    for batch in table_batches:
        processed = helper_function(batch.get('value', 0))
        influxdb3_local.info(f"Processed at {timestamp}: {processed}")
"#;
        fs::write(plugin_dir.join("__main__.py"), main_content)?;

        // Create non-Python file (should be ignored)
        fs::write(plugin_dir.join("README.md"), "This is a readme")?;

        // Set up the processing engine manager
        let start_time = Time::from_rfc3339("2024-11-14T11:00:00+00:00").unwrap();
        let test_store = Arc::new(InMemory::new());
        let wal_config = WalConfig {
            gen1_duration: Gen1Duration::new_1m(),
            max_write_buffer_size: 100,
            flush_interval: Duration::from_millis(10),
            snapshot_size: 1,
            ..Default::default()
        };
        let (_pem, _file) = setup(
            start_time,
            Arc::clone(&test_store) as Arc<dyn ObjectStore>,
            wal_config,
        )
        .await;

        // Create a new ProcessingEngineManagerImpl with the test directory
        let environment_manager = ProcessingEngineEnvironmentManager {
            plugin_dir: Some(temp_dir.path().to_path_buf()),
            virtual_env_location: None,
            package_manager: Arc::new(DisabledManager),
        };

        // Reuse components from setup but with custom environment
        let time_provider: Arc<dyn TimeProvider> = Arc::new(MockProvider::new(start_time));
        let catalog = Arc::new(
            Catalog::new(
                "test_host",
                Arc::clone(&test_store) as Arc<dyn ObjectStore>,
                Arc::clone(&time_provider),
                Default::default(),
            )
            .await
            .unwrap(),
        );
        let sys_event_store = Arc::new(SysEventStore::new(Arc::clone(&time_provider)));
        let persister = Arc::new(Persister::new(
            Arc::clone(&test_store) as Arc<dyn ObjectStore>,
            "test_host".to_string(),
            Arc::clone(&time_provider),
        ));
        let shutdown = ShutdownManager::new_testing();
        let last_cache = LastCacheProvider::new_from_catalog(Arc::clone(&catalog))
            .await
            .unwrap();
        let distinct_cache = DistinctCacheProvider::new_from_catalog(
            Arc::clone(&time_provider),
            Arc::clone(&catalog),
        )
        .await
        .unwrap();
        let wbuf: Arc<dyn WriteBuffer> = WriteBufferImpl::new(WriteBufferImplArgs {
            persister: Arc::clone(&persister),
            catalog: Arc::clone(&catalog),
            last_cache,
            distinct_cache,
            time_provider: Arc::clone(&time_provider),
            executor: make_exec(),
            wal_config: WalConfig::default(),
            parquet_cache: None,
            metric_registry: Arc::new(Registry::default()),
            snapshotted_wal_files_to_keep: 10,
            query_file_limit: None,
            shutdown: shutdown.register(),
            n_snapshots_to_load_on_start: N_SNAPSHOTS_TO_LOAD_ON_START,
            wal_replay_concurrency_limit: 1,
        })
        .await
        .unwrap();
        let qe = Arc::new(UnimplementedQueryExecutor);

        let pem_with_dir = ProcessingEngineManagerImpl::new(
            environment_manager,
            catalog,
            "test_node",
            Arc::clone(&wbuf),
            qe,
            time_provider,
            sys_event_store,
        )
        .await;

        // Test reading the plugin directory
        let plugin_code = pem_with_dir.read_plugin_code("test_plugin").await?;

        // Verify the plugin code was assembled correctly
        match plugin_code {
            PluginCode::LocalDirectory(ref dir) => {
                let code_str = dir.read_module();

                // Verify all files are included
                assert!(code_str.contains("helper_function"));
                assert!(code_str.contains("get_timestamp"));
                assert!(code_str.contains("process_writes"));

                // Verify README.md is not included
                assert!(!code_str.contains("This is a readme"));

                // Verify entrypoint is last
                let main_pos = code_str.find("# Entrypoint: __main__.py").unwrap();
                let helper_pos = code_str.find("# File: helper.py").unwrap();
                let utils_pos = code_str.find("# File: utils.py").unwrap();
                assert!(main_pos > helper_pos);
                assert!(main_pos > utils_pos);
            }
            _ => panic!("Expected LocalDirectory variant"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_validate_plugin_directory() -> Result<(), Box<dyn std::error::Error>> {
        use std::fs;
        use tempfile::TempDir;

        let temp_dir = TempDir::new()?;

        // Set up the processing engine manager
        let start_time = Time::from_rfc3339("2024-11-14T11:00:00+00:00").unwrap();
        let test_store = Arc::new(InMemory::new());
        let _wal_config = WalConfig {
            gen1_duration: Gen1Duration::new_1m(),
            max_write_buffer_size: 100,
            flush_interval: Duration::from_millis(10),
            snapshot_size: 1,
            ..Default::default()
        };

        // Set up a new ProcessingEngineManagerImpl with the test directory
        let time_provider: Arc<dyn TimeProvider> = Arc::new(MockProvider::new(start_time));
        let catalog = Arc::new(
            Catalog::new(
                "test_host",
                Arc::clone(&test_store) as Arc<dyn ObjectStore>,
                Arc::clone(&time_provider),
                Default::default(),
            )
            .await
            .unwrap(),
        );
        let sys_event_store = Arc::new(SysEventStore::new(Arc::clone(&time_provider)));
        let persister = Arc::new(Persister::new(
            Arc::clone(&test_store) as Arc<dyn ObjectStore>,
            "test_host".to_string(),
            Arc::clone(&time_provider),
        ));
        let shutdown = ShutdownManager::new_testing();
        let last_cache = LastCacheProvider::new_from_catalog(Arc::clone(&catalog))
            .await
            .unwrap();
        let distinct_cache = DistinctCacheProvider::new_from_catalog(
            Arc::clone(&time_provider),
            Arc::clone(&catalog),
        )
        .await
        .unwrap();
        let wbuf: Arc<dyn WriteBuffer> = WriteBufferImpl::new(WriteBufferImplArgs {
            persister: Arc::clone(&persister),
            catalog: Arc::clone(&catalog),
            last_cache,
            distinct_cache,
            time_provider: Arc::clone(&time_provider),
            executor: make_exec(),
            wal_config: WalConfig::default(),
            parquet_cache: None,
            metric_registry: Arc::new(Registry::default()),
            snapshotted_wal_files_to_keep: 10,
            query_file_limit: None,
            shutdown: shutdown.register(),
            n_snapshots_to_load_on_start: N_SNAPSHOTS_TO_LOAD_ON_START,
            wal_replay_concurrency_limit: 1,
        })
        .await
        .unwrap();
        let qe = Arc::new(UnimplementedQueryExecutor);

        let pem_with_dir = ProcessingEngineManagerImpl::new(
            ProcessingEngineEnvironmentManager {
                plugin_dir: Some(temp_dir.path().to_path_buf()),
                virtual_env_location: None,
                package_manager: Arc::new(DisabledManager),
            },
            catalog,
            "test_node",
            Arc::clone(&wbuf),
            qe,
            time_provider,
            sys_event_store,
        )
        .await;

        // Test 1: Valid directory with entrypoint
        let valid_dir = temp_dir.path().join("valid_plugin");
        fs::create_dir(&valid_dir)?;
        fs::write(valid_dir.join("__main__.py"), "def process_writes(): pass")?;

        let result = pem_with_dir.validate_plugin_name("valid_plugin").await;
        assert!(result.is_ok());
        let validated = result.unwrap();
        assert_eq!(validated.deref(), "valid_plugin");

        // Test 2: Non-existent directory
        let result = pem_with_dir.validate_plugin_name("nonexistent").await;
        assert!(result.is_err());

        // Test 3: Missing entrypoint
        let no_entrypoint_dir = temp_dir.path().join("no_entrypoint");
        fs::create_dir(&no_entrypoint_dir)?;
        fs::write(no_entrypoint_dir.join("other.py"), "def foo(): pass")?;

        let result = pem_with_dir.validate_plugin_name("no_entrypoint").await;
        assert!(result.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_plugin_directory_file_changes() -> Result<(), Box<dyn std::error::Error>> {
        use std::fs;
        use tempfile::TempDir;
        use tokio::time::sleep;

        let temp_dir = TempDir::new()?;
        let plugin_dir = temp_dir.path().join("changing_plugin");
        fs::create_dir(&plugin_dir)?;

        // Initial file content
        let initial_content = r#"
def process_writes(influxdb3_local, table_batches, args=None):
    influxdb3_local.info("version 1")
"#;
        fs::write(plugin_dir.join("__main__.py"), initial_content)?;

        // Set up the processing engine manager
        let start_time = Time::from_rfc3339("2024-11-14T11:00:00+00:00").unwrap();
        let test_store = Arc::new(InMemory::new());
        let _wal_config = WalConfig::default();

        // Set up a new ProcessingEngineManagerImpl with the test directory
        let time_provider: Arc<dyn TimeProvider> = Arc::new(MockProvider::new(start_time));
        let catalog = Arc::new(
            Catalog::new(
                "test_host",
                Arc::clone(&test_store) as Arc<dyn ObjectStore>,
                Arc::clone(&time_provider),
                Default::default(),
            )
            .await
            .unwrap(),
        );
        let sys_event_store = Arc::new(SysEventStore::new(Arc::clone(&time_provider)));
        let persister = Arc::new(Persister::new(
            Arc::clone(&test_store) as Arc<dyn ObjectStore>,
            "test_host".to_string(),
            Arc::clone(&time_provider),
        ));
        let shutdown = ShutdownManager::new_testing();
        let last_cache = LastCacheProvider::new_from_catalog(Arc::clone(&catalog))
            .await
            .unwrap();
        let distinct_cache = DistinctCacheProvider::new_from_catalog(
            Arc::clone(&time_provider),
            Arc::clone(&catalog),
        )
        .await
        .unwrap();
        let wbuf: Arc<dyn WriteBuffer> = WriteBufferImpl::new(WriteBufferImplArgs {
            persister: Arc::clone(&persister),
            catalog: Arc::clone(&catalog),
            last_cache,
            distinct_cache,
            time_provider: Arc::clone(&time_provider),
            executor: make_exec(),
            wal_config: WalConfig::default(),
            parquet_cache: None,
            metric_registry: Arc::new(Registry::default()),
            snapshotted_wal_files_to_keep: 10,
            query_file_limit: None,
            shutdown: shutdown.register(),
            n_snapshots_to_load_on_start: N_SNAPSHOTS_TO_LOAD_ON_START,
            wal_replay_concurrency_limit: 1,
        })
        .await
        .unwrap();
        let qe = Arc::new(UnimplementedQueryExecutor);

        let pem_with_dir = ProcessingEngineManagerImpl::new(
            ProcessingEngineEnvironmentManager {
                plugin_dir: Some(temp_dir.path().to_path_buf()),
                virtual_env_location: None,
                package_manager: Arc::new(DisabledManager),
            },
            catalog,
            "test_node",
            Arc::clone(&wbuf),
            qe,
            time_provider,
            sys_event_store,
        )
        .await;

        // Read the plugin initially
        let plugin_code = pem_with_dir.read_plugin_code("changing_plugin").await?;

        match plugin_code {
            PluginCode::LocalDirectory(ref dir) => {
                let initial_code = dir.read_module();
                assert!(initial_code.contains("version 1"));

                // Sleep briefly to ensure file modification time differs
                sleep(Duration::from_millis(10)).await;

                // Modify the file
                let updated_content = r#"
def process_writes(influxdb3_local, table_batches, args=None):
    influxdb3_local.info("version 2")
"#;
                fs::write(plugin_dir.join("__main__.py"), updated_content)?;

                // Read again - should detect the change
                let updated_code = dir.read_module();
                assert!(updated_code.contains("version 2"));
                assert!(!updated_code.contains("version 1"));
            }
            _ => panic!("Expected LocalDirectory variant"),
        }

        Ok(())
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

    #[test]
    fn test_read_directory_as_module() {
        // Create a temporary directory with multiple Python files
        let temp_dir = TempDir::new().unwrap();
        let plugin_dir = temp_dir.path().join("test_plugin");
        fs::create_dir(&plugin_dir).unwrap();

        let helper_content = r#"
def helper_function(value):
    return value * 2
"#;
        fs::write(plugin_dir.join("helper.py"), helper_content).unwrap();

        let utils_content = r#"
import datetime

def get_timestamp():
    return datetime.datetime.now()
"#;
        fs::write(plugin_dir.join("utils.py"), utils_content).unwrap();

        let main_content = r#"
from helper import helper_function
from utils import get_timestamp

def process_writes(influxdb3_local, table_batches, args=None):
    timestamp = get_timestamp()
    for batch in table_batches:
        processed = helper_function(batch.get('value', 0))
        influxdb3_local.info(f"Processed at {timestamp}: {processed}")
"#;
        fs::write(plugin_dir.join("__main__.py"), main_content).unwrap();

        // Create non-Python file (should be ignored)
        fs::write(plugin_dir.join("README.md"), "This is a readme").unwrap();

        // Test reading all Python files from the directory
        let mut module_code = String::new();

        // Read all .py files in the directory
        for entry in fs::read_dir(&plugin_dir).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();

            if path.extension().and_then(|s| s.to_str()) == Some("py") {
                let file_name = path.file_name().unwrap().to_str().unwrap();

                // Skip the entrypoint file for now
                if file_name == "__main__.py" {
                    continue;
                }

                // Add the file content as part of the module
                let content = fs::read_to_string(&path).unwrap();
                module_code.push_str(&format!("# File: {}\n", file_name));
                module_code.push_str(&content);
                module_code.push_str("\n\n");
            }
        }

        // Finally, add the entrypoint file
        let entrypoint_path = plugin_dir.join("__main__.py");
        let entrypoint_content = fs::read_to_string(&entrypoint_path).unwrap();
        module_code.push_str("# Entrypoint: __main__.py\n");
        module_code.push_str(&entrypoint_content);

        // Verify all files are included
        assert!(module_code.contains("helper_function"));
        assert!(module_code.contains("get_timestamp"));
        assert!(module_code.contains("process_writes"));

        // Verify README.md is not included
        assert!(!module_code.contains("This is a readme"));

        // Verify entrypoint is last
        let main_pos = module_code.find("# Entrypoint: __main__.py").unwrap();
        let helper_pos = module_code.find("# File: helper.py").unwrap_or(usize::MAX);
        let utils_pos = module_code.find("# File: utils.py").unwrap_or(usize::MAX);

        if helper_pos != usize::MAX {
            assert!(main_pos > helper_pos);
        }
        if utils_pos != usize::MAX {
            assert!(main_pos > utils_pos);
        }
    }
}
