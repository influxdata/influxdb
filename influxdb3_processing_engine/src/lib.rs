use crate::environment::PythonEnvironmentManager;
use crate::manager::ProcessingEngineError;
use influxdb3_processing_engine_telemetry::PluginTriggerInvocationRegistry;

use crate::plugins::{PluginError, ProcessingEngineEnvironmentManager};
use anyhow::{Context, anyhow};
use bytes::Bytes;
use hashbrown::HashMap;
use influxdb3_catalog::catalog::{
    Catalog, CatalogEvent, CatalogUpdateReceiver, NodeSpec, PluginType,
    TriggerSpecificationDefinition, ValidPluginFilename,
};
use influxdb3_id::{DbId, TriggerId};
use influxdb3_py_api::cache::CacheStore;
use influxdb3_py_api::logging::PROCESSING_ENGINE_LOGS_TABLE_NAME;
use influxdb3_py_api::query::QueryEndpoint;
use influxdb3_py_api::wal::WalFlushElement;
use influxdb3_py_api::write::WriteEndpoint;
use influxdb3_shutdown::ShutdownToken;
use influxdb3_types::http::{
    SchedulePluginTestRequest, SchedulePluginTestResponse, WalPluginTestRequest,
    WalPluginTestResponse,
};
use iox_http_util::Response;
use iox_time::TimeProvider;
use observability_deps::tracing::{debug, error, info, warn};
use parking_lot::Mutex;
use std::fs;
use std::io::{Error as IoError, ErrorKind};
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::fs as async_fs;
use tokio::sync::{RwLock, oneshot};
use tokio_util::sync::CancellationToken;

pub mod environment;
pub mod logging;
pub mod manager;
pub mod plugins;
pub mod query;
mod scheduler;
mod scheduler_worker_protocol;
mod wal;
mod worker;
pub mod write;

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
    query_endpoint: Arc<dyn QueryEndpoint>,
    time_provider: Arc<dyn TimeProvider>,
    cache: Arc<Mutex<CacheStore>>,
    scheduler: scheduler::Scheduler,
    /// Maximum concurrent invocations per `run_async` trigger; `NonZeroUsize::MAX`
    /// means unlimited.
    async_trigger_concurrency_limit: NonZeroUsize,
    trigger_registry: RwLock<TriggerRegistry>,
    /// Cancelled when the server begins shutting down. Passed into each plugin
    /// execution so that long-running plugins are interrupted and cannot block
    /// graceful shutdown (see influxdb_pro#2444).
    plugin_shutdown: CancellationToken,
}

#[derive(Debug, Clone)]
pub struct ProcessingEngineManagerOptions {
    pub plugin_trigger_invocation_registry: Option<Arc<PluginTriggerInvocationRegistry>>,
    /// Maximum concurrent invocations per `run_async` trigger; `NonZeroUsize::MAX`
    /// means unlimited.
    pub async_trigger_concurrency_limit: NonZeroUsize,
}

impl Default for ProcessingEngineManagerOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl ProcessingEngineManagerOptions {
    pub fn new() -> Self {
        Self {
            plugin_trigger_invocation_registry: None,
            async_trigger_concurrency_limit: NonZeroUsize::MAX,
        }
    }

    pub fn with_plugin_trigger_invocation_registry(
        mut self,
        plugin_trigger_invocation_registry: Option<Arc<PluginTriggerInvocationRegistry>>,
    ) -> Self {
        self.plugin_trigger_invocation_registry = plugin_trigger_invocation_registry;
        self
    }

    pub fn with_async_trigger_concurrency_limit(
        mut self,
        async_trigger_concurrency_limit: NonZeroUsize,
    ) -> Self {
        self.async_trigger_concurrency_limit = async_trigger_concurrency_limit;
        self
    }
}

#[derive(Debug, Clone)]
enum WalRouteFilter {
    AllTables,
    SingleTable(String),
}

#[derive(Debug, Clone)]
struct WalRoute {
    database_name: Arc<str>,
    filter: WalRouteFilter,
}

impl WalRoute {
    fn matches(&self, database_name: &str, wal_contents: &[WalFlushElement]) -> bool {
        if self.database_name.as_ref() != database_name {
            return false;
        }

        match &self.filter {
            WalRouteFilter::AllTables => !wal_contents.is_empty(),
            WalRouteFilter::SingleTable(table_name) => wal_contents
                .iter()
                .any(|element| element.table_name.as_ref() == table_name),
        }
    }
}

#[derive(Debug, Clone)]
enum TriggerRoute {
    Wal(WalRoute),
    Request { path: String },
}

#[derive(Debug, Default)]
struct TriggerRegistry {
    /// Routing metadata for triggers that receive externally supplied events.
    routes: HashMap<scheduler::TriggerKey, TriggerRoute>,
    /// Secondary index: HTTP request triggers are routed by URL path, which
    /// is all the request handler knows. Maps path -> trigger key.
    request_paths: HashMap<String, scheduler::TriggerKey>,
}

/// Nominal per-trigger outstanding-invocations capacity. For async triggers a
/// configured concurrency limit above this raises the capacity to the limit
/// (see [`scheduler::SchedulerConfig`]).
pub const TRIGGER_QUEUE_SIZE: usize = 60;

impl TriggerRegistry {
    fn remove_request_path_for_route(&mut self, key: scheduler::TriggerKey, route: TriggerRoute) {
        if let TriggerRoute::Request { path } = route
            && self
                .request_paths
                .get(&path)
                .is_some_and(|mapped| *mapped == key)
        {
            self.request_paths.remove(&path);
        }
    }

    fn insert_route(&mut self, key: scheduler::TriggerKey, route: TriggerRoute) {
        if let Some(old_route) = self.routes.insert(key, route) {
            self.remove_request_path_for_route(key, old_route);
        }
    }

    fn remove_trigger(&mut self, key: scheduler::TriggerKey) {
        if let Some(route) = self.routes.remove(&key) {
            self.remove_request_path_for_route(key, route);
        }
    }

    fn remove_all_for_db(&mut self, db_id: DbId) {
        let keys: Vec<_> = self
            .routes
            .keys()
            .filter(|key| key.db_id == db_id)
            .copied()
            .collect();
        for key in keys {
            self.remove_trigger(key);
        }
    }

    fn add_wal_trigger(
        &mut self,
        key: scheduler::TriggerKey,
        database_name: Arc<str>,
        filter: WalRouteFilter,
    ) {
        self.insert_route(
            key,
            TriggerRoute::Wal(WalRoute {
                database_name,
                filter,
            }),
        );
    }

    fn add_request_trigger(&mut self, key: scheduler::TriggerKey, path: String) {
        self.insert_route(key, TriggerRoute::Request { path: path.clone() });
        self.request_paths.insert(path, key);
    }

    fn wal_invocations(
        &self,
        database_name: Arc<str>,
        wal_contents: Arc<[WalFlushElement]>,
    ) -> Vec<scheduler::TriggerInvocation> {
        let wal_contents: Arc<[WalFlushElement]> = wal_contents
            .iter()
            .filter(|element| element.table_name.as_ref() != PROCESSING_ENGINE_LOGS_TABLE_NAME)
            .cloned()
            .collect::<Vec<_>>()
            .into();
        if wal_contents.is_empty() {
            return Vec::new();
        }

        self.routes
            .iter()
            .filter_map(|(key, route)| {
                let TriggerRoute::Wal(route) = route else {
                    return None;
                };
                if !route.matches(&database_name, &wal_contents) {
                    return None;
                }
                Some(scheduler::TriggerInvocation::new(
                    *key,
                    scheduler::TriggerPayload::Wal {
                        database_name: Arc::clone(&database_name),
                        wal_contents: Arc::clone(&wal_contents),
                    },
                ))
            })
            .collect()
    }

    fn request_invocation(
        &self,
        trigger_path: &str,
        payload: scheduler::RequestPayload,
    ) -> Result<scheduler::TriggerInvocation, ProcessingEngineError> {
        let Some(key) = self.request_paths.get(trigger_path).copied() else {
            return Err(ProcessingEngineError::RequestTriggerNotFound);
        };
        let Some(TriggerRoute::Request { .. }) = self.routes.get(&key) else {
            return Err(ProcessingEngineError::RequestTriggerNotFound);
        };

        let payload = scheduler::TriggerPayload::Request(payload);
        Ok(scheduler::TriggerInvocation::new(key, payload))
    }
}

impl ProcessingEngineManagerImpl {
    async fn enqueue_wal_invocations(
        scheduler: &scheduler::Scheduler,
        invocations: Vec<scheduler::TriggerInvocation>,
    ) {
        for invocation in invocations {
            let key = invocation.key;
            if let Err(e) = scheduler.enqueue(invocation).await {
                warn!(%e, ?key, "error sending wal contents to plugin");
            }
        }
    }

    pub async fn new(
        environment: ProcessingEngineEnvironmentManager,
        catalog: Arc<Catalog>,
        node_id: impl Into<Arc<str>>,
        write_endpoint: Arc<dyn WriteEndpoint>,
        query_endpoint: Arc<dyn QueryEndpoint>,
        time_provider: Arc<dyn TimeProvider>,
    ) -> Result<Arc<Self>, environment::PluginEnvironmentError> {
        Self::new_with_options(
            environment,
            catalog,
            node_id,
            write_endpoint,
            query_endpoint,
            time_provider,
            ProcessingEngineManagerOptions::new(),
        )
        .await
    }

    pub async fn new_with_options(
        environment: ProcessingEngineEnvironmentManager,
        catalog: Arc<Catalog>,
        node_id: impl Into<Arc<str>>,
        write_endpoint: Arc<dyn WriteEndpoint>,
        query_endpoint: Arc<dyn QueryEndpoint>,
        time_provider: Arc<dyn TimeProvider>,
        options: ProcessingEngineManagerOptions,
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

        let node_id = node_id.into();
        let plugin_trigger_invocation_registry = options.plugin_trigger_invocation_registry;
        let async_trigger_concurrency_limit = options.async_trigger_concurrency_limit;
        if async_trigger_concurrency_limit == NonZeroUsize::MAX {
            info!("async trigger concurrency: unlimited");
        } else {
            info!(
                limit = async_trigger_concurrency_limit.get(),
                "async trigger concurrency limit configured"
            );
        }
        let plugin_shutdown = CancellationToken::new();
        let worker = worker::local::make_trigger_worker(worker::local::TriggerWorkerContext {
            environment_manager: environment.clone(),
            catalog: Arc::clone(&catalog),
            node_id: Arc::clone(&node_id),
            write_endpoint: Arc::clone(&write_endpoint),
            query_endpoint: Arc::clone(&query_endpoint),
            time_provider: Arc::clone(&time_provider),
            cache: Arc::clone(&cache),
            plugin_shutdown: plugin_shutdown.clone(),
            plugin_trigger_invocation_registry: plugin_trigger_invocation_registry.clone(),
        });
        let scheduler = scheduler::Scheduler::new(Arc::clone(&node_id), |scheduler| {
            worker.register_scheduler(scheduler);
            vec![worker]
        });
        let pem = Arc::new(Self {
            environment_manager: environment,
            catalog,
            node_id,
            query_endpoint,
            time_provider,
            scheduler,
            async_trigger_concurrency_limit,
            trigger_registry: Default::default(),
            cache,
            plugin_shutdown,
        });

        background_catalog_update(Arc::clone(&pem), catalog_sub);

        Ok(pem)
    }

    pub fn node_id(&self) -> Arc<str> {
        Arc::clone(&self.node_id)
    }

    /// Token passed into running plugins; cancel it (via [`Self::shutdown_plugins`])
    /// to interrupt long-running plugins during graceful shutdown.
    pub fn plugin_shutdown_token(&self) -> CancellationToken {
        self.plugin_shutdown.clone()
    }

    /// Signal all running plugins to abort, so they cannot block graceful shutdown.
    fn shutdown_plugins(&self) {
        self.plugin_shutdown.cancel();
    }

    /// Spawn a task that interrupts running plugins when the server begins shutting
    /// down. The given [`ShutdownToken`] is obtained from the server's shutdown
    /// manager; when it fires, all in-flight plugin executions are signalled to abort
    /// (so a long-running request/schedule plugin cannot block graceful shutdown — see
    /// influxdb_pro#2444), then the token is marked complete.
    ///
    /// Plugins observe the abort signal via the host API (`influxdb3_local.query`,
    /// `.write_sync`, etc.), which raises `KeyboardInterrupt` — a `BaseException` that a
    /// plugin's `except Exception` handler cannot swallow. A plugin wedged inside a single
    /// uninterruptible call cannot observe it, so in-flight request plugins also reply
    /// immediately on shutdown (see `process_request`) and their abandoned `spawn_blocking`
    /// thread is released when the server shuts the io runtime down in the background.
    pub fn shutdown_plugins_on(self: &Arc<Self>, shutdown: ShutdownToken) {
        let pe = Arc::clone(self);
        tokio::spawn(async move {
            shutdown.wait_for_shutdown().await;
            pe.shutdown_plugins();
            shutdown.complete();
        });
    }

    pub async fn validate_plugin_filename<'a>(
        &self,
        name: &'a str,
    ) -> Result<ValidPluginFilename<'a>, PluginError> {
        let _ = self.read_plugin_code(name).await?;
        Ok(ValidPluginFilename::from_validated_name(name))
    }

    pub async fn read_plugin_code(&self, name: &str) -> Result<PluginCode, PluginError> {
        read_plugin_code(&self.environment_manager, name).await
    }
}

pub(crate) async fn read_plugin_code(
    environment_manager: &ProcessingEngineEnvironmentManager,
    name: &str,
) -> Result<PluginCode, PluginError> {
    // if the name starts with gh: then we use the custom repo if set or we need to get it from
    // the public github repo at https://github.com/influxdata/influxdb3_plugins/tree/main
    if name.starts_with("gh:") {
        if environment_manager.plugin_dir_only {
            return Err(PluginError::PluginInstallationDisabled);
        }

        let plugin_path = name.strip_prefix("gh:").unwrap();
        let plugin_repo = environment_manager
            .plugin_repo
            .as_deref()
            .unwrap_or("https://raw.githubusercontent.com/influxdata/influxdb3_plugins/main/");

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
    let plugin_dir = environment_manager
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
    async fn run_trigger(
        self: Arc<Self>,
        db_id: DbId,
        trigger_id: TriggerId,
    ) -> Result<(), ProcessingEngineError> {
        {
            let db_schema = self
                .catalog
                .db_schema_by_id(&db_id)
                .ok_or_else(|| ProcessingEngineError::DatabaseNotFound(db_id.to_string()))?;
            let db_name = Arc::clone(&db_schema.name);
            let trigger = db_schema
                .processing_engine_triggers
                .get_by_id(&trigger_id)
                .ok_or(ProcessingEngineError::TriggerNotFound { db_id, trigger_id })?;
            debug!(%db_name, trigger_name = trigger.trigger_name.as_ref(), "starting trigger");

            // OSS does not support multi-node; only run triggers whose node
            // specification targets every node.
            if !matches!(trigger.node_spec, NodeSpec::All) {
                error!(
                    trigger_name = trigger.trigger_name.as_ref(),
                    "not running trigger with an enterprise node specification"
                );
                return Ok(());
            }
            if self.environment_manager.plugin_dir.is_none() {
                info!(
                    trigger_name = trigger.trigger_name.as_ref(),
                    node_id = self.node_id.as_ref(),
                    "not running trigger because no plugin directory is configured"
                );
                return Ok(());
            }

            // Per-trigger scheduler cancellation token, a child of the node-wide
            // `plugin_shutdown` token: cancelled on full node shutdown (via the
            // parent) or on its own when this trigger is disabled/force-deleted.
            // The scheduler uses it to stop queueing and to drop in-flight worker
            // attempts; the worker owns any attempt-local plugin cancellation.
            let cancel = self.plugin_shutdown.child_token();
            let key = scheduler::TriggerKey { db_id, trigger_id };
            let scheduler_config = scheduler::SchedulerConfig::new(
                TRIGGER_QUEUE_SIZE,
                trigger.trigger_settings.run_async,
                self.async_trigger_concurrency_limit,
            );
            let auto_disable = {
                let manager = Arc::clone(&self);
                let trigger = Arc::clone(&trigger);
                Arc::new(move || {
                    let manager = Arc::clone(&manager);
                    let trigger = Arc::clone(&trigger);
                    Box::pin(async move { manager.disable_trigger_from_scheduler(trigger).await })
                        as scheduler::AutoDisableFuture
                }) as scheduler::AutoDisable
            };
            self.scheduler
                .register_trigger(scheduler::TriggerRegistration {
                    key,
                    trigger_definition: Arc::clone(&trigger),
                    cancel: cancel.clone(),
                    config: scheduler_config,
                    auto_disable,
                })
                .await;

            match trigger.trigger.plugin_type() {
                PluginType::WalRows => {
                    let filter = match &trigger.trigger {
                        TriggerSpecificationDefinition::AllTablesWalWrite => {
                            WalRouteFilter::AllTables
                        }
                        TriggerSpecificationDefinition::SingleTableWalWrite { table_name } => {
                            WalRouteFilter::SingleTable(table_name.clone())
                        }
                        _ => unreachable!(),
                    };
                    self.trigger_registry.write().await.add_wal_trigger(
                        key,
                        Arc::clone(&trigger.database_name),
                        filter,
                    );
                }
                PluginType::Schedule => plugins::run_schedule_event_source(
                    Arc::clone(&trigger),
                    Arc::clone(&self.time_provider),
                    self.scheduler.clone(),
                    key,
                    cancel,
                )?,
                PluginType::Request => {
                    let TriggerSpecificationDefinition::RequestPath { path } = &trigger.trigger
                    else {
                        unreachable!()
                    };
                    self.trigger_registry
                        .write()
                        .await
                        .add_request_trigger(key, path.to_string());
                }
            }
        }

        Ok(())
    }

    pub(crate) async fn disable_trigger_from_scheduler(
        &self,
        trigger_definition: Arc<influxdb3_catalog::catalog::TriggerDefinition>,
    ) -> bool {
        if let Err(error) = self
            .catalog
            .disable_processing_engine_trigger(
                trigger_definition.database_name.as_ref(),
                trigger_definition.trigger_name.as_ref(),
            )
            .await
        {
            warn!(%error, ?trigger_definition, "failed to persist trigger auto-disable");
            return false;
        }
        true
    }

    async fn stop_trigger(
        &self,
        db_id: DbId,
        trigger_id: TriggerId,
    ) -> Result<(), ProcessingEngineError> {
        let key = scheduler::TriggerKey { db_id, trigger_id };
        self.scheduler.shutdown_trigger(key).await;
        self.trigger_registry.write().await.remove_trigger(key);
        self.cache.lock().drop_trigger_cache(db_id, trigger_id);

        Ok(())
    }

    pub async fn start_triggers(self: Arc<Self>) {
        for (db_id, trigger_id) in self.catalog.active_triggers() {
            if let Err(error) = Arc::clone(&self).run_trigger(db_id, trigger_id).await {
                error!(
                    ?error,
                    ?db_id,
                    ?trigger_id,
                    "failed to start trigger at boot; continuing in degraded state"
                );
            }
        }
    }

    /// dry_run_wal_plugin doesn't write data to the DB but it does perform
    /// real queries. If the plugin under test does other actions with side
    /// effects those will be real too.
    pub async fn dry_run_wal_plugin(
        &self,
        request: WalPluginTestRequest,
    ) -> Result<WalPluginTestResponse, plugins::PluginError> {
        {
            let catalog = Arc::clone(&self.catalog);
            let now = self.time_provider.now();
            let query_endpoint = Arc::clone(&self.query_endpoint);

            let code = self.read_plugin_code(&request.filename).await?;
            let cache = Arc::clone(&self.cache);
            let code_string = code.code().to_string();

            let res = tokio::task::spawn_blocking(move || {
                plugins::run_dry_run_wal_plugin(
                    now,
                    catalog,
                    query_endpoint,
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
    ) -> Result<SchedulePluginTestResponse, PluginError> {
        {
            let catalog = Arc::clone(&self.catalog);
            let now = self.time_provider.now();
            let query_endpoint = Arc::clone(&self.query_endpoint);

            let code = self.read_plugin_code(&request.filename).await?;
            let code_string = code.code().to_string();
            let cache = Arc::clone(&self.cache);

            let res = tokio::task::spawn_blocking(move || {
                plugins::run_dry_run_schedule_plugin(
                    now,
                    catalog,
                    query_endpoint,
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
        let payload =
            scheduler::RequestPayload::new(query_params, request_headers, request_body, tx);

        let invocation = {
            let trigger_registry = self.trigger_registry.read().await;
            trigger_registry.request_invocation(trigger_path, payload)?
        };
        if self.scheduler.enqueue(invocation).await.is_err() {
            return Err(ProcessingEngineError::RequestTriggerNotFound);
        }

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
        if self.environment_manager.plugin_dir_only {
            return Err(ProcessingEngineError::PluginError(
                PluginError::PluginInstallationDisabled,
            ));
        }

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
        if self.environment_manager.plugin_dir_only {
            return Err(ProcessingEngineError::PluginError(
                PluginError::PluginInstallationDisabled,
            ));
        }

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
        if self.environment_manager.plugin_dir_only {
            return Err(ProcessingEngineError::PluginError(
                PluginError::PluginInstallationDisabled,
            ));
        }

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
            for event in catalog_update.events() {
                let processing_engine_manager = Arc::clone(&processing_engine_manager);
                match event {
                    CatalogEvent::TriggerCreated { db_id, trigger_id } => {
                        // Only run if the trigger was created in the enabled state.
                        if let Some(db_schema) =
                            processing_engine_manager.catalog.db_schema_by_id(db_id)
                            && let Some(trigger) =
                                db_schema.processing_engine_triggers.get_by_id(trigger_id)
                            && !trigger.disabled
                            && let Err(error) = Arc::clone(&processing_engine_manager)
                                .run_trigger(*db_id, *trigger_id)
                                .await
                        {
                            error!(?error, "failed to run the created trigger");
                        }
                    }
                    CatalogEvent::TriggerEnabled { db_id, trigger_id } => {
                        if let Err(error) = Arc::clone(&processing_engine_manager)
                            .run_trigger(*db_id, *trigger_id)
                            .await
                        {
                            error!(?error, "failed to run the trigger");
                        }
                    }
                    CatalogEvent::TriggerDeleted {
                        db_id,
                        trigger_id,
                        force: true,
                    } => {
                        if let Err(error) = processing_engine_manager
                            .stop_trigger(*db_id, *trigger_id)
                            .await
                        {
                            error!(?error, "failed to stop the deleted trigger");
                        }
                    }
                    CatalogEvent::TriggerDisabled { db_id, trigger_id } => {
                        if let Err(error) = processing_engine_manager
                            .stop_trigger(*db_id, *trigger_id)
                            .await
                        {
                            error!(?error, "failed to disable the trigger");
                        }
                    }
                    CatalogEvent::DatabaseSoftDeleted { db_id } => {
                        info!(?db_id, "database soft deleted, disabling all triggers");
                        // If a hard delete is scheduled, defer full cleanup to the
                        // hard-delete handler; otherwise tear everything down now.
                        let hard_delete_pending = processing_engine_manager
                            .catalog
                            .db_schema_by_id(db_id)
                            .is_some_and(|db| db.hard_delete_time.is_some());
                        processing_engine_manager
                            .scheduler
                            .shutdown_triggers_for_db(*db_id)
                            .await;
                        if !hard_delete_pending {
                            processing_engine_manager
                                .trigger_registry
                                .write()
                                .await
                                .remove_all_for_db(*db_id);
                            processing_engine_manager
                                .cache
                                .lock()
                                .drop_all_trigger_caches_for_db(*db_id);
                        }
                    }
                    CatalogEvent::DatabaseHardDeleted { db_id } => {
                        info!(?db_id, "database hard deleted, removing all triggers");
                        processing_engine_manager
                            .scheduler
                            .shutdown_triggers_for_db(*db_id)
                            .await;
                        processing_engine_manager
                            .trigger_registry
                            .write()
                            .await
                            .remove_all_for_db(*db_id);
                        processing_engine_manager
                            .cache
                            .lock()
                            .drop_all_trigger_caches_for_db(*db_id);
                    }
                    _ => (),
                }
            }
        }
    })
}

#[cfg(test)]
mod tests;
