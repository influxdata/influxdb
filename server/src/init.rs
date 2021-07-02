//! Routines to initialize a server.
use data_types::{
    database_rules::{DatabaseRules, WriteBufferConnection},
    database_state::DatabaseStateCode,
    server_id::ServerId,
    DatabaseName,
};
use futures::TryStreamExt;
use generated_types::database_rules::decode_database_rules;
use internal_types::once::OnceNonZeroU32;
use object_store::{
    path::{parsed::DirsAndFileName, ObjectStorePath, Path},
    ObjectStore, ObjectStoreApi,
};
use observability_deps::tracing::{debug, error, info, warn};
use parking_lot::Mutex;
use parquet_file::catalog::PreservedCatalog;
use query::exec::Executor;
use snafu::{OptionExt, ResultExt, Snafu};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use tokio::sync::Semaphore;

use crate::{
    config::{object_store_path_for_database_config, Config, DatabaseHandle, DB_RULES_FILE_NAME},
    db::load::load_or_create_preserved_catalog,
    write_buffer::WriteBufferConfig,
    DatabaseError,
};

const STORE_ERROR_PAUSE_SECONDS: u64 = 100;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("cannot load catalog: {}", source))]
    CatalogLoadError { source: DatabaseError },

    #[snafu(display("error deserializing database rules from protobuf: {}", source))]
    ErrorDeserializingRulesProtobuf {
        source: generated_types::database_rules::DecodeError,
    },

    #[snafu(display("id already set"))]
    IdAlreadySet { id: ServerId },

    #[snafu(display("unable to use server until id is set"))]
    IdNotSet,

    #[snafu(display(
        "no database configuration present in directory that contains data: {:?}",
        location
    ))]
    NoDatabaseConfigError { location: object_store::path::Path },

    #[snafu(display("store error: {}", source))]
    StoreError { source: object_store::Error },

    #[snafu(display("Cannot recover DB: {}", source))]
    RecoverDbError {
        source: Arc<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Cannot init DB: {}", source))]
    InitDbError { source: Box<crate::Error> },

    #[snafu(display("Cannot wipe preserved catalog DB: {}", source))]
    PreservedCatalogWipeError {
        source: Box<parquet_file::catalog::Error>,
    },

    #[snafu(display("Cannot parse DB name: {}", source))]
    DatabaseNameError {
        source: data_types::DatabaseNameError,
    },

    #[snafu(display(
        "Cannot create write buffer with config: {:?}, error: {}",
        config,
        source
    ))]
    CreateWriteBuffer {
        config: Option<WriteBufferConnection>,
        source: DatabaseError,
    },

    #[snafu(display(
        "Cannot wipe catalog because DB init progress has already read it: {}",
        db_name
    ))]
    DbPartiallyInitialized { db_name: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Default)]
pub struct CurrentServerId(OnceNonZeroU32);

impl CurrentServerId {
    pub fn set(&self, id: ServerId) -> Result<()> {
        let id = id.get();

        match self.0.set(id) {
            Ok(()) => {
                info!(server_id = id, "server ID set");
                Ok(())
            }
            Err(id) => Err(Error::IdAlreadySet {
                id: ServerId::new(id),
            }),
        }
    }

    pub fn get(&self) -> Result<ServerId> {
        self.0.get().map(ServerId::new).context(IdNotSet)
    }
}

#[derive(Debug)]
pub struct InitStatus {
    pub server_id: CurrentServerId,

    /// Flags that databases are loaded and server is ready to read/write data.
    initialized: AtomicBool,

    /// Semaphore that limits the number of jobs that load DBs when the serverID is set.
    ///
    /// Note that this semaphore is more of a "lock" than an arbitrary semaphore. All the other sync structures (mutex,
    /// rwlock) require something to be wrapped which we don't have in our case, so we're using a semaphore here. We
    /// want exactly 1 background worker to mess with the server init / DB loading, otherwise everything in the critical
    /// section (in [`maybe_initialize_server`](Self::maybe_initialize_server)) will break apart. So this semaphore
    /// cannot be configured.
    initialize_semaphore: Semaphore,

    /// Error occurred during generic server init (e.g. listing store content).
    error_generic: Mutex<Option<Arc<Error>>>,

    /// Errors that occurred during some DB init.
    errors_databases: Arc<Mutex<HashMap<String, Arc<Error>>>>,

    /// Automatic wipe-on-error recovery
    ///
    /// See https://github.com/influxdata/influxdb_iox/issues/1522)
    pub(crate) wipe_on_error: AtomicBool,
}

impl InitStatus {
    /// Create new "not initialized" status.
    pub fn new() -> Self {
        Self {
            server_id: Default::default(),
            initialized: AtomicBool::new(false),
            // Always set semaphore permits to `1`, see design comments in `Server::initialize_semaphore`.
            initialize_semaphore: Semaphore::new(1),
            error_generic: Default::default(),
            errors_databases: Default::default(),
            wipe_on_error: AtomicBool::new(true),
        }
    }

    /// Base location in object store for this writer.
    pub fn root_path(&self, store: &ObjectStore) -> Result<Path> {
        let id = self.server_id.get()?;

        let mut path = store.new_path();
        path.push_dir(format!("{}", id));
        Ok(path)
    }

    /// Check if server is loaded. Databases are loaded and server is ready to read/write.
    pub fn initialized(&self) -> bool {
        // Need `Acquire` ordering because IF we a `true` here, this thread will likely also read data that
        // `maybe_initialize_server` wrote before toggling the flag with `Release`. The `Acquire` flag here ensures that
        // every data acccess AFTER the following line will also stay AFTER this line.
        self.initialized.load(Ordering::Acquire)
    }

    /// Error occurred during generic server init (e.g. listing store content).
    pub fn error_generic(&self) -> Option<Arc<Error>> {
        let guard = self.error_generic.lock();
        guard.clone()
    }

    /// List all databases with errors in sorted order.
    pub fn databases_with_errors(&self) -> Vec<String> {
        let guard = self.errors_databases.lock();
        let mut names: Vec<_> = guard.keys().cloned().collect();
        names.sort();
        names
    }

    /// Error that occurred during initialization of a specific database.
    pub fn error_database(&self, db_name: &str) -> Option<Arc<Error>> {
        let guard = self.errors_databases.lock();
        guard.get(db_name).cloned()
    }

    /// Loads the database configurations based on the databases in the
    /// object store. Any databases in the config already won't be
    /// replaced.
    ///
    /// This requires the serverID to be set (will be a no-op otherwise).
    ///
    /// It will be a no-op if the configs are already loaded and the server is ready.
    pub(crate) async fn maybe_initialize_server(
        &self,
        store: Arc<ObjectStore>,
        config: Arc<Config>,
        exec: Arc<Executor>,
    ) {
        let server_id = match self.server_id.get() {
            Ok(id) => id,
            Err(e) => {
                debug!(%e, "cannot initialize server because cannot get serverID");
                return;
            }
        };

        let _guard = self
            .initialize_semaphore
            .acquire()
            .await
            .expect("semaphore should not be closed");

        // Note that we use Acquire-Release ordering for the atomic within the semaphore to ensure that another thread
        // that enters this semaphore after we've left actually sees the correct `is_ready` flag.
        if self.initialized.load(Ordering::Acquire) {
            // already loaded, so do nothing
            return;
        }

        // Check if there was a previous failed attempt
        if self.error_generic().is_some() {
            return;
        }

        match self
            .maybe_initialize_server_inner(store, config, exec, server_id)
            .await
        {
            Ok(_) => {
                // mark as ready (use correct ordering for Acquire-Release)
                self.initialized.store(true, Ordering::Release);
                info!("loaded databases, server is initalized");
            }
            Err(e) => {
                error!(%e, "error during server init");
                let mut guard = self.error_generic.lock();
                *guard = Some(Arc::new(e));
            }
        }
    }

    async fn maybe_initialize_server_inner(
        &self,
        store: Arc<ObjectStore>,
        config: Arc<Config>,
        exec: Arc<Executor>,
        server_id: ServerId,
    ) -> Result<()> {
        let root = self.root_path(&store)?;

        // get the database names from the object store prefixes
        // TODO: update object store to pull back all common prefixes by
        //       following the next tokens.
        let list_result = store.list_with_delimiter(&root).await.context(StoreError)?;

        let handles: Vec<_> = list_result
            .common_prefixes
            .into_iter()
            .filter_map(|mut path| {
                let store = Arc::clone(&store);
                let config = Arc::clone(&config);
                let exec = Arc::clone(&exec);
                let errors_databases = Arc::clone(&self.errors_databases);
                let wipe_on_error = self.wipe_on_error.load(Ordering::Relaxed);
                let root = root.clone();

                path.set_file_name(DB_RULES_FILE_NAME);

                match db_name_from_rules_path(&path) {
                    Ok(db_name) => {
                        let handle = tokio::task::spawn(async move {
                            match Self::initialize_database(
                                server_id,
                                store,
                                config,
                                exec,
                                root,
                                db_name.clone(),
                                wipe_on_error,
                            )
                            .await
                            {
                                Ok(()) => {
                                    info!(%db_name, "database initialized");
                                }
                                Err(e) => {
                                    error!(%e, %db_name, "cannot load database");
                                    let mut guard = errors_databases.lock();
                                    guard.insert(db_name.to_string(), Arc::new(e));
                                }
                            }
                        });
                        Some(handle)
                    }
                    Err(e) => {
                        error!(%e, "invalid database path");
                        None
                    }
                }
            })
            .collect();

        futures::future::join_all(handles).await;

        Ok(())
    }

    async fn initialize_database(
        server_id: ServerId,
        store: Arc<ObjectStore>,
        config: Arc<Config>,
        exec: Arc<Executor>,
        root: Path,
        db_name: DatabaseName<'static>,
        wipe_on_error: bool,
    ) -> Result<()> {
        // Reserve name before expensive IO (e.g. loading the preserved catalog)
        let mut handle = config
            .create_db(store, exec, server_id, db_name)
            .map_err(Box::new)
            .context(InitDbError)?;

        match Self::try_advance_database_init_process_until_complete(
            &mut handle,
            &root,
            wipe_on_error,
        )
        .await
        {
            Ok(true) => {
                // finished init and keep DB
                handle.commit();
                Ok(())
            }
            Ok(false) => {
                // finished but do not keep DB
                handle.abort();
                Ok(())
            }
            Err(e) => {
                // encountered some error, still commit intermediate result
                handle.commit();
                Err(e)
            }
        }
    }

    async fn load_database_rules(
        store: Arc<ObjectStore>,
        path: Path,
    ) -> Result<Option<DatabaseRules>> {
        let serialized_rules = loop {
            match get_database_config_bytes(&path, &store).await {
                Ok(data) => break data,
                Err(e) => {
                    if let Error::NoDatabaseConfigError { location } = &e {
                        warn!(?location, "{}", e);
                        return Ok(None);
                    }
                    error!(
                        "error getting database config {:?} from object store: {}",
                        path, e
                    );
                    tokio::time::sleep(tokio::time::Duration::from_secs(STORE_ERROR_PAUSE_SECONDS))
                        .await;
                }
            }
        };
        let rules = decode_database_rules(serialized_rules.freeze())
            .context(ErrorDeserializingRulesProtobuf)?;

        Ok(Some(rules))
    }

    pub(crate) async fn wipe_preserved_catalog_and_maybe_recover(
        &self,
        store: Arc<ObjectStore>,
        config: Arc<Config>,
        server_id: ServerId,
        db_name: DatabaseName<'static>,
    ) -> Result<()> {
        if config.has_uninitialized_database(&db_name) {
            let mut handle = config
                .recover_db(db_name.clone())
                .map_err(|e| Arc::new(e) as _)
                .context(RecoverDbError)?;

            if !((handle.state_code() == DatabaseStateCode::Known)
                || (handle.state_code() == DatabaseStateCode::RulesLoaded))
            {
                // cannot wipe because init state is already too far
                return Err(Error::DbPartiallyInitialized {
                    db_name: db_name.to_string(),
                });
            }

            // wipe while holding handle so no other init/wipe process can interact with the catalog
            PreservedCatalog::wipe(&store, handle.server_id(), &db_name)
                .await
                .map_err(Box::new)
                .context(PreservedCatalogWipeError)?;

            let root = self.root_path(&store)?;
            let wipe_on_error = self.wipe_on_error.load(Ordering::Relaxed);
            match Self::try_advance_database_init_process_until_complete(
                &mut handle,
                &root,
                wipe_on_error,
            )
            .await
            {
                Ok(_) => {
                    // yeah, recovered DB
                    handle.commit();

                    let mut guard = self.errors_databases.lock();
                    guard.remove(&db_name.to_string());

                    info!(%db_name, "wiped preserved catalog of registered database and recovered");
                    Ok(())
                }
                Err(e) => {
                    // could not recover, but still keep new result
                    handle.commit();

                    let mut guard = self.errors_databases.lock();
                    let e = Arc::new(e);
                    guard.insert(db_name.to_string(), Arc::clone(&e));

                    warn!(%db_name, %e, "wiped preserved catalog of registered database but still cannot recover");
                    Err(Error::RecoverDbError { source: e })
                }
            }
        } else {
            let handle = config
                .block_db(db_name.clone())
                .map_err(|e| Arc::new(e) as _)
                .context(RecoverDbError)?;

            PreservedCatalog::wipe(&store, server_id, &db_name)
                .await
                .map_err(Box::new)
                .context(PreservedCatalogWipeError)?;

            drop(handle);

            info!(%db_name, "wiped preserved catalog of non-registered database");
            Ok(())
        }
    }

    /// Try to make as much progress as possible with DB init.
    ///
    /// Returns an error if there was an error along the way (in which case the handle should still be commit to safe
    /// the intermediate result). Returns `Ok(true)` if DB init is finished and `Ok(false)` if the DB can be forgotten
    /// (e.g. because not rules file is present.)
    async fn try_advance_database_init_process_until_complete(
        handle: &mut DatabaseHandle<'_>,
        root: &Path,
        wipe_on_error: bool,
    ) -> Result<bool> {
        loop {
            match Self::try_advance_database_init_process(handle, root, wipe_on_error).await? {
                InitProgress::Unfinished => {}
                InitProgress::Done => {
                    return Ok(true);
                }
                InitProgress::Forget => {
                    return Ok(false);
                }
            }
        }
    }

    /// Try to make some progress in the DB init.
    async fn try_advance_database_init_process(
        handle: &mut DatabaseHandle<'_>,
        root: &Path,
        wipe_on_error: bool,
    ) -> Result<InitProgress> {
        match handle.state_code() {
            DatabaseStateCode::Known => {
                // known => load DB rules
                let path = object_store_path_for_database_config(root, &handle.db_name());
                match Self::load_database_rules(handle.object_store(), path).await? {
                    Some(rules) => {
                        handle
                            .advance_rules_loaded(rules)
                            .map_err(Box::new)
                            .context(InitDbError)?;

                        // there is still more work to do for this DB
                        Ok(InitProgress::Unfinished)
                    }
                    None => {
                        // no rules file present, advice to forget his DB
                        Ok(InitProgress::Forget)
                    }
                }
            }
            DatabaseStateCode::RulesLoaded => {
                // rules already loaded => continue with loading preserved catalog
                let (preserved_catalog, catalog) = load_or_create_preserved_catalog(
                    &handle.db_name(),
                    handle.object_store(),
                    handle.server_id(),
                    handle.metrics_registry(),
                    wipe_on_error,
                )
                .await
                .map_err(|e| Box::new(e) as _)
                .context(CatalogLoadError)?;

                let rules = handle
                    .rules()
                    .expect("in this state rules should be loaded");
                let write_buffer = WriteBufferConfig::new(handle.server_id(), &rules).context(
                    CreateWriteBuffer {
                        config: rules.write_buffer_connection.clone(),
                    },
                )?;

                handle
                    .advance_replay(preserved_catalog, catalog, write_buffer)
                    .map_err(Box::new)
                    .context(InitDbError)?;

                // there is still more work to do for this DB
                Ok(InitProgress::Unfinished)
            }
            DatabaseStateCode::Replay => {
                let db = handle
                    .db_any_state()
                    .expect("DB should be available in this state");
                db.perform_replay().await;

                handle
                    .advance_init()
                    .map_err(Box::new)
                    .context(InitDbError)?;

                // there is still more work to do for this DB
                Ok(InitProgress::Unfinished)
            }
            DatabaseStateCode::Initialized => {
                // database fully initialized => nothing to do
                Ok(InitProgress::Done)
            }
        }
    }
}

enum InitProgress {
    Unfinished,
    Done,
    Forget,
}

// get bytes from the location in object store
async fn get_store_bytes(
    location: &object_store::path::Path,
    store: &ObjectStore,
) -> Result<bytes::BytesMut> {
    let b = store
        .get(location)
        .await
        .context(StoreError)?
        .map_ok(|b| bytes::BytesMut::from(&b[..]))
        .try_concat()
        .await
        .context(StoreError)?;

    Ok(b)
}

// get the bytes for the database rule config file, if it exists,
// otherwise it returns none.
async fn get_database_config_bytes(
    location: &object_store::path::Path,
    store: &ObjectStore,
) -> Result<bytes::BytesMut> {
    let list_result = store
        .list_with_delimiter(location)
        .await
        .context(StoreError)?;
    if list_result.objects.is_empty() {
        return NoDatabaseConfigError {
            location: location.clone(),
        }
        .fail();
    }
    get_store_bytes(location, store).await
}

/// Helper to extract the DB name from the rules file path.
fn db_name_from_rules_path(path: &Path) -> Result<DatabaseName<'static>> {
    let path_parsed: DirsAndFileName = path.clone().into();
    let db_name = path_parsed
        .directories
        .last()
        .map(|part| part.encoded().to_string())
        .unwrap_or_else(String::new);
    DatabaseName::new(db_name).context(DatabaseNameError)
}

#[cfg(test)]
mod tests {
    use object_store::{memory::InMemory, path::ObjectStorePath};

    use super::*;

    #[tokio::test]
    async fn test_get_database_config_bytes() {
        let object_store = ObjectStore::new_in_memory(InMemory::new());
        let mut rules_path = object_store.new_path();
        rules_path.push_all_dirs(&["1", "foo_bar"]);
        rules_path.set_file_name("rules.pb");

        let res = get_database_config_bytes(&rules_path, &object_store)
            .await
            .unwrap_err();
        assert!(matches!(res, Error::NoDatabaseConfigError { .. }));
    }
}
