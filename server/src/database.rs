use crate::{
    db::{
        load::{create_preserved_catalog, load_or_create_preserved_catalog},
        DatabaseToCommit,
    },
    ApplicationState, Db, DB_RULES_FILE_NAME,
};
use bytes::BytesMut;
use data_types::{
    database_rules::DatabaseRules, database_state::DatabaseStateCode, server_id::ServerId,
    DatabaseName,
};
use futures::{
    future::{BoxFuture, Shared},
    FutureExt, TryFutureExt,
};
use generated_types::database_rules::encode_database_rules;
use internal_types::freezable::Freezable;
use iox_object_store::IoxObjectStore;
use object_store::{
    path::{ObjectStorePath, Path},
    ObjectStore, ObjectStoreApi,
};
use observability_deps::tracing::{error, info, warn};
use parking_lot::RwLock;
use parquet_file::catalog::PreservedCatalog;
use persistence_windows::checkpoint::ReplayPlan;
use snafu::{ResultExt, Snafu};
use std::{future::Future, sync::Arc, time::Duration};
use tokio::{sync::Notify, task::JoinError};
use tokio_util::sync::CancellationToken;

const INIT_BACKOFF: Duration = Duration::from_secs(1);

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "a state transition is already in progress for database ({}) in state {}",
        db_name,
        state
    ))]
    TransitionInProgress {
        db_name: String,
        state: DatabaseStateCode,
    },

    #[snafu(display(
        "database ({}) in invalid state ({:?}) for transition ({})",
        db_name,
        state,
        transition
    ))]
    InvalidState {
        db_name: String,
        state: DatabaseStateCode,
        transition: String,
    },

    #[snafu(display(
        "failed to wipe preserved catalog of database ({}): {}",
        db_name,
        source
    ))]
    WipePreservedCatalog {
        db_name: String,
        source: Box<parquet_file::catalog::Error>,
    },
}

/// A `Database` represents a single configured IOx database - i.e. an entity with a corresponding
/// set of `DatabaseRules`.
///
/// `Database` composes together the various subsystems responsible for implementing
/// `DatabaseRules` and handles their startup and shutdown. This includes instance-local
/// data storage (i.e. `Db`), the write buffer, request routing, data lifecycle, etc...
///
/// TODO: Make the above accurate
#[derive(Debug)]
pub struct Database {
    /// Future that resolves when the background worker exits
    join: Shared<BoxFuture<'static, Result<(), Arc<JoinError>>>>,

    /// The state shared with the background worker
    shared: Arc<DatabaseShared>,
}

#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    pub name: DatabaseName<'static>,
    pub server_id: ServerId,
    pub store_prefix: Path,
    pub wipe_catalog_on_error: bool,
    pub skip_replay: bool,
}

impl Database {
    /// Create in-mem database object.
    ///
    /// This is backed by an existing database, which was [created](Self::create) some time in the past.
    pub fn new(application: Arc<ApplicationState>, config: DatabaseConfig) -> Self {
        info!(db_name=%config.name, store_prefix=%config.store_prefix.display(), "new database");

        let iox_object_store = Arc::new(IoxObjectStore::new(
            Arc::clone(application.object_store()),
            config.server_id,
            &config.name,
        ));
        let shared = Arc::new(DatabaseShared {
            config,
            application,
            shutdown: Default::default(),
            state: RwLock::new(Freezable::new(DatabaseState::Known(DatabaseStateKnown {}))),
            state_notify: Default::default(),
            iox_object_store,
        });

        let handle = tokio::spawn(background_worker(Arc::clone(&shared)));
        let join = handle.map_err(Arc::new).boxed().shared();

        Self { join, shared }
    }

    /// Create fresh database w/o any state.
    pub async fn create(
        application: Arc<ApplicationState>,
        store_prefix: &Path,
        rules: DatabaseRules,
        server_id: ServerId,
    ) -> Result<(), InitError> {
        let db_name = rules.name.clone();

        persist_database_rules(application.object_store(), store_prefix, rules).await?;

        create_preserved_catalog(
            db_name.as_str(),
            Arc::new(IoxObjectStore::new(
                Arc::clone(application.object_store()),
                server_id,
                &db_name,
            )),
            server_id,
            Arc::clone(application.metric_registry()),
            true,
        )
        .await
        .context(CannotCreatePreservedCatalog)?;

        Ok(())
    }

    /// Triggers shutdown of this `Database`
    pub fn shutdown(&self) {
        info!(db_name=%self.shared.config.name, "database shutting down");
        self.shared.shutdown.cancel()
    }

    /// Waits for the background worker of this `Database` to exit
    pub fn join(&self) -> impl Future<Output = Result<(), Arc<JoinError>>> {
        self.join.clone()
    }

    /// Returns the config of this database
    pub fn config(&self) -> &DatabaseConfig {
        &self.shared.config
    }

    /// Returns the initialization status of this database
    pub fn state_code(&self) -> DatabaseStateCode {
        self.shared.state.read().state_code()
    }

    /// Returns the initialization error of this database if any
    pub fn init_error(&self) -> Option<Arc<InitError>> {
        self.shared.state.read().error().cloned()
    }

    /// Gets the current database state
    pub fn is_initialized(&self) -> bool {
        self.shared.state.read().get_initialized().is_some()
    }

    /// Returns the database rules if they're loaded
    pub fn rules(&self) -> Option<Arc<DatabaseRules>> {
        self.shared.state.read().rules()
    }

    /// Gets access to an initialized `Db`
    pub fn initialized_db(&self) -> Option<Arc<Db>> {
        self.shared
            .state
            .read()
            .get_initialized()
            .map(|state| Arc::clone(&state.db))
    }

    pub fn iox_object_store(&self) -> Arc<IoxObjectStore> {
        Arc::clone(&self.shared.iox_object_store)
    }

    /// Returns Ok(()) when the Database is initialized, or the error
    /// if one is encountered
    pub async fn wait_for_init(&self) -> Result<(), Arc<InitError>> {
        loop {
            // Register interest before checking to avoid race
            let notify = self.shared.state_notify.notified();

            // Note: this is not guaranteed to see non-terminal states
            // as the state machine may advance past them between
            // the notification being fired, and this task waking up
            match &**self.shared.state.read() {
                DatabaseState::Known(_)
                | DatabaseState::RulesLoaded(_)
                | DatabaseState::CatalogLoaded(_) => {} // Non-terminal state
                DatabaseState::Initialized(_) => return Ok(()),
                DatabaseState::RulesLoadError(_, e)
                | DatabaseState::CatalogLoadError(_, e)
                | DatabaseState::ReplayError(_, e) => return Err(Arc::clone(e)),
            }

            notify.await;
        }
    }

    /// Recover from a CatalogLoadError by wiping the catalog
    pub fn wipe_preserved_catalog(&self) -> Result<impl Future<Output = Result<(), Error>>, Error> {
        let db_name = &self.shared.config.name;
        let (current_state, handle) = {
            let state = self.shared.state.read();
            let current_state = match &**state {
                DatabaseState::CatalogLoadError(rules_loaded, _) => rules_loaded.clone(),
                _ => {
                    return Err(Error::InvalidState {
                        db_name: db_name.to_string(),
                        state: state.state_code(),
                        transition: "WipePreservedCatalog".to_string(),
                    })
                }
            };

            let handle = state.try_freeze().ok_or(Error::TransitionInProgress {
                db_name: db_name.to_string(),
                state: state.state_code(),
            })?;

            (current_state, handle)
        };

        let shared = Arc::clone(&self.shared);

        Ok(async move {
            let db_name = &shared.config.name;

            PreservedCatalog::wipe(&shared.iox_object_store)
                .await
                .map_err(Box::new)
                .context(WipePreservedCatalog { db_name })?;

            {
                let mut state = shared.state.write();
                *state.unfreeze(handle) = DatabaseState::RulesLoaded(current_state);
            }

            Ok(())
        })
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        let db_name = &self.shared.config.name;
        if !self.shared.shutdown.is_cancelled() {
            warn!(%db_name, "database dropped without calling shutdown()");
            self.shared.shutdown.cancel();
        }

        if self.join.clone().now_or_never().is_none() {
            warn!(%db_name, "database dropped without waiting for worker termination");
        }
    }
}

/// State shared with the `Database` background worker
#[derive(Debug)]
struct DatabaseShared {
    /// Configuration provided to the database at startup
    config: DatabaseConfig,

    /// A token that is used to trigger shutdown of the background worker
    shutdown: CancellationToken,

    /// Application-global state
    application: Arc<ApplicationState>,

    /// The state of the `Database`
    state: RwLock<Freezable<DatabaseState>>,

    /// Notify that the database state has changed
    state_notify: Notify,

    /// The object store interface for this database
    iox_object_store: Arc<IoxObjectStore>,
}

/// The background worker for `Database` - there should only ever be one
async fn background_worker(shared: Arc<DatabaseShared>) {
    info!(db_name=%shared.config.name, "started database background worker");

    initialize_database(shared.as_ref()).await;

    if !shared.shutdown.is_cancelled() {
        let db = Arc::clone(
            &shared
                .state
                .read()
                .get_initialized()
                .expect("expected initialized")
                .db,
        );

        info!(db_name=%shared.config.name, "database finished initialization - starting Db worker");

        crate::utils::panic_test(|| {
            Some(format!(
                "database background worker: {}",
                shared.config.name
            ))
        });

        // TODO: Pull background_worker out of `Db`
        db.background_worker(shared.shutdown.clone()).await
    }

    info!(db_name=%shared.config.name, "draining tasks");

    // Loop in case tasks are spawned during shutdown
    loop {
        use futures::stream::{FuturesUnordered, StreamExt};

        // We get a list of jobs from the global registry and filter them for this database
        let jobs = shared.application.job_registry().running();
        let mut futures: FuturesUnordered<_> = jobs
            .iter()
            .filter_map(|tracker| {
                let db_name = tracker.metadata().db_name()?;
                if db_name.as_ref() != shared.config.name.as_str() {
                    return None;
                }
                Some(tracker.join())
            })
            .collect();

        if futures.is_empty() {
            break;
        }

        info!(db_name=%shared.config.name, count=futures.len(), "waiting for jobs");

        while futures.next().await.is_some() {}
    }

    info!(db_name=%shared.config.name, "database worker finished");
}

/// Try to drive the database to `DatabaseState::Initialized` returns when
/// this is achieved or the shutdown signal is triggered
async fn initialize_database(shared: &DatabaseShared) {
    let db_name = &shared.config.name;
    info!(%db_name, "database initialization started");

    while !shared.shutdown.is_cancelled() {
        // Acquire locks and determine if work to be done
        let maybe_transaction = {
            let state = shared.state.read();

            match &**state {
                // Already initialized
                DatabaseState::Initialized(_) => break,
                // Can perform work
                DatabaseState::Known(_)
                | DatabaseState::RulesLoaded(_)
                | DatabaseState::CatalogLoaded(_) => {
                    match state.try_freeze() {
                        Some(handle) => Some((DatabaseState::clone(&state), handle)),
                        None => {
                            // Backoff if there is already an in-progress initialization action (e.g. recovery)
                            info!(%db_name, %state, "init transaction already in progress");
                            None
                        }
                    }
                }
                // Operator intervention required
                DatabaseState::RulesLoadError(_, e)
                | DatabaseState::CatalogLoadError(_, e)
                | DatabaseState::ReplayError(_, e) => {
                    error!(%db_name, %e, %state, "database in error state - operator intervention required");
                    None
                }
            }
        };

        // Backoff if no work to be done
        let (state, handle) = match maybe_transaction {
            Some((state, handle)) => (state, handle),
            None => {
                info!(%db_name, "backing off initialization");
                tokio::time::sleep(INIT_BACKOFF).await;
                continue;
            }
        };

        info!(%db_name, %state, "attempting to advance database initialization state");

        // Try to advance to the next state
        let next_state = match state {
            DatabaseState::Known(state) => match state.advance(shared).await {
                Ok(state) => DatabaseState::RulesLoaded(state),
                Err(e) => DatabaseState::RulesLoadError(state, Arc::new(e)),
            },
            DatabaseState::RulesLoaded(state) => match state.advance(shared).await {
                Ok(state) => DatabaseState::CatalogLoaded(state),
                Err(e) => DatabaseState::CatalogLoadError(state, Arc::new(e)),
            },
            DatabaseState::CatalogLoaded(state) => match state.advance().await {
                Ok(state) => DatabaseState::Initialized(state),
                Err(e) => DatabaseState::ReplayError(state, Arc::new(e)),
            },
            state => unreachable!("{:?}", state),
        };

        // Commit the next state
        {
            let mut state = shared.state.write();
            info!(%db_name, from=%state, to=%next_state, "database initialization state transition");

            *state.unfreeze(handle) = next_state;
            shared.state_notify.notify_waiters();
        }
    }
}

/// Errors encountered during initialization of a database
#[derive(Debug, Snafu)]
pub enum InitError {
    #[snafu(display("error fetching rules: {}", source))]
    RulesFetch { source: object_store::Error },

    #[snafu(display("error decoding database rules: {}", source))]
    RulesDecode {
        source: generated_types::database_rules::DecodeError,
    },

    #[snafu(display(
        "Database names in deserialized rules ({}) does not match expected value ({})",
        actual,
        expected
    ))]
    RulesDatabaseNameMismatch { actual: String, expected: String },

    #[snafu(display("error loading catalog: {}", source))]
    CatalogLoad { source: crate::db::load::Error },

    #[snafu(display("error creating write buffer: {}", source))]
    CreateWriteBuffer {
        source: write_buffer::core::WriteBufferError,
    },

    #[snafu(display("error during replay: {}", source))]
    Replay { source: crate::db::Error },

    #[snafu(display("store error: {}", source))]
    StoreError { source: object_store::Error },

    #[snafu(display("error serializing database rules to protobuf: {}", source))]
    ErrorSerializingRulesProtobuf {
        source: generated_types::database_rules::EncodeError,
    },

    #[snafu(display("cannot create preserved catalog: {}", source))]
    CannotCreatePreservedCatalog { source: crate::db::load::Error },
}

/// The Database startup state machine
///
/// A Database starts in DatabaseState::Known and advances through the
/// states in sequential order until it reaches Initialized or an error
/// is encountered.
#[derive(Debug, Clone)]
enum DatabaseState {
    Known(DatabaseStateKnown),

    RulesLoaded(DatabaseStateRulesLoaded),
    CatalogLoaded(DatabaseStateCatalogLoaded),
    Initialized(DatabaseStateInitialized),

    RulesLoadError(DatabaseStateKnown, Arc<InitError>),
    CatalogLoadError(DatabaseStateRulesLoaded, Arc<InitError>),
    ReplayError(DatabaseStateCatalogLoaded, Arc<InitError>),
}

impl std::fmt::Display for DatabaseState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.state_code().fmt(f)
    }
}

impl DatabaseState {
    fn state_code(&self) -> DatabaseStateCode {
        match self {
            DatabaseState::Known(_) => DatabaseStateCode::Known,
            DatabaseState::RulesLoaded(_) => DatabaseStateCode::RulesLoaded,
            DatabaseState::CatalogLoaded(_) => DatabaseStateCode::CatalogLoaded,
            DatabaseState::Initialized(_) => DatabaseStateCode::Initialized,
            DatabaseState::RulesLoadError(_, _) => DatabaseStateCode::Known,
            DatabaseState::CatalogLoadError(_, _) => DatabaseStateCode::RulesLoaded,
            DatabaseState::ReplayError(_, _) => DatabaseStateCode::CatalogLoaded,
        }
    }

    fn error(&self) -> Option<&Arc<InitError>> {
        match self {
            DatabaseState::Known(_)
            | DatabaseState::RulesLoaded(_)
            | DatabaseState::CatalogLoaded(_)
            | DatabaseState::Initialized(_) => None,
            DatabaseState::RulesLoadError(_, e)
            | DatabaseState::CatalogLoadError(_, e)
            | DatabaseState::ReplayError(_, e) => Some(e),
        }
    }

    fn rules(&self) -> Option<Arc<DatabaseRules>> {
        match self {
            DatabaseState::Known(_) | DatabaseState::RulesLoadError(_, _) => None,
            DatabaseState::RulesLoaded(state) | DatabaseState::CatalogLoadError(state, _) => {
                Some(Arc::clone(&state.rules))
            }
            DatabaseState::CatalogLoaded(state) | DatabaseState::ReplayError(state, _) => {
                Some(state.db.rules())
            }
            DatabaseState::Initialized(state) => Some(state.db.rules()),
        }
    }

    fn get_initialized(&self) -> Option<&DatabaseStateInitialized> {
        match self {
            DatabaseState::Initialized(state) => Some(state),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
struct DatabaseStateKnown {}

impl DatabaseStateKnown {
    /// Load database rules from object storage
    async fn advance(
        &self,
        shared: &DatabaseShared,
    ) -> Result<DatabaseStateRulesLoaded, InitError> {
        let mut location = shared.config.store_prefix.clone();
        location.set_file_name(DB_RULES_FILE_NAME);

        // TODO: Retry this
        let bytes = get_store_bytes(shared.application.object_store().as_ref(), &location)
            .await
            .context(RulesFetch)?;

        let rules =
            generated_types::database_rules::decode_database_rules(bytes).context(RulesDecode)?;

        if rules.name != shared.config.name {
            return Err(InitError::RulesDatabaseNameMismatch {
                actual: rules.name.to_string(),
                expected: shared.config.name.to_string(),
            });
        }

        Ok(DatabaseStateRulesLoaded {
            rules: Arc::new(rules),
        })
    }
}

#[derive(Debug, Clone)]
struct DatabaseStateRulesLoaded {
    rules: Arc<DatabaseRules>,
}

impl DatabaseStateRulesLoaded {
    /// Load catalog from object storage
    async fn advance(
        &self,
        shared: &DatabaseShared,
    ) -> Result<DatabaseStateCatalogLoaded, InitError> {
        let (preserved_catalog, catalog, replay_plan) = load_or_create_preserved_catalog(
            shared.config.name.as_str(),
            Arc::clone(&shared.iox_object_store),
            shared.config.server_id,
            Arc::clone(shared.application.metric_registry()),
            shared.config.wipe_catalog_on_error,
            shared.config.skip_replay,
        )
        .await
        .context(CatalogLoad)?;

        let write_buffer = shared
            .application
            .write_buffer_factory()
            .new_config(shared.config.server_id, self.rules.as_ref())
            .await
            .context(CreateWriteBuffer)?;

        let database_to_commit = DatabaseToCommit {
            server_id: shared.config.server_id,
            iox_object_store: Arc::new(IoxObjectStore::new(
                Arc::clone(shared.application.object_store()),
                shared.config.server_id,
                &shared.config.name,
            )),
            exec: Arc::clone(shared.application.executor()),
            rules: Arc::clone(&self.rules),
            preserved_catalog,
            catalog,
            write_buffer,
        };

        let db = Db::new(
            database_to_commit,
            Arc::clone(shared.application.job_registry()),
        );

        Ok(DatabaseStateCatalogLoaded {
            db,
            replay_plan: Arc::new(replay_plan),
        })
    }
}

#[derive(Debug, Clone)]
struct DatabaseStateCatalogLoaded {
    db: Arc<Db>,
    replay_plan: Arc<Option<ReplayPlan>>,
}

impl DatabaseStateCatalogLoaded {
    /// Perform replay
    async fn advance(&self) -> Result<DatabaseStateInitialized, InitError> {
        let db = Arc::clone(&self.db);

        db.perform_replay(self.replay_plan.as_ref().as_ref())
            .await
            .context(Replay)?;

        // TODO: Pull write buffer and lifecycle out of Db
        db.unsuppress_persistence().await;
        db.allow_write_buffer_read();

        Ok(DatabaseStateInitialized { db })
    }
}

#[derive(Debug, Clone)]
struct DatabaseStateInitialized {
    db: Arc<Db>,
}

/// Get the bytes for a given object store location
///
/// TODO: move to object_store crate
async fn get_store_bytes(
    store: &ObjectStore,
    location: &Path,
) -> Result<bytes::Bytes, object_store::Error> {
    use futures::stream::TryStreamExt;

    let stream = store.get(location).await?;
    let bytes = stream
        .try_fold(BytesMut::new(), |mut acc, buf| async move {
            acc.extend_from_slice(&buf);
            Ok(acc)
        })
        .await?;

    Ok(bytes.freeze())
}

/// Persist the the `DatabaseRules` given the `Database` store prefix
pub(super) async fn persist_database_rules(
    object_store: &ObjectStore,
    store_prefix: &Path,
    rules: DatabaseRules,
) -> Result<(), InitError> {
    let mut data = BytesMut::new();
    encode_database_rules(rules, &mut data).context(ErrorSerializingRulesProtobuf)?;

    let mut location = store_prefix.clone();
    location.set_file_name(DB_RULES_FILE_NAME);

    let len = data.len();

    let stream_data = std::io::Result::Ok(data.freeze());
    object_store
        .put(
            &location,
            futures::stream::once(async move { stream_data }),
            Some(len),
        )
        .await
        .context(StoreError)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroU32;

    #[tokio::test]
    async fn database_shutdown_waits_for_jobs() {
        let application = Arc::new(ApplicationState::new(
            Arc::new(ObjectStore::new_in_memory()),
            None,
        ));

        let database = Database::new(
            Arc::clone(&application),
            DatabaseConfig {
                name: DatabaseName::new("test").unwrap(),
                server_id: ServerId::new(NonZeroU32::new(23).unwrap()),
                store_prefix: application.object_store().new_path(),
                wipe_catalog_on_error: false,
                skip_replay: false,
            },
        );

        // Should have failed to load (this isn't important to the test)
        let err = database.wait_for_init().await.unwrap_err();
        assert!(matches!(err.as_ref(), InitError::RulesFetch { .. }));

        // Database should be running
        assert!(database.join().now_or_never().is_none());

        // Spawn a dummy job associated with this database
        let database_dummy_job = application
            .job_registry()
            .spawn_dummy_job(vec![50_000_000], Some(Arc::from("test")));

        // Spawn a dummy job not associated with this database
        let server_dummy_job = application
            .job_registry()
            .spawn_dummy_job(vec![10_000_000_000], None);

        // Trigger database shutdown
        database.shutdown();

        // Expect timeout to not complete
        tokio::time::timeout(tokio::time::Duration::from_millis(1), database.join())
            .await
            .unwrap_err();

        // Database task shouldn't have finished yet
        assert!(!database_dummy_job.is_complete());

        // Wait for database to finish
        database.join().await.unwrap();

        // Database task should have finished
        assert!(database_dummy_job.is_complete());

        // Shouldn't have waited for server tracker to finish
        assert!(!server_dummy_job.is_complete());
    }
}
