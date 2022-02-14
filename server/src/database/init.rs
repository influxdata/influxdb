//! Database initialization / creation logic
use crate::{rules::ProvidedDatabaseRules, ApplicationState};
use data_types::{server_id::ServerId, DatabaseName};
use db::{
    load::{create_preserved_catalog, load_or_create_preserved_catalog},
    write_buffer::WriteBufferConsumer,
    DatabaseToCommit, Db, LifecycleWorker,
};
use generated_types::{
    database_state::DatabaseState as DatabaseStateCode, influxdata::iox::management,
};
use iox_object_store::IoxObjectStore;
use observability_deps::tracing::{error, info, warn};
use persistence_windows::checkpoint::ReplayPlan;
use rand::{thread_rng, Rng};
use snafu::{ResultExt, Snafu};
use std::{sync::Arc, time::Duration};
use tokio::task::JoinError;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::state::DatabaseShared;

/// Errors encountered during initialization of a database
#[derive(Debug, Snafu)]
pub enum InitError {
    #[snafu(display("error finding database directory in object storage: {}", source))]
    DatabaseObjectStoreLookup {
        source: iox_object_store::IoxObjectStoreError,
    },

    #[snafu(display(
        "Database name in deserialized rules ({}) does not match expected value ({})",
        actual,
        expected
    ))]
    RulesDatabaseNameMismatch { actual: String, expected: String },

    #[snafu(display("error loading catalog: {}", source))]
    CatalogLoad { source: db::load::Error },

    #[snafu(display("error creating write buffer: {}", source))]
    CreateWriteBuffer {
        source: write_buffer::core::WriteBufferError,
    },

    #[snafu(display("error during replay: {}", source))]
    Replay { source: db::Error },

    #[snafu(display("error creating database owner info: {}", source))]
    CreatingOwnerInfo { source: crate::config::Error },

    #[snafu(display("error getting database owner info: {}", source))]
    FetchingOwnerInfo { source: crate::config::Error },

    #[snafu(display("error updating database owner info: {}", source))]
    UpdatingOwnerInfo { source: crate::config::Error },

    #[snafu(display(
        "Server ID in the database's owner info file ({}) does not match this server's ID ({})",
        actual,
        expected
    ))]
    DatabaseOwnerMismatch { actual: u32, expected: u32 },

    #[snafu(display(
        "The database with UUID `{}` is already owned by the server with ID {}",
        uuid,
        server_id
    ))]
    CantClaimDatabaseCurrentlyOwned { uuid: Uuid, server_id: u32 },

    #[snafu(display("error saving database rules: {}", source))]
    SavingRules { source: crate::config::Error },

    #[snafu(display("error loading database rules: {}", source))]
    LoadingRules { source: crate::config::Error },

    #[snafu(display("{}", source))]
    IoxObjectStoreError {
        source: iox_object_store::IoxObjectStoreError,
    },

    #[snafu(display("The database with UUID `{}` named `{}` is already active", uuid, name))]
    AlreadyActive { name: String, uuid: Uuid },

    #[snafu(display("cannot create preserved catalog: {}", source))]
    CannotCreatePreservedCatalog { source: db::load::Error },

    #[snafu(display("database is not running"))]
    Shutdown,
}

/// The Database startup state machine
///
/// ```text
///                      (start)
///                         |
///                         |----------------------------o     o-o
///                         V                            V     V |
///                      [Known]-------------->[OwnerInfoLoadError]
///                         |                           |
///                         +---------------------------o
///                         |
///                         |                                 o-o
///                         V                                 V |
///                  [OwnerInfoLoaded]----------->[RulesLoadError]
///                         |                           |
///                         +---------------------------o
///                         |
///                         |                                   o-o
///                         V                                   V |
///                   [RulesLoaded]-------------->[CatalogLoadError]
///                         |                           |
///                         +---------------------------o
///                         |
///                         |                                        o-o
///                         V                                        V |
///                  [CatalogLoaded]---------->[WriteBufferCreationError]
///                         |    |               |       |
///                         |    |               |       |    o-o
///                         |    |               |       V    V |
///                         |    o---------------|-->[ReplayError]
///                         |                    |       |
///                         +--------------------+-------o
///                         |
///                         |
///                         V
///                   [Initialized]
///
///                         |
///                         V
///                     [Shutdown]
/// ```
///
/// A Database starts in [`DatabaseState::Known`] and advances through the
/// non error states in sequential order until either:
///
/// 1. It reaches [`DatabaseState::Initialized`]: Database is initialized
/// 2. An error is encountered, in which case it transitions to one of
///    the error states. We try to recover from all of them. For all except [`DatabaseState::ReplayError`] this is a
///    rather cheap operation since we can just retry the actual operation. For [`DatabaseState::ReplayError`] we need
///    to dump the potentially half-modified in-memory catalog before retrying.
#[derive(Debug, Clone)]
pub(crate) enum DatabaseState {
    // Database not running, with an optional shutdown error
    Shutdown(Option<Arc<JoinError>>),
    // Basic initialization sequence states:
    Known(DatabaseStateKnown),
    OwnerInfoLoaded(DatabaseStateOwnerInfoLoaded),
    RulesLoaded(DatabaseStateRulesLoaded),
    CatalogLoaded(DatabaseStateCatalogLoaded),

    // Terminal state (success)
    Initialized(DatabaseStateInitialized),

    // Error states, we'll try to recover from them
    OwnerInfoLoadError(DatabaseStateKnown, Arc<InitError>),
    RulesLoadError(DatabaseStateOwnerInfoLoaded, Arc<InitError>),
    CatalogLoadError(DatabaseStateRulesLoaded, Arc<InitError>),
    WriteBufferCreationError(DatabaseStateCatalogLoaded, Arc<InitError>),
    ReplayError(DatabaseStateCatalogLoaded, Arc<InitError>),
}

impl std::fmt::Display for DatabaseState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.state_code().fmt(f)
    }
}

impl DatabaseState {
    // Construct the start state of the database machine
    pub fn new_known() -> Self {
        Self::Known(DatabaseStateKnown {})
    }

    pub(crate) fn state_code(&self) -> DatabaseStateCode {
        match self {
            DatabaseState::Shutdown(_) => DatabaseStateCode::Shutdown,
            DatabaseState::Known(_) => DatabaseStateCode::Known,
            DatabaseState::OwnerInfoLoaded(_) => DatabaseStateCode::OwnerInfoLoaded,
            DatabaseState::RulesLoaded(_) => DatabaseStateCode::RulesLoaded,
            DatabaseState::CatalogLoaded(_) => DatabaseStateCode::CatalogLoaded,
            DatabaseState::Initialized(_) => DatabaseStateCode::Initialized,
            DatabaseState::OwnerInfoLoadError(_, _) => DatabaseStateCode::OwnerInfoLoadError,
            DatabaseState::RulesLoadError(_, _) => DatabaseStateCode::RulesLoadError,
            DatabaseState::CatalogLoadError(_, _) => DatabaseStateCode::CatalogLoadError,
            DatabaseState::WriteBufferCreationError(_, _) => {
                DatabaseStateCode::WriteBufferCreationError
            }
            DatabaseState::ReplayError(_, _) => DatabaseStateCode::ReplayError,
        }
    }

    pub(crate) fn error(&self) -> Option<&Arc<InitError>> {
        match self {
            DatabaseState::Known(_)
            | DatabaseState::Shutdown(_)
            | DatabaseState::OwnerInfoLoaded(_)
            | DatabaseState::RulesLoaded(_)
            | DatabaseState::CatalogLoaded(_)
            | DatabaseState::Initialized(_) => None,
            DatabaseState::OwnerInfoLoadError(_, e)
            | DatabaseState::RulesLoadError(_, e)
            | DatabaseState::CatalogLoadError(_, e)
            | DatabaseState::WriteBufferCreationError(_, e)
            | DatabaseState::ReplayError(_, e) => Some(e),
        }
    }

    pub(crate) fn provided_rules(&self) -> Option<Arc<ProvidedDatabaseRules>> {
        match self {
            DatabaseState::Known(_)
            | DatabaseState::Shutdown(_)
            | DatabaseState::OwnerInfoLoaded(_)
            | DatabaseState::OwnerInfoLoadError(_, _)
            | DatabaseState::RulesLoadError(_, _) => None,
            DatabaseState::RulesLoaded(state) | DatabaseState::CatalogLoadError(state, _) => {
                Some(Arc::clone(&state.provided_rules))
            }
            DatabaseState::CatalogLoaded(state)
            | DatabaseState::WriteBufferCreationError(state, _)
            | DatabaseState::ReplayError(state, _) => Some(Arc::clone(&state.provided_rules)),
            DatabaseState::Initialized(state) => Some(Arc::clone(&state.provided_rules)),
        }
    }

    pub(crate) fn owner_info(&self) -> Option<management::v1::OwnerInfo> {
        match self {
            DatabaseState::Known(_)
            | DatabaseState::Shutdown(_)
            | DatabaseState::OwnerInfoLoadError(_, _)
            | DatabaseState::RulesLoadError(_, _) => None,
            DatabaseState::OwnerInfoLoaded(state) => Some(state.owner_info.clone()),
            DatabaseState::RulesLoaded(state) | DatabaseState::CatalogLoadError(state, _) => {
                Some(state.owner_info.clone())
            }
            DatabaseState::CatalogLoaded(state)
            | DatabaseState::WriteBufferCreationError(state, _)
            | DatabaseState::ReplayError(state, _) => Some(state.owner_info.clone()),
            DatabaseState::Initialized(state) => Some(state.owner_info.clone()),
        }
    }

    /// Whether this is shutdown
    pub(crate) fn is_shutdown(&self) -> bool {
        matches!(self, DatabaseState::Shutdown(_))
    }

    pub(crate) fn get_initialized(&self) -> Option<&DatabaseStateInitialized> {
        match self {
            DatabaseState::Initialized(state) => Some(state),
            _ => None,
        }
    }

    /// Try to advance to the next state
    ///
    /// # Panic
    ///
    /// Panics if the database cannot be advanced (already initialized or shutdown)
    async fn advance(self, shared: &DatabaseShared) -> Self {
        match self {
            Self::Known(state) | Self::OwnerInfoLoadError(state, _) => {
                match state.advance(shared).await {
                    Ok(state) => Self::OwnerInfoLoaded(state),
                    Err(e) => Self::OwnerInfoLoadError(state, Arc::new(e)),
                }
            }
            Self::OwnerInfoLoaded(state) | Self::RulesLoadError(state, _) => {
                match state.advance(shared).await {
                    Ok(state) => Self::RulesLoaded(state),
                    Err(e) => Self::RulesLoadError(state, Arc::new(e)),
                }
            }
            Self::RulesLoaded(state) | Self::CatalogLoadError(state, _) => {
                match state.advance(shared).await {
                    Ok(state) => Self::CatalogLoaded(state),
                    Err(e) => Self::CatalogLoadError(state, Arc::new(e)),
                }
            }
            Self::CatalogLoaded(state) | Self::WriteBufferCreationError(state, _) => {
                match state.advance(shared).await {
                    Ok(state) => Self::Initialized(state),
                    Err(e @ InitError::CreateWriteBuffer { .. }) => {
                        Self::WriteBufferCreationError(state, Arc::new(e))
                    }
                    Err(e) => Self::ReplayError(state, Arc::new(e)),
                }
            }
            Self::ReplayError(state, _) => {
                let state2 = state.rollback();
                match state2.advance(shared).await {
                    Ok(state2) => match state2.advance(shared).await {
                        Ok(state2) => Self::Initialized(state2),
                        Err(e) => Self::ReplayError(state, Arc::new(e)),
                    },
                    Err(e) => Self::ReplayError(state, Arc::new(e)),
                }
            }
            Self::Initialized(_) => unreachable!(),
            Self::Shutdown(_) => unreachable!(),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct DatabaseStateKnown {}

impl DatabaseStateKnown {
    /// Load owner info from object storage and verify it matches the current owner
    async fn advance(
        &self,
        shared: &DatabaseShared,
    ) -> Result<DatabaseStateOwnerInfoLoaded, InitError> {
        let (server_id, uuid) = {
            let config = shared.config.read();
            (config.server_id, config.database_uuid)
        };

        let owner_info = shared
            .application
            .config_provider()
            .fetch_owner_info(server_id, uuid)
            .await
            .context(FetchingOwnerInfoSnafu)?;

        if owner_info.id != server_id.get_u32() {
            return DatabaseOwnerMismatchSnafu {
                actual: owner_info.id,
                expected: server_id.get_u32(),
            }
            .fail();
        }

        Ok(DatabaseStateOwnerInfoLoaded { owner_info })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct DatabaseStateOwnerInfoLoaded {
    owner_info: management::v1::OwnerInfo,
}

impl DatabaseStateOwnerInfoLoaded {
    /// Load database rules from object storage
    async fn advance(
        &self,
        shared: &DatabaseShared,
    ) -> Result<DatabaseStateRulesLoaded, InitError> {
        let uuid = shared.config.read().database_uuid;
        let provided_rules = shared
            .application
            .config_provider()
            .fetch_rules(uuid)
            .await
            .context(LoadingRulesSnafu)?;

        let db_name = shared.config.read().name.clone();
        if provided_rules.db_name() != &db_name {
            return RulesDatabaseNameMismatchSnafu {
                actual: provided_rules.db_name(),
                expected: db_name.as_str(),
            }
            .fail();
        }

        Ok(DatabaseStateRulesLoaded {
            provided_rules: Arc::new(provided_rules),
            owner_info: self.owner_info.clone(),
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct DatabaseStateRulesLoaded {
    provided_rules: Arc<ProvidedDatabaseRules>,
    owner_info: management::v1::OwnerInfo,
}

impl DatabaseStateRulesLoaded {
    /// Load catalog from object storage
    async fn advance(
        &self,
        shared: &DatabaseShared,
    ) -> Result<DatabaseStateCatalogLoaded, InitError> {
        let (db_name, wipe_catalog_on_error, skip_replay, server_id) = {
            let config = shared.config.read();
            (
                config.name.clone(),
                config.wipe_catalog_on_error,
                config.skip_replay,
                config.server_id,
            )
        };
        let (preserved_catalog, catalog, replay_plan) = load_or_create_preserved_catalog(
            db_name.as_str(),
            Arc::clone(&shared.iox_object_store),
            Arc::clone(shared.application.metric_registry()),
            Arc::clone(shared.application.time_provider()),
            wipe_catalog_on_error,
            skip_replay,
        )
        .await
        .context(CatalogLoadSnafu)?;

        let database_to_commit = DatabaseToCommit {
            server_id,
            iox_object_store: Arc::clone(&shared.iox_object_store),
            exec: Arc::clone(shared.application.executor()),
            rules: Arc::clone(self.provided_rules.rules()),
            preserved_catalog,
            catalog,
            metric_registry: Arc::clone(shared.application.metric_registry()),
            time_provider: Arc::clone(shared.application.time_provider()),
        };

        let db = Arc::new(Db::new(
            database_to_commit,
            Arc::clone(shared.application.job_registry()),
        ));

        let lifecycle_worker = Arc::new(LifecycleWorker::new(Arc::clone(&db)));

        Ok(DatabaseStateCatalogLoaded {
            db,
            lifecycle_worker,
            replay_plan: Arc::new(replay_plan),
            provided_rules: Arc::clone(&self.provided_rules),
            owner_info: self.owner_info.clone(),
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct DatabaseStateCatalogLoaded {
    db: Arc<Db>,
    lifecycle_worker: Arc<LifecycleWorker>,
    replay_plan: Arc<Option<ReplayPlan>>,
    provided_rules: Arc<ProvidedDatabaseRules>,
    owner_info: management::v1::OwnerInfo,
}

impl DatabaseStateCatalogLoaded {
    /// Perform replay
    async fn advance(
        &self,
        shared: &DatabaseShared,
    ) -> Result<DatabaseStateInitialized, InitError> {
        let db = Arc::clone(&self.db);

        let rules = self.provided_rules.rules();
        let trace_collector = shared.application.trace_collector();
        let write_buffer_factory = shared.application.write_buffer_factory();
        let (db_name, skip_replay) = {
            let config = shared.config.read();
            (config.name.clone(), config.skip_replay)
        };
        let write_buffer_consumer = match rules.write_buffer_connection.as_ref() {
            Some(connection) => {
                let consumer = write_buffer_factory
                    .new_config_read(db_name.as_str(), trace_collector.as_ref(), connection)
                    .await
                    .context(CreateWriteBufferSnafu)?;

                let replay_plan = if skip_replay {
                    None
                } else {
                    self.replay_plan.as_ref().as_ref()
                };

                let streams = db
                    .perform_replay(replay_plan, Arc::clone(&consumer))
                    .await
                    .context(ReplaySnafu)?;

                Some(Arc::new(WriteBufferConsumer::new(
                    consumer,
                    streams,
                    Arc::clone(&db),
                    shared.application.metric_registry().as_ref(),
                )))
            }
            _ => None,
        };

        self.lifecycle_worker.unsuppress_persistence();

        Ok(DatabaseStateInitialized {
            db,
            write_buffer_consumer,
            lifecycle_worker: Arc::clone(&self.lifecycle_worker),
            provided_rules: Arc::clone(&self.provided_rules),
            owner_info: self.owner_info.clone(),
        })
    }

    /// Rolls back state to an unloaded catalog.
    pub(crate) fn rollback(&self) -> DatabaseStateRulesLoaded {
        warn!(db_name=%self.db.name(), "throwing away loaded catalog to recover from replay error");
        DatabaseStateRulesLoaded {
            provided_rules: Arc::clone(&self.provided_rules),
            owner_info: self.owner_info.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct DatabaseStateInitialized {
    db: Arc<Db>,
    write_buffer_consumer: Option<Arc<WriteBufferConsumer>>,
    lifecycle_worker: Arc<LifecycleWorker>,
    provided_rules: Arc<ProvidedDatabaseRules>,
    owner_info: management::v1::OwnerInfo,
}

impl DatabaseStateInitialized {
    pub fn db(&self) -> &Arc<Db> {
        &self.db
    }

    pub fn write_buffer_consumer(&self) -> Option<&Arc<WriteBufferConsumer>> {
        self.write_buffer_consumer.as_ref()
    }

    pub fn set_provided_rules(&mut self, provided_rules: Arc<ProvidedDatabaseRules>) {
        self.provided_rules = provided_rules
    }

    /// Get a reference to the database state initialized's lifecycle worker.
    pub(crate) fn lifecycle_worker(&self) -> &Arc<LifecycleWorker> {
        &self.lifecycle_worker
    }
}

const INIT_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(500);

/// Try to drive the database to `DatabaseState::Initialized` returns when
/// this is achieved or the shutdown signal is triggered
pub(crate) async fn initialize_database(shared: &DatabaseShared, shutdown: CancellationToken) {
    let db_name = shared.config.read().name.clone();
    info!(%db_name, "database initialization started");

    // A backoff duration for retrying errors that will change over the course of multiple errors
    let mut backoff = INIT_BACKOFF;

    while !shutdown.is_cancelled() {
        let handle = shared.state.read().freeze();
        let handle = handle.await;

        // Re-acquire read lock to avoid holding lock across await point
        let state = DatabaseState::clone(&shared.state.read());

        info!(%db_name, %state, "attempting to advance database initialization state");

        match &state {
            DatabaseState::Initialized(_) => break,
            DatabaseState::Shutdown(_) => {
                info!(%db_name, "database in shutdown - aborting initialization");
                shutdown.cancel();
                return;
            }
            _ => {}
        }

        // Try to advance to the next state
        let next_state = tokio::select! {
            next_state = state.advance(shared) => next_state,
            _ = shutdown.cancelled() => {
                info!(%db_name, "initialization aborted by shutdown");
                return
            }
        };

        let state_code = next_state.state_code();
        let maybe_error = next_state.error().cloned();
        // Commit the next state
        {
            let mut state = shared.state.write();
            info!(%db_name, from=%state, to=%next_state, "database initialization state transition");

            *state.unfreeze(handle) = next_state;
            shared.state_notify.notify_waiters();
        }

        match maybe_error {
            Some(error) => {
                // exponential backoff w/ jitter, decorrelated
                // see https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
                backoff = Duration::from_secs_f64(
                    MAX_BACKOFF.as_secs_f64().min(
                        thread_rng()
                            .gen_range(INIT_BACKOFF.as_secs_f64()..(backoff.as_secs_f64() * 3.0)),
                    ),
                );

                error!(
                    %db_name,
                    %error,
                    state=%state_code,
                    backoff_secs = backoff.as_secs_f64(),
                    "database in error state - backing off initialization"
                );

                // Wait for timeout or shutdown signal
                tokio::select! {
                    _ = tokio::time::sleep(backoff) => {},
                    _ = shutdown.cancelled() => {}
                }
            }
            None => {
                // reset backoff
                backoff = INIT_BACKOFF;
            }
        }
    }
}

/// Create fresh database without any any state. Returns its location in object storage
/// for saving in the server config file.
pub async fn create_empty_db_in_object_store(
    application: Arc<ApplicationState>,
    uuid: Uuid,
    provided_rules: ProvidedDatabaseRules,
    server_id: ServerId,
) -> Result<String, InitError> {
    let db_name = provided_rules.db_name().clone();
    let iox_object_store = Arc::new(
        match IoxObjectStore::create(Arc::clone(application.object_store()), uuid).await {
            Ok(ios) => ios,
            Err(source) => return Err(InitError::IoxObjectStoreError { source }),
        },
    );

    let database_location = iox_object_store.root_path();

    application
        .config_provider()
        .create_owner_info(server_id, uuid)
        .await
        .context(CreatingOwnerInfoSnafu)?;

    application
        .config_provider()
        .store_rules(uuid, &provided_rules)
        .await
        .context(SavingRulesSnafu)?;

    create_preserved_catalog(
        &db_name,
        iox_object_store,
        Arc::clone(application.metric_registry()),
        Arc::clone(application.time_provider()),
        true,
    )
    .await
    .context(CannotCreatePreservedCatalogSnafu)?;

    Ok(database_location)
}

/// Create an claimed database without any state. Returns its
/// location in object storage for saving in the server config
/// file.
///
/// if `force` is true, a missing owner info or owner info that is
/// for the wrong server id are ignored (do not cause errors)
pub async fn claim_database_in_object_store(
    application: Arc<ApplicationState>,
    db_name: &DatabaseName<'static>,
    uuid: Uuid,
    server_id: ServerId,
    force: bool,
) -> Result<String, InitError> {
    info!(%db_name, %uuid, %force, "claiming database");

    let iox_object_store = IoxObjectStore::load(Arc::clone(application.object_store()), uuid)
        .await
        .context(IoxObjectStoreSnafu)?;

    let owner_info = application
        .config_provider()
        .fetch_owner_info(server_id, uuid)
        .await
        .context(FetchingOwnerInfoSnafu);

    // try to recreate owner_info if force is specified
    let owner_info = match owner_info {
        Err(_) if force => {
            warn!("Attempting to recreate missing owner info due to force");

            application
                .config_provider()
                .create_owner_info(server_id, uuid)
                .await
                .context(CreatingOwnerInfoSnafu)?;

            application
                .config_provider()
                .fetch_owner_info(server_id, uuid)
                .await
                .context(FetchingOwnerInfoSnafu)
        }
        t => t,
    }?;

    if owner_info.id != 0 {
        if !force {
            return CantClaimDatabaseCurrentlyOwnedSnafu {
                uuid,
                server_id: owner_info.id,
            }
            .fail();
        } else {
            warn!(
                owner_id = owner_info.id,
                "Ignoring owner info mismatch due to force"
            );
        }
    }

    let database_location = iox_object_store.root_path();
    application
        .config_provider()
        .update_owner_info(Some(server_id), uuid)
        .await
        .context(UpdatingOwnerInfoSnafu)?;

    Ok(database_location)
}
