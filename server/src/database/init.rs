//! Database initialization / creation logic
use crate::{
    database::owner::{
        fetch_owner_info, update_owner_info, OwnerInfoCreateError, OwnerInfoFetchError,
        OwnerInfoUpdateError,
    },
    rules::{PersistedDatabaseRules, ProvidedDatabaseRules},
    ApplicationState,
};
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
use uuid::Uuid;

use super::{owner::create_owner_info, state::DatabaseShared};

/// Errors encountered during initialization of a database
#[derive(Debug, Snafu)]
pub enum InitError {
    #[snafu(display("error finding database directory in object storage: {}", source))]
    DatabaseObjectStoreLookup {
        source: iox_object_store::IoxObjectStoreError,
    },

    #[snafu(display("no active database found in object storage, not loading"))]
    NoActiveDatabase,

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
    CreatingOwnerInfo { source: OwnerInfoCreateError },

    #[snafu(display("error getting database owner info: {}", source))]
    FetchingOwnerInfo { source: OwnerInfoFetchError },

    #[snafu(display("error updating database owner info: {}", source))]
    UpdatingOwnerInfo { source: OwnerInfoUpdateError },

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
    SavingRules { source: crate::rules::Error },

    #[snafu(display("error loading database rules: {}", source))]
    LoadingRules { source: crate::rules::Error },

    #[snafu(display("{}", source))]
    IoxObjectStoreError {
        source: iox_object_store::IoxObjectStoreError,
    },

    #[snafu(display("The database with UUID `{}` named `{}` is already active", uuid, name))]
    AlreadyActive { name: String, uuid: Uuid },

    #[snafu(display("cannot create preserved catalog: {}", source))]
    CannotCreatePreservedCatalog { source: db::load::Error },
}

/// The Database startup state machine
///
/// ```text
///                      (start)
///                         |
///  o-o           o--------|-------------------o                         o-o
///  | V           V        V                   V                         V |
/// [NoActiveDatabase]<--[Known]------------->[DatabaseObjectStoreLookupError]
///        |                |                           |
///        o----------------+---------------------------o
///                         |
///                         |                                     o-o
///                         V                                     V |
///              [DatabaseObjectStoreFound]------>[OwnerInfoLoadError]
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
///                         |
///                         V
///                       (end)
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
    // Basic initialization sequence states:
    Known(DatabaseStateKnown),
    DatabaseObjectStoreFound(DatabaseStateDatabaseObjectStoreFound),
    OwnerInfoLoaded(DatabaseStateOwnerInfoLoaded),
    RulesLoaded(DatabaseStateRulesLoaded),
    CatalogLoaded(DatabaseStateCatalogLoaded),

    // Terminal state (success)
    Initialized(DatabaseStateInitialized),

    // Error states, we'll try to recover from them
    NoActiveDatabase(DatabaseStateKnown, Arc<InitError>),
    DatabaseObjectStoreLookupError(DatabaseStateKnown, Arc<InitError>),
    OwnerInfoLoadError(DatabaseStateDatabaseObjectStoreFound, Arc<InitError>),
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

    // Construct a now active datbaase state
    pub fn new_no_active_database() -> Self {
        Self::NoActiveDatabase(DatabaseStateKnown {}, Arc::new(InitError::NoActiveDatabase))
    }

    pub(crate) fn state_code(&self) -> DatabaseStateCode {
        match self {
            DatabaseState::Known(_) => DatabaseStateCode::Known,
            DatabaseState::DatabaseObjectStoreFound(_) => {
                DatabaseStateCode::DatabaseObjectStoreFound
            }
            DatabaseState::OwnerInfoLoaded(_) => DatabaseStateCode::OwnerInfoLoaded,
            DatabaseState::RulesLoaded(_) => DatabaseStateCode::RulesLoaded,
            DatabaseState::CatalogLoaded(_) => DatabaseStateCode::CatalogLoaded,
            DatabaseState::Initialized(_) => DatabaseStateCode::Initialized,
            DatabaseState::DatabaseObjectStoreLookupError(_, _) => {
                DatabaseStateCode::DatabaseObjectStoreLookupError
            }
            DatabaseState::NoActiveDatabase(_, _) => DatabaseStateCode::NoActiveDatabase,
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
            | DatabaseState::DatabaseObjectStoreFound(_)
            | DatabaseState::OwnerInfoLoaded(_)
            | DatabaseState::RulesLoaded(_)
            | DatabaseState::CatalogLoaded(_)
            | DatabaseState::Initialized(_) => None,
            DatabaseState::DatabaseObjectStoreLookupError(_, e)
            | DatabaseState::NoActiveDatabase(_, e)
            | DatabaseState::OwnerInfoLoadError(_, e)
            | DatabaseState::RulesLoadError(_, e)
            | DatabaseState::CatalogLoadError(_, e)
            | DatabaseState::WriteBufferCreationError(_, e)
            | DatabaseState::ReplayError(_, e) => Some(e),
        }
    }

    pub(crate) fn provided_rules(&self) -> Option<Arc<ProvidedDatabaseRules>> {
        match self {
            DatabaseState::Known(_)
            | DatabaseState::DatabaseObjectStoreFound(_)
            | DatabaseState::DatabaseObjectStoreLookupError(_, _)
            | DatabaseState::NoActiveDatabase(_, _)
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

    pub(crate) fn uuid(&self) -> Option<Uuid> {
        match self {
            DatabaseState::Known(_)
            | DatabaseState::DatabaseObjectStoreFound(_)
            | DatabaseState::DatabaseObjectStoreLookupError(_, _)
            | DatabaseState::NoActiveDatabase(_, _)
            | DatabaseState::OwnerInfoLoaded(_)
            | DatabaseState::OwnerInfoLoadError(_, _)
            | DatabaseState::RulesLoadError(_, _) => None,
            DatabaseState::RulesLoaded(state) | DatabaseState::CatalogLoadError(state, _) => {
                Some(state.uuid)
            }
            DatabaseState::CatalogLoaded(state)
            | DatabaseState::WriteBufferCreationError(state, _)
            | DatabaseState::ReplayError(state, _) => Some(state.uuid),
            DatabaseState::Initialized(state) => Some(state.uuid),
        }
    }

    pub(crate) fn owner_info(&self) -> Option<management::v1::OwnerInfo> {
        match self {
            DatabaseState::Known(_)
            | DatabaseState::DatabaseObjectStoreFound(_)
            | DatabaseState::DatabaseObjectStoreLookupError(_, _)
            | DatabaseState::NoActiveDatabase(_, _)
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

    pub(crate) fn iox_object_store(&self) -> Option<Arc<IoxObjectStore>> {
        match self {
            DatabaseState::Known(_)
            | DatabaseState::DatabaseObjectStoreLookupError(_, _)
            | DatabaseState::NoActiveDatabase(_, _) => None,
            DatabaseState::DatabaseObjectStoreFound(state)
            | DatabaseState::OwnerInfoLoadError(state, _) => {
                Some(Arc::clone(&state.iox_object_store))
            }
            DatabaseState::OwnerInfoLoaded(state) | DatabaseState::RulesLoadError(state, _) => {
                Some(Arc::clone(&state.iox_object_store))
            }
            DatabaseState::RulesLoaded(state) | DatabaseState::CatalogLoadError(state, _) => {
                Some(Arc::clone(&state.iox_object_store))
            }
            DatabaseState::CatalogLoaded(state)
            | DatabaseState::WriteBufferCreationError(state, _)
            | DatabaseState::ReplayError(state, _) => Some(state.db.iox_object_store()),
            DatabaseState::Initialized(state) => Some(state.db.iox_object_store()),
        }
    }

    /// Whether the end user would want to know about this database or whether they would consider
    /// this database to be deleted
    pub(crate) fn is_active(&self) -> bool {
        !matches!(self, DatabaseState::NoActiveDatabase(_, _))
    }

    pub(crate) fn get_initialized(&self) -> Option<&DatabaseStateInitialized> {
        match self {
            DatabaseState::Initialized(state) => Some(state),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct DatabaseStateKnown {}

impl DatabaseStateKnown {
    /// Find active object storage for this database
    async fn advance(
        &self,
        shared: &DatabaseShared,
    ) -> Result<DatabaseStateDatabaseObjectStoreFound, InitError> {
        let location = shared.config.read().location.clone();
        let iox_object_store = IoxObjectStore::load_at_root_path(
            Arc::clone(shared.application.object_store()),
            &location,
        )
        .await
        .context(DatabaseObjectStoreLookupSnafu)?;

        Ok(DatabaseStateDatabaseObjectStoreFound {
            iox_object_store: Arc::new(iox_object_store),
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct DatabaseStateDatabaseObjectStoreFound {
    iox_object_store: Arc<IoxObjectStore>,
}

impl DatabaseStateDatabaseObjectStoreFound {
    /// Load owner info from object storage and verify it matches the current owner
    async fn advance(
        &self,
        shared: &DatabaseShared,
    ) -> Result<DatabaseStateOwnerInfoLoaded, InitError> {
        let owner_info = fetch_owner_info(&self.iox_object_store)
            .await
            .context(FetchingOwnerInfoSnafu)?;

        let server_id = shared.config.read().server_id.get_u32();
        if owner_info.id != server_id {
            return DatabaseOwnerMismatchSnafu {
                actual: owner_info.id,
                expected: server_id,
            }
            .fail();
        }

        Ok(DatabaseStateOwnerInfoLoaded {
            owner_info,
            iox_object_store: Arc::clone(&self.iox_object_store),
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct DatabaseStateOwnerInfoLoaded {
    owner_info: management::v1::OwnerInfo,
    iox_object_store: Arc<IoxObjectStore>,
}

impl DatabaseStateOwnerInfoLoaded {
    /// Load database rules from object storage
    async fn advance(
        &self,
        shared: &DatabaseShared,
    ) -> Result<DatabaseStateRulesLoaded, InitError> {
        let rules = PersistedDatabaseRules::load(&self.iox_object_store)
            .await
            .context(LoadingRulesSnafu)?;

        let db_name = shared.config.read().name.clone();
        if rules.db_name() != &db_name {
            return RulesDatabaseNameMismatchSnafu {
                actual: rules.db_name(),
                expected: db_name.as_str(),
            }
            .fail();
        }

        Ok(DatabaseStateRulesLoaded {
            provided_rules: rules.provided_rules(),
            uuid: rules.uuid(),
            owner_info: self.owner_info.clone(),
            iox_object_store: Arc::clone(&self.iox_object_store),
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct DatabaseStateRulesLoaded {
    provided_rules: Arc<ProvidedDatabaseRules>,
    uuid: Uuid,
    owner_info: management::v1::OwnerInfo,
    iox_object_store: Arc<IoxObjectStore>,
}

impl DatabaseStateRulesLoaded {
    pub(crate) fn iox_object_store(&self) -> &Arc<IoxObjectStore> {
        &self.iox_object_store
    }

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
            Arc::clone(&self.iox_object_store),
            Arc::clone(shared.application.metric_registry()),
            Arc::clone(shared.application.time_provider()),
            wipe_catalog_on_error,
            skip_replay,
        )
        .await
        .context(CatalogLoadSnafu)?;

        let database_to_commit = DatabaseToCommit {
            server_id,
            iox_object_store: Arc::clone(&self.iox_object_store),
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
            uuid: self.uuid,
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
    uuid: Uuid,
    owner_info: management::v1::OwnerInfo,
}

impl DatabaseStateCatalogLoaded {
    pub(crate) fn iox_object_store(&self) -> Arc<IoxObjectStore> {
        self.db.iox_object_store()
    }

    /// Perform replay
    async fn advance(
        &self,
        shared: &DatabaseShared,
    ) -> Result<DatabaseStateInitialized, InitError> {
        let db = Arc::clone(&self.db);

        let rules = self.provided_rules.rules();
        let trace_collector = shared.application.trace_collector();
        let write_buffer_factory = shared.application.write_buffer_factory();
        let (db_name, server_id, skip_replay) = {
            let config = shared.config.read();
            (config.name.clone(), config.server_id, config.skip_replay)
        };
        let write_buffer_consumer = match rules.write_buffer_connection.as_ref() {
            Some(connection) => {
                let mut consumer = write_buffer_factory
                    .new_config_read(
                        server_id,
                        db_name.as_str(),
                        trace_collector.as_ref(),
                        connection,
                    )
                    .await
                    .context(CreateWriteBufferSnafu)?;

                let replay_plan = if skip_replay {
                    None
                } else {
                    self.replay_plan.as_ref().as_ref()
                };

                db.perform_replay(replay_plan, consumer.as_mut())
                    .await
                    .context(ReplaySnafu)?;

                Some(Arc::new(WriteBufferConsumer::new(
                    consumer,
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
            uuid: self.uuid,
            owner_info: self.owner_info.clone(),
        })
    }

    /// Rolls back state to an unloaded catalog.
    pub(crate) fn rollback(&self) -> DatabaseStateRulesLoaded {
        warn!(db_name=%self.db.name(), "throwing away loaded catalog to recover from replay error");
        DatabaseStateRulesLoaded {
            provided_rules: Arc::clone(&self.provided_rules),
            uuid: self.uuid,
            owner_info: self.owner_info.clone(),
            iox_object_store: self.db.iox_object_store(),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct DatabaseStateInitialized {
    db: Arc<Db>,
    write_buffer_consumer: Option<Arc<WriteBufferConsumer>>,
    lifecycle_worker: Arc<LifecycleWorker>,
    provided_rules: Arc<ProvidedDatabaseRules>,
    uuid: Uuid,
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

    pub fn provided_rules(&self) -> &Arc<ProvidedDatabaseRules> {
        &self.provided_rules
    }

    pub fn uuid(&self) -> Uuid {
        self.uuid
    }

    /// Get a reference to the database state initialized's lifecycle worker.
    pub(crate) fn lifecycle_worker(&self) -> &Arc<LifecycleWorker> {
        &self.lifecycle_worker
    }
}

/// Determine what the init loop should do next.
enum TransactionOrWait {
    /// We can transition from one state into another.
    Transaction(DatabaseState, internal_types::freezable::FreezeHandle),

    /// We have to wait to backoff from an error.
    Wait(Duration),
}

const INIT_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(500);

/// Try to drive the database to `DatabaseState::Initialized` returns when
/// this is achieved or the shutdown signal is triggered
pub(crate) async fn initialize_database(shared: &DatabaseShared) {
    let db_name = shared.config.read().name.clone();
    info!(%db_name, "database initialization started");

    // error throttling
    // - checks if the current error was already throttled
    // - keeps a backoff duration that will change over the course of multiple errors
    let mut throttled_error = false;
    let mut backoff = INIT_BACKOFF;

    while !shared.shutdown.is_cancelled() {
        // Acquire locks and determine if work to be done
        let maybe_transaction = {
            // lock-dance to make this future `Send`
            let handle = shared.state.read().freeze();
            let handle = handle.await;
            let state = shared.state.read();

            match &**state {
                // Already initialized
                DatabaseState::Initialized(_) => break,

                // Can perform work
                _ if state.error().is_none() || (state.error().is_some() && throttled_error) => {
                    TransactionOrWait::Transaction(DatabaseState::clone(&state), handle)
                }

                // Unthrottled error state, need to wait
                _ => {
                    let e = state
                        .error()
                        .expect("How did we end up in a non-error state?");
                    error!(
                        %db_name,
                        %e,
                        %state,
                        "database in error state - wait until retry"
                    );
                    throttled_error = true;

                    // exponential backoff w/ jitter, decorrelated
                    // see https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
                    let mut rng = thread_rng();
                    backoff = Duration::from_secs_f64(MAX_BACKOFF.as_secs_f64().min(
                        rng.gen_range(INIT_BACKOFF.as_secs_f64()..(backoff.as_secs_f64() * 3.0)),
                    ));
                    TransactionOrWait::Wait(backoff)
                }
            }
        };

        // Backoff if no work to be done
        let (state, handle) = match maybe_transaction {
            TransactionOrWait::Transaction(state, handle) => (state, handle),
            TransactionOrWait::Wait(d) => {
                info!(%db_name, "backing off initialization");
                tokio::time::sleep(d).await;
                continue;
            }
        };

        info!(%db_name, %state, "attempting to advance database initialization state");

        // Try to advance to the next state
        let next_state = match state {
            DatabaseState::Known(state)
            | DatabaseState::DatabaseObjectStoreLookupError(state, _)
            | DatabaseState::NoActiveDatabase(state, _) => match state.advance(shared).await {
                Ok(state) => DatabaseState::DatabaseObjectStoreFound(state),
                Err(InitError::NoActiveDatabase) => {
                    DatabaseState::NoActiveDatabase(state, Arc::new(InitError::NoActiveDatabase))
                }
                Err(e) => DatabaseState::DatabaseObjectStoreLookupError(state, Arc::new(e)),
            },
            DatabaseState::DatabaseObjectStoreFound(state)
            | DatabaseState::OwnerInfoLoadError(state, _) => match state.advance(shared).await {
                Ok(state) => DatabaseState::OwnerInfoLoaded(state),
                Err(e) => DatabaseState::OwnerInfoLoadError(state, Arc::new(e)),
            },
            DatabaseState::OwnerInfoLoaded(state) | DatabaseState::RulesLoadError(state, _) => {
                match state.advance(shared).await {
                    Ok(state) => DatabaseState::RulesLoaded(state),
                    Err(e) => DatabaseState::RulesLoadError(state, Arc::new(e)),
                }
            }
            DatabaseState::RulesLoaded(state) | DatabaseState::CatalogLoadError(state, _) => {
                match state.advance(shared).await {
                    Ok(state) => DatabaseState::CatalogLoaded(state),
                    Err(e) => DatabaseState::CatalogLoadError(state, Arc::new(e)),
                }
            }
            DatabaseState::CatalogLoaded(state)
            | DatabaseState::WriteBufferCreationError(state, _) => {
                match state.advance(shared).await {
                    Ok(state) => DatabaseState::Initialized(state),
                    Err(e @ InitError::CreateWriteBuffer { .. }) => {
                        DatabaseState::WriteBufferCreationError(state, Arc::new(e))
                    }
                    Err(e) => DatabaseState::ReplayError(state, Arc::new(e)),
                }
            }
            DatabaseState::ReplayError(state, _) => {
                let state2 = state.rollback();
                match state2.advance(shared).await {
                    Ok(state2) => match state2.advance(shared).await {
                        Ok(state2) => DatabaseState::Initialized(state2),
                        Err(e) => DatabaseState::ReplayError(state, Arc::new(e)),
                    },
                    Err(e) => DatabaseState::ReplayError(state, Arc::new(e)),
                }
            }
            DatabaseState::Initialized(_) => {
                unreachable!("{:?}", state)
            }
        };

        if next_state.error().is_some() {
            // this is a new error that needs to be throttled
            throttled_error = false;
        } else {
            // reset backoff
            backoff = INIT_BACKOFF;
        }

        // Commit the next state
        {
            let mut state = shared.state.write();
            info!(%db_name, from=%state, to=%next_state, "database initialization state transition");

            *state.unfreeze(handle) = next_state;
            shared.state_notify.notify_waiters();
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
    let server_location =
        IoxObjectStore::server_config_path(application.object_store(), server_id).to_string();

    create_owner_info(server_id, server_location, &iox_object_store)
        .await
        .context(CreatingOwnerInfoSnafu)?;

    let rules_to_persist = PersistedDatabaseRules::new(uuid, provided_rules);
    rules_to_persist
        .persist(&iox_object_store)
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

    let owner_info = fetch_owner_info(&iox_object_store)
        .await
        .context(FetchingOwnerInfoSnafu);

    // try to recreate owner_info if force is specified
    let owner_info = match owner_info {
        Err(_) if force => {
            warn!("Attempting to recreate missing owner info due to force");

            let server_location =
                IoxObjectStore::server_config_path(application.object_store(), server_id)
                    .to_string();

            create_owner_info(server_id, server_location, &iox_object_store)
                .await
                .context(CreatingOwnerInfoSnafu)?;

            fetch_owner_info(&iox_object_store)
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
    let server_location =
        IoxObjectStore::server_config_path(application.object_store(), server_id).to_string();

    update_owner_info(
        Some(server_id),
        Some(server_location),
        application.time_provider().now(),
        &iox_object_store,
    )
    .await
    .context(UpdatingOwnerInfoSnafu)?;

    Ok(database_location)
}
