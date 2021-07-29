use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, RwLock},
};

use data_types::{
    database_rules::DatabaseRules, database_state::DatabaseStateCode, server_id::ServerId,
    DatabaseName,
};
use metrics::MetricRegistry;
use object_store::{path::ObjectStorePath, ObjectStore, ObjectStoreApi};
use parquet_file::catalog::PreservedCatalog;
use persistence_windows::checkpoint::ReplayPlan;
use query::exec::Executor;
use write_buffer::config::WriteBufferConfig;

/// This module contains code for managing the configuration of the server.
use crate::{
    db::{catalog::Catalog, DatabaseToCommit, Db},
    DatabaseAlreadyExists, DatabaseNotFound, DatabaseReserved, Error,
    InvalidDatabaseStateTransition, JobRegistry, Result, RulesDatabaseNameMismatch,
    ServerShuttingDown,
};
use object_store::path::Path;
use observability_deps::tracing::{self, error, info, warn, Instrument};
use snafu::{ensure, OptionExt};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub(crate) const DB_RULES_FILE_NAME: &str = "rules.pb";

/// The Config tracks the configuration of databases and their rules along
/// with host groups for replication. It is used as an in-memory structure
/// that can be loaded incrementally from object storage.
///
/// drain() should be called prior to drop to ensure termination
/// of background worker tasks. They will be cancelled on drop
/// but they are effectively "detached" at that point, and they may not
/// run to completion if the tokio runtime is dropped
#[derive(Debug)]
pub(crate) struct Config {
    jobs: Arc<JobRegistry>,
    object_store: Arc<ObjectStore>,
    exec: Arc<Executor>,
    server_id: ServerId,
    metric_registry: Arc<MetricRegistry>,

    shutdown: CancellationToken,
    state: RwLock<ConfigState>,
}

pub(crate) enum UpdateError<E> {
    Update(Error),
    Closure(E),
}

impl<E> From<Error> for UpdateError<E> {
    fn from(e: Error) -> Self {
        Self::Update(e)
    }
}

impl Config {
    /// Create new empty config.
    pub(crate) fn new(
        jobs: Arc<JobRegistry>,
        object_store: Arc<ObjectStore>,
        exec: Arc<Executor>,
        server_id: ServerId,
        metric_registry: Arc<MetricRegistry>,
    ) -> Self {
        Self {
            jobs,
            object_store,
            exec,
            server_id,
            metric_registry,
            shutdown: Default::default(),
            state: Default::default(),
        }
    }

    /// Get handle to create a database.
    ///
    /// The handle present a database in the [`Known`](DatabaseStateCode::Known) state. Note that until the handle is
    /// [committed](DatabaseHandle::commit) the database will not be present in the config. Hence
    /// [aborting](DatabaseHandle::abort) will discard the to-be-created database.
    ///
    /// While the handle is held, no other operations for the given database can be executed.
    ///
    /// This only works if the database is not yet known. To recover a database out of an uninitialized state, see
    /// [`recover_db`](Self::recover_db). To do maintainance work on data linked to the database (e.g. the catalog)
    /// without initializing it, see [`block_db`](Self::block_db).
    pub(crate) fn create_db(&self, db_name: DatabaseName<'static>) -> Result<DatabaseHandle<'_>> {
        let mut state = self.state.write().expect("mutex poisoned");
        ensure!(
            !state.reservations.contains(&db_name),
            DatabaseReserved { db_name }
        );
        ensure!(
            !state.databases.contains_key(&db_name),
            DatabaseAlreadyExists { db_name }
        );

        state.reservations.insert(db_name.clone());
        Ok(DatabaseHandle {
            state: Some(Arc::new(DatabaseState::Known { db_name })),
            config: &self,
        })
    }

    /// Get handle to recover database out of an uninitialized state.
    ///
    /// The state of the handle will be identical to the one that was last committed.
    ///
    /// While the handle is held, no other operations for the given database can be executed.
    ///
    /// This only works if the database is known but is uninitialized. To create a new database that is not yet known,
    /// see [`create_db`](Self::create_db). To do maintenance work on data linked to the database (e.g. the catalog)
    /// without initializing it, see [`block_db`](Self::block_db).
    pub(crate) fn recover_db(&self, db_name: DatabaseName<'static>) -> Result<DatabaseHandle<'_>> {
        let mut state = self.state.write().expect("mutex poisoned");
        ensure!(
            !state.reservations.contains(&db_name),
            DatabaseReserved { db_name }
        );

        let db_state = state
            .databases
            .get(&db_name)
            .cloned()
            .context(DatabaseNotFound { db_name: &db_name })?;

        ensure!(
            !db_state.is_initialized(),
            DatabaseAlreadyExists { db_name }
        );

        state.reservations.insert(db_name);
        Ok(DatabaseHandle {
            state: Some(db_state),
            config: &self,
        })
    }

    /// Get guard that blocks database creations. Useful when messing with the preserved catalog of unregistered DBs.
    ///
    /// While the handle is held, no other operations for the given database can be executed.
    ///
    /// This only works if the database is not yet registered. To create a new database that is not yet known,
    /// see [`create_db`](Self::create_db). To recover a database out of an uninitialized state, see
    /// [`recover_db`](Self::recover_db).
    pub(crate) fn block_db(
        &self,
        db_name: DatabaseName<'static>,
    ) -> Result<BlockDatabaseGuard<'_>> {
        let mut state = self.state.write().expect("mutex poisoned");
        ensure!(
            !state.reservations.contains(&db_name),
            DatabaseReserved { db_name }
        );
        ensure!(
            !state.databases.contains_key(&db_name),
            DatabaseAlreadyExists { db_name }
        );

        state.reservations.insert(db_name.clone());
        Ok(BlockDatabaseGuard {
            db_name: Some(db_name),
            config: &self,
        })
    }

    /// Get database, if registered and fully initialized.
    pub(crate) fn db_initialized(&self, name: &DatabaseName<'_>) -> Option<Arc<Db>> {
        let state = self.state.read().expect("mutex poisoned");
        state
            .databases
            .get(name)
            .map(|db_state| db_state.db_initialized())
            .flatten()
    }

    /// Check if there is a database with the given name that is registered but is uninitialized.
    pub(crate) fn has_uninitialized_database(&self, name: &DatabaseName<'_>) -> bool {
        let state = self.state.read().expect("mutex poisoned");
        state
            .databases
            .get(name)
            .map(|db_state| !db_state.is_initialized())
            .unwrap_or(false)
    }

    /// Current database init state
    pub(crate) fn db_state(&self, name: &DatabaseName<'_>) -> Option<DatabaseStateCode> {
        let state = self.state.read().expect("mutex poisoned");
        state.databases.get(name).map(|db_state| db_state.code())
    }

    /// Get all database names in all states (blocked, uninitialized, fully initialized).
    pub(crate) fn db_names_sorted(&self) -> Vec<DatabaseName<'static>> {
        let state = self.state.read().expect("mutex poisoned");
        let mut names: Vec<_> = state
            .reservations
            .iter()
            .cloned()
            .chain(state.databases.keys().cloned())
            .collect();
        names.sort();
        names
    }

    /// Update database rules of a fully initialized database.
    pub(crate) fn update_db_rules<F, E>(
        &self,
        db_name: &DatabaseName<'static>,
        update: F,
    ) -> std::result::Result<Arc<DatabaseRules>, UpdateError<E>>
    where
        F: FnOnce(DatabaseRules) -> std::result::Result<DatabaseRules, E>,
    {
        // TODO: implement for non-initialized databases
        let db = self
            .db_initialized(db_name)
            .context(DatabaseNotFound { db_name })?;

        db.update_rules(update).map_err(UpdateError::Closure)
    }

    /// Commit new or unchanged database state.
    fn commit_db(&self, db_state: Arc<DatabaseState>) {
        let mut state = self.state.write().expect("mutex poisoned");
        let name = db_state.db_name();
        state.databases.insert(name.clone(), db_state);
        state.reservations.remove(&name);
    }

    /// Forgets reservation for the given database.
    fn forget_reservation(&self, name: &DatabaseName<'static>) {
        let mut state = self.state.write().expect("mutex poisoned");
        state.reservations.remove(name);
    }

    /// Cancels and drains all background worker tasks
    pub(crate) async fn drain(&self) {
        info!("shutting down database background workers");

        // This will cancel all background child tasks
        self.shutdown.cancel();

        let handles: Vec<_> = {
            let mut state = self.state.write().expect("mutex poisoned");

            let mut databases = BTreeMap::new();
            std::mem::swap(&mut databases, &mut state.databases);

            databases
                .into_iter()
                .filter_map(|(_, db_state)| {
                    Arc::try_unwrap(db_state)
                        .expect("who else has a DB handle here?!")
                        .join()
                })
                .collect()
        };

        for handle in handles {
            let _ = handle.await;
        }

        info!("database background workers shutdown");
    }

    /// Metrics registry associated with this config and that should be used to create all databases.
    pub fn metrics_registry(&self) -> Arc<MetricRegistry> {
        Arc::clone(&self.metric_registry)
    }

    /// Returns the object store of this server
    pub fn object_store(&self) -> Arc<ObjectStore> {
        Arc::clone(&self.object_store)
    }

    /// Returns the server id of this server
    pub fn server_id(&self) -> ServerId {
        self.server_id
    }

    /// Base location in object store for this server.
    pub fn root_path(&self) -> Path {
        let id = self.server_id.get();
        let mut path = self.object_store.new_path();
        path.push_dir(format!("{}", id));
        path
    }
}

/// Get object store path for the database config under the given root (= path under with the server with the current ID
/// stores all its data).
pub fn object_store_path_for_database_config<P: ObjectStorePath>(
    root: &P,
    name: &DatabaseName<'_>,
) -> P {
    let mut path = root.clone();
    path.push_dir(name.to_string());
    path.set_file_name(DB_RULES_FILE_NAME);
    path
}

/// Inner config state that is protected by a lock.
#[derive(Default, Debug)]
struct ConfigState {
    /// Databases for which there are handled but that are not yet committed to `databases`.
    reservations: BTreeSet<DatabaseName<'static>>,

    /// Databases in different states.
    databases: BTreeMap<DatabaseName<'static>, Arc<DatabaseState>>,
}

/// Internal representation of the different database states.
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
enum DatabaseState {
    /// Database is known but nothing is loaded.
    Known { db_name: DatabaseName<'static> },

    /// Rules are loaded
    RulesLoaded { rules: Arc<DatabaseRules> },

    /// Catalog is loaded but data from sequencers / write buffers is not yet replayed.
    Replay {
        db: Arc<Db>,
        replay_plan: ReplayPlan,
    },

    /// Fully initialized database.
    Initialized {
        db: Arc<Db>,
        handle: Option<JoinHandle<()>>,
        shutdown: CancellationToken,
    },
}

impl DatabaseState {
    fn join(&mut self) -> Option<JoinHandle<()>> {
        match self {
            DatabaseState::Initialized { handle, .. } => handle.take(),
            _ => None,
        }
    }

    fn code(&self) -> DatabaseStateCode {
        match self {
            DatabaseState::Known { .. } => DatabaseStateCode::Known,
            DatabaseState::RulesLoaded { .. } => DatabaseStateCode::RulesLoaded,
            DatabaseState::Replay { .. } => DatabaseStateCode::Replay,
            DatabaseState::Initialized { .. } => DatabaseStateCode::Initialized,
        }
    }

    fn is_initialized(&self) -> bool {
        matches!(self, DatabaseState::Initialized { .. })
    }

    fn db_any_state(&self) -> Option<Arc<Db>> {
        match self {
            DatabaseState::Replay { db, .. } => Some(Arc::clone(&db)),
            DatabaseState::Initialized { db, .. } => Some(Arc::clone(&db)),
            _ => None,
        }
    }

    fn db_initialized(&self) -> Option<Arc<Db>> {
        match self {
            DatabaseState::Initialized { db, .. } => Some(Arc::clone(&db)),
            _ => None,
        }
    }

    fn db_name(&self) -> DatabaseName<'static> {
        match self {
            DatabaseState::Known { db_name, .. } => db_name.clone(),
            DatabaseState::RulesLoaded { rules, .. } => rules.name.clone(),
            DatabaseState::Replay { db, .. } => db.rules().name.clone(),
            DatabaseState::Initialized { db, .. } => db.rules().name.clone(),
        }
    }

    fn rules(&self) -> Option<Arc<DatabaseRules>> {
        match self {
            DatabaseState::Known { .. } => None,
            DatabaseState::RulesLoaded { rules, .. } => Some(Arc::clone(&rules)),
            DatabaseState::Replay { db, .. } => Some(db.rules()),
            DatabaseState::Initialized { db, .. } => Some(db.rules()),
        }
    }

    fn replay_plan(&self) -> Option<ReplayPlan> {
        match self {
            DatabaseState::Known { .. } => None,
            DatabaseState::RulesLoaded { .. } => None,
            DatabaseState::Replay { replay_plan, .. } => Some(replay_plan.clone()),
            DatabaseState::Initialized { .. } => None,
        }
    }
}

impl Drop for DatabaseState {
    fn drop(&mut self) {
        if let DatabaseState::Initialized {
            handle, shutdown, ..
        } = self
        {
            if handle.is_some() {
                // Join should be called on `DatabaseState` prior to dropping, for example, by
                // calling drain() on the owning `Config`
                warn!("DatabaseState dropped without waiting for background task to complete");
                shutdown.cancel();
            }
        }
    }
}

/// This handle is returned when a call is made to [`create_db`](Config::create_db) or
/// [`recover_db`](Config::recover_db) on the Config struct. The handle can be used to hold a reservation for the
/// database name. Calling `commit` on the handle will consume the struct and move the database from reserved to being
/// in the config.
///
/// # Concurrent Actions
/// The goal is to ensure that database names can be reserved with minimal time holding a write lock on the config
/// state. This allows the caller (the server) to reserve the database name, setup the database (e.g. load the preserved
/// catalog), persist its configuration and then commit the change in-memory after it has been persisted.
///
/// # State
/// The handle represents databases in different states. The state can be queries with [`DatabaseHandle::state_code`].
/// See [`DatabaseStateCode`] for the description of the different states. States can be advances by using on of the
/// `advance_*` methods.
#[derive(Debug)]
pub(crate) struct DatabaseHandle<'a> {
    /// Partial moves aren't supported on structures that implement Drop
    /// so use Option to allow taking DatabaseRules out in `commit`
    state: Option<Arc<DatabaseState>>,
    config: &'a Config,
}

impl<'a> DatabaseHandle<'a> {
    fn state(&self) -> Arc<DatabaseState> {
        Arc::clone(&self.state.as_ref().expect("not committed"))
    }

    /// Get current [`DatabaseStateCode`] associated with this handle.
    pub fn state_code(&self) -> DatabaseStateCode {
        self.state().code()
    }

    /// Get database name.
    pub fn db_name(&self) -> DatabaseName<'static> {
        self.state().db_name()
    }

    /// Get object store.
    pub fn object_store(&self) -> Arc<ObjectStore> {
        Arc::clone(&self.config.object_store)
    }

    /// Get server ID.
    pub fn server_id(&self) -> ServerId {
        self.config.server_id
    }

    /// Get metrics registry.
    pub fn metrics_registry(&self) -> Arc<MetricRegistry> {
        self.config.metrics_registry()
    }

    /// Get rules, if already known in the current state.
    pub fn rules(&self) -> Option<Arc<DatabaseRules>> {
        self.state().rules()
    }

    /// Get database linked to this state, if any
    ///
    /// This database may be uninitialized.
    pub fn db_any_state(&self) -> Option<Arc<Db>> {
        self.state().db_any_state()
    }

    /// Get replay plan, if any.
    pub fn replay_plan(&self) -> Option<ReplayPlan> {
        self.state().replay_plan()
    }

    /// Commit modification done to this handle to config.
    ///
    /// After commiting a new handle for the same database can be created.
    pub fn commit(mut self) {
        let state = self.state.take().expect("not committed");
        self.config.commit_db(state);
    }

    /// Discard modification done to this handle.
    ///
    /// After aborting a new handle for the same database can be created.
    pub fn abort(mut self) {
        let state = self.state.take().expect("not committed");
        self.config.forget_reservation(&state.db_name())
    }

    /// Advance database state to [`RulesLoaded`](DatabaseStateCode::RulesLoaded).
    pub fn advance_rules_loaded(&mut self, rules: DatabaseRules) -> Result<()> {
        match self.state().as_ref() {
            DatabaseState::Known { db_name } => {
                ensure!(
                    db_name == &rules.name,
                    RulesDatabaseNameMismatch {
                        actual: rules.name,
                        expected: db_name,
                    }
                );

                self.state = Some(Arc::new(DatabaseState::RulesLoaded {
                    rules: Arc::new(rules),
                }));

                Ok(())
            }
            state => InvalidDatabaseStateTransition {
                actual: state.code(),
                expected: DatabaseStateCode::Known,
            }
            .fail(),
        }
    }

    /// Advance database state to [`Replay`](DatabaseStateCode::Replay).
    pub fn advance_replay(
        &mut self,
        preserved_catalog: PreservedCatalog,
        catalog: Catalog,
        replay_plan: ReplayPlan,
        write_buffer: Option<WriteBufferConfig>,
    ) -> Result<()> {
        match self.state().as_ref() {
            DatabaseState::RulesLoaded { rules } => {
                let database_to_commit = DatabaseToCommit {
                    server_id: self.config.server_id,
                    object_store: Arc::clone(&self.config.object_store),
                    exec: Arc::clone(&self.config.exec),
                    preserved_catalog,
                    catalog,
                    rules: Arc::clone(&rules),
                    write_buffer,
                };
                let db = Arc::new(Db::new(database_to_commit, Arc::clone(&self.config.jobs)));

                self.state = Some(Arc::new(DatabaseState::Replay { db, replay_plan }));

                Ok(())
            }
            state => InvalidDatabaseStateTransition {
                actual: state.code(),
                expected: DatabaseStateCode::RulesLoaded,
            }
            .fail(),
        }
    }

    /// Advance database state to [`Initialized`](DatabaseStateCode::Initialized).
    pub fn advance_init(&mut self) -> Result<()> {
        match self.state().as_ref() {
            DatabaseState::Replay { db, .. } => {
                if self.config.shutdown.is_cancelled() {
                    error!("server is shutting down");
                    return ServerShuttingDown.fail();
                }

                let shutdown = self.config.shutdown.child_token();
                let shutdown_captured = shutdown.clone();
                let db_captured = Arc::clone(&db);
                let rules = db.rules();

                let handle = Some(tokio::spawn(async move {
                    db_captured
                        .background_worker(shutdown_captured)
                        .instrument(tracing::info_span!("db_worker", database=%rules.name))
                        .await
                }));

                self.state = Some(Arc::new(DatabaseState::Initialized {
                    db: Arc::clone(&db),
                    handle,
                    shutdown,
                }));

                Ok(())
            }
            state => InvalidDatabaseStateTransition {
                actual: state.code(),
                expected: DatabaseStateCode::Replay,
            }
            .fail(),
        }
    }
}

impl<'a> Drop for DatabaseHandle<'a> {
    fn drop(&mut self) {
        if let Some(state) = self.state.as_ref() {
            self.config.forget_reservation(&state.db_name())
        }
    }
}

/// Guard that blocks DB creations. Useful when messing with the preserved catalog of unregistered DBs.
#[derive(Debug)]
pub(crate) struct BlockDatabaseGuard<'a> {
    /// Partial moves aren't supported on structures that implement Drop
    /// so use Option to allow taking DatabaseRules out in `commit`
    db_name: Option<DatabaseName<'static>>,
    config: &'a Config,
}

impl<'a> Drop for BlockDatabaseGuard<'a> {
    fn drop(&mut self) {
        if let Some(db_name) = self.db_name.take() {
            self.config.forget_reservation(&db_name)
        }
    }
}

#[cfg(test)]
mod test {
    use std::convert::TryFrom;

    use object_store::{ObjectStore, ObjectStoreApi};

    use crate::db::load::load_or_create_preserved_catalog;

    use super::*;

    fn make_config() -> Config {
        let store = Arc::new(ObjectStore::new_in_memory());
        let server_id = ServerId::try_from(1).unwrap();
        let metric_registry = Arc::new(metrics::MetricRegistry::new());
        Config::new(
            Arc::new(JobRegistry::new()),
            Arc::clone(&store),
            Arc::new(Executor::new(1)),
            server_id,
            Arc::clone(&metric_registry),
        )
    }

    #[tokio::test]
    async fn create_db() {
        // setup
        let name = DatabaseName::new("foo").unwrap();
        let config = make_config();
        let rules = DatabaseRules::new(name.clone());

        // getting handle while DB is reserved => fails
        {
            let _db_reservation = config.create_db(name.clone()).unwrap();

            let err = config.create_db(name.clone()).unwrap_err();
            assert!(matches!(err, Error::DatabaseReserved { .. }));

            let err = config.block_db(name.clone()).unwrap_err();
            assert!(matches!(err, Error::DatabaseReserved { .. }));
        }
        assert!(config.db_initialized(&name).is_none());
        assert_eq!(config.db_names_sorted(), vec![]);
        assert!(!config.has_uninitialized_database(&name));

        // name in rules must match reserved name
        {
            let mut db_reservation = config.create_db(DatabaseName::new("bar").unwrap()).unwrap();

            let err = db_reservation
                .advance_rules_loaded(rules.clone())
                .unwrap_err();
            assert!(matches!(err, Error::RulesDatabaseNameMismatch { .. }));
        }
        assert!(config.db_initialized(&name).is_none());
        assert_eq!(config.db_names_sorted(), vec![]);
        assert!(!config.has_uninitialized_database(&name));

        // handle.abort just works (aka does not mess up the transaction afterwards)
        {
            let db_reservation = config.create_db(DatabaseName::new("bar").unwrap()).unwrap();

            db_reservation.abort();
        }
        assert!(config.db_initialized(&name).is_none());
        assert_eq!(config.db_names_sorted(), vec![]);
        assert!(!config.has_uninitialized_database(&name));

        // create DB successfull
        {
            let mut db_reservation = config.create_db(name.clone()).unwrap();

            db_reservation.advance_rules_loaded(rules).unwrap();

            let (preserved_catalog, catalog, replay_plan) = load_or_create_preserved_catalog(
                &name,
                config.object_store(),
                config.server_id(),
                config.metrics_registry(),
                false,
            )
            .await
            .unwrap();
            db_reservation
                .advance_replay(preserved_catalog, catalog, replay_plan, None)
                .unwrap();

            db_reservation.advance_init().unwrap();

            db_reservation.commit();
        }
        assert!(config.db_initialized(&name).is_some());
        assert_eq!(config.db_names_sorted(), vec![name.clone()]);
        assert!(!config.has_uninitialized_database(&name));

        // check that background workers is running
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        assert!(
            config
                .db_initialized(&name)
                .expect("expected database")
                .worker_iterations_lifecycle()
                > 0
        );
        assert!(
            config
                .db_initialized(&name)
                .expect("expected database")
                .worker_iterations_cleanup()
                > 0
        );

        // recover a fully initialzed DB => fail
        let err = config.recover_db(name.clone()).unwrap_err();
        assert!(matches!(err, Error::DatabaseAlreadyExists { .. }));

        // create DB as second time => fail
        let err = config.create_db(name.clone()).unwrap_err();
        assert!(matches!(err, Error::DatabaseAlreadyExists { .. }));

        // block fully initiliazed DB => fail
        let err = config.block_db(name.clone()).unwrap_err();
        assert!(matches!(err, Error::DatabaseAlreadyExists { .. }));

        // cleanup
        config.drain().await
    }

    #[tokio::test]
    async fn recover_db() {
        // setup
        let name = DatabaseName::new("foo").unwrap();
        let config = make_config();
        let rules = DatabaseRules::new(name.clone());

        // create DB but don't continue with rules loaded (e.g. because the rules file is broken)
        {
            let db_reservation = config.create_db(name.clone()).unwrap();
            db_reservation.commit();
        }
        assert!(config.has_uninitialized_database(&name));

        // create DB while it is uninitialized => fail
        let err = config.create_db(name.clone()).unwrap_err();
        assert!(matches!(err, Error::DatabaseAlreadyExists { .. }));

        // recover an unknown DB => fail
        let err = config
            .recover_db(DatabaseName::new("bar").unwrap())
            .unwrap_err();
        assert!(matches!(err, Error::DatabaseNotFound { .. }));

        // recover DB
        {
            let mut db_reservation = config.recover_db(name.clone()).unwrap();
            assert_eq!(db_reservation.state_code(), DatabaseStateCode::Known);
            assert_eq!(db_reservation.db_name(), name);
            assert_eq!(db_reservation.server_id(), config.server_id());
            assert!(db_reservation.rules().is_none());
            assert!(db_reservation.replay_plan().is_none());

            db_reservation.advance_rules_loaded(rules).unwrap();
            assert_eq!(db_reservation.state_code(), DatabaseStateCode::RulesLoaded);
            assert_eq!(db_reservation.db_name(), name);
            assert_eq!(db_reservation.server_id(), config.server_id());
            assert!(db_reservation.rules().is_some());
            assert!(db_reservation.replay_plan().is_none());

            let (preserved_catalog, catalog, replay_plan) = load_or_create_preserved_catalog(
                &name,
                config.object_store(),
                config.server_id(),
                config.metrics_registry(),
                false,
            )
            .await
            .unwrap();
            db_reservation
                .advance_replay(preserved_catalog, catalog, replay_plan, None)
                .unwrap();
            assert_eq!(db_reservation.state_code(), DatabaseStateCode::Replay);
            assert_eq!(db_reservation.db_name(), name);
            assert_eq!(db_reservation.server_id(), config.server_id());
            assert!(db_reservation.rules().is_some());
            assert!(db_reservation.replay_plan().is_some());

            db_reservation.advance_init().unwrap();
            assert_eq!(db_reservation.state_code(), DatabaseStateCode::Initialized);
            assert_eq!(db_reservation.db_name(), name);
            assert_eq!(db_reservation.server_id(), config.server_id());
            assert!(db_reservation.rules().is_some());
            assert!(db_reservation.replay_plan().is_none());

            db_reservation.commit();
        }
        assert!(config.db_initialized(&name).is_some());
        assert_eq!(config.db_names_sorted(), vec![name.clone()]);
        assert!(!config.has_uninitialized_database(&name));

        // recover DB a second time => fail
        let err = config.recover_db(name.clone()).unwrap_err();
        assert!(matches!(err, Error::DatabaseAlreadyExists { .. }));

        // create recovered DB => fail
        let err = config.create_db(name.clone()).unwrap_err();
        assert!(matches!(err, Error::DatabaseAlreadyExists { .. }));

        // block recovered DB => fail
        let err = config.block_db(name.clone()).unwrap_err();
        assert!(matches!(err, Error::DatabaseAlreadyExists { .. }));

        // cleanup
        config.drain().await
    }

    #[tokio::test]
    async fn block_db() {
        // setup
        let name = DatabaseName::new("foo").unwrap();
        let config = make_config();

        // block DB
        let handle = config.block_db(name.clone()).unwrap();

        // create while blocked => fail
        let err = config.create_db(name.clone()).unwrap_err();
        assert!(matches!(err, Error::DatabaseReserved { .. }));

        // recover while blocked => fail
        let err = config.recover_db(name.clone()).unwrap_err();
        assert!(matches!(err, Error::DatabaseReserved { .. }));

        // block while blocked => fail
        let err = config.block_db(name.clone()).unwrap_err();
        assert!(matches!(err, Error::DatabaseReserved { .. }));

        // unblock => DB can be created
        drop(handle);
        config.create_db(name.clone()).unwrap();

        // cleanup
        config.drain().await
    }

    #[tokio::test]
    async fn test_db_drop() {
        // setup
        let name = DatabaseName::new("foo").unwrap();
        let config = make_config();
        let rules = DatabaseRules::new(name.clone());
        let (preserved_catalog, catalog, replay_plan) = load_or_create_preserved_catalog(
            &name,
            config.object_store(),
            config.server_id(),
            config.metrics_registry(),
            false,
        )
        .await
        .unwrap();

        // create DB
        let mut db_reservation = config.create_db(name.clone()).unwrap();
        db_reservation.advance_rules_loaded(rules).unwrap();
        db_reservation
            .advance_replay(preserved_catalog, catalog, replay_plan, None)
            .unwrap();
        db_reservation.advance_init().unwrap();
        db_reservation.commit();

        // get shutdown token
        let token = match config
            .state
            .read()
            .expect("lock poisoned")
            .databases
            .get(&name)
            .unwrap()
            .as_ref()
        {
            DatabaseState::Initialized { shutdown, .. } => shutdown.clone(),
            _ => panic!("wrong state"),
        };

        // Drop config without calling drain
        std::mem::drop(config);

        // This should cancel the the background task
        assert!(token.is_cancelled());
    }

    #[test]
    fn object_store_path_for_database_config() {
        let storage = ObjectStore::new_in_memory();
        let mut base_path = storage.new_path();
        base_path.push_dir("1");

        let name = DatabaseName::new("foo").unwrap();
        let rules_path = super::object_store_path_for_database_config(&base_path, &name);

        let mut expected_path = base_path;
        expected_path.push_dir("foo");
        expected_path.set_file_name("rules.pb");

        assert_eq!(rules_path, expected_path);
    }
}
