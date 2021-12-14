use crate::lifecycle::LifecycleWorker;
use crate::write_buffer::WriteBufferConsumer;
use crate::{
    db::{
        load::{create_preserved_catalog, load_or_create_preserved_catalog},
        DatabaseToCommit,
    },
    rules::{PersistedDatabaseRules, ProvidedDatabaseRules},
    ApplicationState, Db,
};
use data_types::job::Job;
use data_types::{server_id::ServerId, DatabaseName};
use futures::{
    future::{BoxFuture, FusedFuture, Shared},
    FutureExt, TryFutureExt,
};
use generated_types::{
    database_state::DatabaseState as DatabaseStateCode, influxdata::iox::management,
};
use internal_types::freezable::Freezable;
use iox_object_store::IoxObjectStore;
use observability_deps::tracing::{error, info, warn};
use parking_lot::{MappedRwLockReadGuard, RwLock, RwLockReadGuard};
use parquet_catalog::core::{PreservedCatalog, PreservedCatalogConfig};
use persistence_windows::checkpoint::ReplayPlan;
use rand::{thread_rng, Rng};
use snafu::{ensure, OptionExt, ResultExt, Snafu};
use std::{future::Future, sync::Arc, time::Duration};
use time::Time;
use tokio::{sync::Notify, task::JoinError};
use tokio_util::sync::CancellationToken;
use tracker::{TaskTracker, TrackedFutureExt};
use uuid::Uuid;

/// Matches an error [`DatabaseState`] and clones the contained state
macro_rules! error_state {
    ($s:expr, $transition: literal, $variant:ident) => {
        match &**$s.shared.state.read() {
            DatabaseState::$variant(state, _) => state.clone(),
            state => {
                return InvalidState {
                    db_name: &$s.shared.config.read().name,
                    state: state.state_code(),
                    transition: $transition,
                }
                .fail()
            }
        }
    };
}

const INIT_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(500);

#[derive(Debug, Snafu)]
pub enum Error {
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
        source: Box<parquet_catalog::core::Error>,
    },

    #[snafu(display(
        "database ({}) in invalid state for catalog rebuild ({:?}). Expected {}",
        db_name,
        state,
        expected
    ))]
    InvalidStateForRebuild {
        db_name: String,
        expected: String,
        state: DatabaseStateCode,
    },

    #[snafu(display(
        "Internal error during rebuild. Database ({}) transitioned to unexpected state ({:?})",
        db_name,
        state,
    ))]
    UnexpectedTransitionForRebuild {
        db_name: String,
        state: DatabaseStateCode,
    },

    #[snafu(display(
        "failed to rebuild preserved catalog of database ({}): {}",
        db_name,
        source
    ))]
    RebuildPreservedCatalog {
        db_name: String,
        source: Box<parquet_catalog::rebuild::Error>,
    },

    #[snafu(display("failed to skip replay for database ({}): {}", db_name, source))]
    SkipReplay {
        db_name: String,
        source: Box<InitError>,
    },

    #[snafu(display("cannot update database rules for {} in state {}", db_name, state))]
    RulesNotUpdateable {
        db_name: String,
        state: DatabaseStateCode,
    },

    #[snafu(display("cannot persisted updated rules: {}", source))]
    CannotPersistUpdatedRules { source: crate::rules::Error },

    #[snafu(display(
        "cannot release database named {} that has already been released",
        db_name
    ))]
    CannotReleaseUnowned { db_name: String },

    #[snafu(display("cannot release database {}: {}", db_name, source))]
    CannotRelease {
        db_name: String,
        source: OwnerInfoUpdateError,
    },
}

type BackgroundWorkerFuture = Shared<BoxFuture<'static, Result<(), Arc<JoinError>>>>;

/// A `Database` represents a single configured IOx database - i.e. an
/// entity with a corresponding set of `DatabaseRules`.
///
/// `Database` composes together the various subsystems responsible for implementing
/// `DatabaseRules` and handles their startup and shutdown. This includes instance-local
/// data storage (i.e. `Db`), the write buffer, request routing, data lifecycle, etc...
///
/// TODO: Make the above accurate
#[derive(Debug)]
pub struct Database {
    /// Future that resolves when the background worker exits
    join: BackgroundWorkerFuture,

    /// The state shared with the background worker
    shared: Arc<DatabaseShared>,
}

#[derive(Debug, Clone)]
/// Information about where a database is located on object store,
/// and how to perform startup activities.
pub struct DatabaseConfig {
    pub name: DatabaseName<'static>,
    pub location: String,
    pub server_id: ServerId,
    pub wipe_catalog_on_error: bool,
    pub skip_replay: bool,
}

impl Database {
    /// Create in-mem database object.
    ///
    /// This is backed by an existing database, which was [created](Self::create) some time in the
    /// past.
    pub fn new(application: Arc<ApplicationState>, config: DatabaseConfig) -> Self {
        info!(
            db_name=%config.name,
            "new database"
        );

        let shared = Arc::new(DatabaseShared {
            config: RwLock::new(config),
            application,
            shutdown: Default::default(),
            state: RwLock::new(Freezable::new(DatabaseState::Known(DatabaseStateKnown {}))),
            state_notify: Default::default(),
        });

        let handle = tokio::spawn(background_worker(Arc::clone(&shared)));
        let join = handle.map_err(Arc::new).boxed().shared();

        Self { join, shared }
    }

    /// Create fresh database without any any state. Returns its location in object storage
    /// for saving in the server config file.
    pub async fn create(
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
            .context(CreatingOwnerInfo)?;

        let rules_to_persist = PersistedDatabaseRules::new(uuid, provided_rules);
        rules_to_persist
            .persist(&iox_object_store)
            .await
            .context(SavingRules)?;

        create_preserved_catalog(
            &db_name,
            iox_object_store,
            Arc::clone(application.metric_registry()),
            Arc::clone(application.time_provider()),
            true,
        )
        .await
        .context(CannotCreatePreservedCatalog)?;

        Ok(database_location)
    }

    /// Release this database from this server.
    pub async fn release(&self) -> Result<Uuid, Error> {
        let db_name = self.name();

        let handle = self.shared.state.read().freeze();
        let handle = handle.await;

        let (uuid, iox_object_store) = {
            let state = self.shared.state.read();
            // Can't release an already released database
            ensure!(state.is_active(), CannotReleaseUnowned { db_name });

            let uuid = state.uuid().expect("Active databases have UUIDs");
            let iox_object_store = self
                .iox_object_store()
                .expect("Active databases have iox_object_stores");

            (uuid, iox_object_store)
        };

        info!(%db_name, %uuid, "releasing database");

        update_owner_info(
            None,
            None,
            self.shared.application.time_provider().now(),
            &iox_object_store,
        )
        .await
        .context(CannotRelease { db_name })?;

        let mut state = self.shared.state.write();
        let mut state = state.unfreeze(handle);
        *state = DatabaseState::NoActiveDatabase(
            DatabaseStateKnown {},
            Arc::new(InitError::NoActiveDatabase),
        );
        self.shared.state_notify.notify_waiters();

        Ok(uuid)
    }

    /// Create an claimed database without any state. Returns its
    /// location in object storage for saving in the server config
    /// file.
    ///
    /// if `force` is true, a missing owner info or owner info that is
    /// for the wrong server id are ignored (do not cause errors)
    pub async fn claim(
        application: Arc<ApplicationState>,
        db_name: &DatabaseName<'static>,
        uuid: Uuid,
        server_id: ServerId,
        force: bool,
    ) -> Result<String, InitError> {
        info!(%db_name, %uuid, %force, "claiming database");

        let iox_object_store = IoxObjectStore::load(Arc::clone(application.object_store()), uuid)
            .await
            .context(IoxObjectStoreError)?;

        let owner_info = fetch_owner_info(&iox_object_store)
            .await
            .context(FetchingOwnerInfo);

        // try to recreate owner_info if force is specified
        let owner_info = match owner_info {
            Err(_) if force => {
                warn!("Attempting to recreate missing owner info due to force");

                let server_location =
                    IoxObjectStore::server_config_path(application.object_store(), server_id)
                        .to_string();

                create_owner_info(server_id, server_location, &iox_object_store)
                    .await
                    .context(CreatingOwnerInfo)?;

                fetch_owner_info(&iox_object_store)
                    .await
                    .context(FetchingOwnerInfo)
            }
            t => t,
        }?;

        if owner_info.id != 0 {
            if !force {
                return CantClaimDatabaseCurrentlyOwned {
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
        .context(UpdatingOwnerInfo)?;

        Ok(database_location)
    }

    /// Triggers shutdown of this `Database`
    pub fn shutdown(&self) {
        let db_name = self.name();
        info!(%db_name, "database shutting down");
        self.shared.shutdown.cancel()
    }

    /// Triggers a restart of this `Database` and wait for it to re-initialize
    pub async fn restart(&self) -> Result<(), Arc<InitError>> {
        let db_name = self.name();
        info!(%db_name, "restarting database");

        let handle = self.shared.state.read().freeze();
        let handle = handle.await;

        {
            let mut state = self.shared.state.write();
            let mut state = state.unfreeze(handle);
            *state = DatabaseState::Known(DatabaseStateKnown {});
        }
        self.shared.state_notify.notify_waiters();
        info!(%db_name, "set database state to known");

        self.wait_for_init().await
    }

    /// Waits for the background worker of this `Database` to exit
    pub fn join(&self) -> impl Future<Output = Result<(), Arc<JoinError>>> {
        self.join.clone()
    }

    /// Returns the config of this database
    pub fn config(&self) -> DatabaseConfig {
        self.shared.config.read().clone()
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

    /// Whether the database is active
    pub fn is_active(&self) -> bool {
        self.shared.state.read().is_active()
    }

    /// Returns the database rules if they're loaded
    pub fn provided_rules(&self) -> Option<Arc<ProvidedDatabaseRules>> {
        self.shared.state.read().provided_rules()
    }

    /// Returns the database UUID if it's loaded
    pub fn uuid(&self) -> Option<Uuid> {
        self.shared.state.read().uuid()
    }

    /// Returns the info about the owning server if it has been loaded
    pub fn owner_info(&self) -> Option<management::v1::OwnerInfo> {
        self.shared.state.read().owner_info()
    }

    /// Location in object store; may not actually exist yet
    pub fn location(&self) -> String {
        self.shared.config.read().location.clone()
    }

    /// Database name
    pub fn name(&self) -> DatabaseName<'static> {
        self.shared.config.read().name.clone()
    }

    /// Update the database rules, panic'ing if the state is invalid
    pub async fn update_provided_rules(
        &self,
        new_provided_rules: ProvidedDatabaseRules,
    ) -> Result<Arc<ProvidedDatabaseRules>, Error> {
        // get a handle to signal our intention to update the state
        let handle = self.shared.state.read().freeze();

        // wait for the freeze handle. Only one thread can ever have
        // it at any time so we know past this point no other thread
        // can change the DatabaseState (even though this code
        // doesn't hold a lock for the entire time)
        let handle = handle.await;

        // scope so we drop the read lock
        let (iox_object_store, uuid) = {
            let state = self.shared.state.read();
            let state_code = state.state_code();
            let db_name = new_provided_rules.db_name();

            // ensure the database is in initialized state (since we
            // hold the freeze handle, nothing could have changed this)
            let initialized = state.get_initialized().context(RulesNotUpdateable {
                db_name,
                state: state_code,
            })?;

            // A handle to the object store and a copy of the UUID so we can update the rules
            // in object store prior to obtaining exclusive write access to the `DatabaseState`
            // (which we can't hold across the await to write to the object store)
            (initialized.db.iox_object_store(), initialized.uuid)
        }; // drop read lock

        // Attempt to persist to object store, if that fails, roll
        // back the whole transaction (leave the rules unchanged).
        //
        // Even though we don't hold a lock here, the freeze handle
        // ensures the state can not be modified.
        let rules_to_persist = PersistedDatabaseRules::new(uuid, new_provided_rules);
        rules_to_persist
            .persist(&iox_object_store)
            .await
            .context(CannotPersistUpdatedRules)?;

        let mut state = self.shared.state.write();

        // Exchange FreezeHandle for mutable access to DatabaseState
        // via WriteGuard
        let mut state = state.unfreeze(handle);

        if let DatabaseState::Initialized(DatabaseStateInitialized {
            db, provided_rules, ..
        }) = &mut *state
        {
            db.update_rules(Arc::clone(rules_to_persist.rules()));
            *provided_rules = rules_to_persist.provided_rules();
            Ok(Arc::clone(provided_rules))
        } else {
            // The freeze handle should have prevented any changes to
            // the database state between when it was checked above
            // and now
            unreachable!()
        }
    }

    /// Returns the IoxObjectStore if it has been found
    pub fn iox_object_store(&self) -> Option<Arc<IoxObjectStore>> {
        self.shared.state.read().iox_object_store()
    }

    pub fn initialized(&self) -> Option<MappedRwLockReadGuard<'_, DatabaseStateInitialized>> {
        RwLockReadGuard::try_map(self.shared.state.read(), |state| state.get_initialized()).ok()
    }

    /// Gets access to an initialized `Db`
    pub fn initialized_db(&self) -> Option<Arc<Db>> {
        let initialized = self.initialized()?;
        Some(Arc::clone(initialized.db()))
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
                | DatabaseState::DatabaseObjectStoreFound(_)
                | DatabaseState::OwnerInfoLoaded(_)
                | DatabaseState::RulesLoaded(_)
                | DatabaseState::CatalogLoaded(_) => {} // Non-terminal state
                DatabaseState::Initialized(_) => return Ok(()),
                DatabaseState::DatabaseObjectStoreLookupError(_, e)
                | DatabaseState::NoActiveDatabase(_, e)
                | DatabaseState::OwnerInfoLoadError(_, e)
                | DatabaseState::RulesLoadError(_, e)
                | DatabaseState::CatalogLoadError(_, e)
                | DatabaseState::WriteBufferCreationError(_, e)
                | DatabaseState::ReplayError(_, e) => return Err(Arc::clone(e)),
            }

            notify.await;
        }
    }

    /// Recover from a CatalogLoadError by wiping the catalog
    pub async fn wipe_preserved_catalog(&self) -> Result<TaskTracker<Job>, Error> {
        let handle = self.shared.state.read().freeze();
        let handle = handle.await;

        let db_name = self.name();
        let current_state = error_state!(self, "WipePreservedCatalog", CatalogLoadError);

        let registry = self.shared.application.job_registry();
        let (tracker, registration) = registry.register(Job::WipePreservedCatalog {
            db_name: Arc::from(db_name.as_str()),
        });

        let shared = Arc::clone(&self.shared);

        tokio::spawn(
            async move {
                PreservedCatalog::wipe(&current_state.iox_object_store)
                    .await
                    .map_err(Box::new)
                    .context(WipePreservedCatalog { db_name })?;

                {
                    let mut state = shared.state.write();
                    *state.unfreeze(handle) = DatabaseState::RulesLoaded(current_state);
                }

                Ok::<_, Error>(())
            }
            .track(registration),
        );

        Ok(tracker)
    }

    /// Rebuilding the catalog from parquet files. This can be used to
    /// recover from a CatalogLoadError, or if new parquet files are
    /// added to the data directory
    pub async fn rebuild_preserved_catalog(&self, force: bool) -> Result<TaskTracker<Job>, Error> {
        let handle = self.shared.state.read().freeze();
        let handle = handle.await;

        let db_name = self.name();

        {
            // If the force flag is not specified, can only rebuild
            // the catalog if it is in ended up in an error loading
            let state = self.shared.state.read();
            if !force && !matches!(&**state, DatabaseState::CatalogLoadError { .. }) {
                return InvalidStateForRebuild {
                    db_name,
                    state: state.state_code(),
                    expected: "(CatalogLoadError)",
                }
                .fail();
            }
        }

        let registry = self.shared.application.job_registry();
        let (tracker, registration) = registry.register(Job::RebuildPreservedCatalog {
            db_name: Arc::from(db_name.as_str()),
        });

        let shared = Arc::clone(&self.shared);
        let iox_object_store = self.iox_object_store().context(InvalidStateForRebuild {
            db_name: &db_name,
            state: shared.state.read().state_code(),
            expected: "Object store initialized",
        })?;

        tokio::spawn(
            async move {
                // shutdown / stop the DB if it is running so it can't
                // be read / written to, while also preventing
                // anything else from driving the state machine
                // forward.
                info!(%db_name, "rebuilding catalog, resetting database state");
                {
                    let mut state = shared.state.write();
                    // Dropping the state here also terminates the
                    // LifeCycleWorker and WriteBufferConsumer so all
                    // background work should have completed.
                    *state.unfreeze(handle) = DatabaseState::Known(DatabaseStateKnown {});
                    // tell existing db background tasks, if any, to start shutting down
                    shared.state_notify.notify_waiters();
                };

                // get another freeze handle to prevent anything else
                // from messing with the state while we rebuild the
                // catalog (is there a better way??)
                let handle = shared.state.read().freeze();
                let _handle = handle.await;

                // check that during lock gap the state has not changed
                {
                    let state = shared.state.read();
                    ensure!(
                        matches!(&**state, DatabaseState::Known(_)),
                        UnexpectedTransitionForRebuild {
                            db_name: &db_name,
                            state: state.state_code()
                        }
                    );
                }

                info!(%db_name, "rebuilding catalog from parquet files");

                // Now wipe the catalog and rebuild it from parquet files
                PreservedCatalog::wipe(iox_object_store.as_ref())
                    .await
                    .map_err(Box::new)
                    .context(WipePreservedCatalog { db_name: &db_name })?;

                let config = PreservedCatalogConfig::new(
                    Arc::clone(&iox_object_store),
                    db_name.to_string(),
                    Arc::clone(shared.application.time_provider()),
                );
                parquet_catalog::rebuild::rebuild_catalog(config, false)
                    .await
                    .map_err(Box::new)
                    .context(RebuildPreservedCatalog { db_name: &db_name })?;

                // Double check the state hasn't changed (we hold the
                // freeze handle to make sure it does not)
                assert!(matches!(&**shared.state.read(), DatabaseState::Known(_)));

                info!(%db_name, "catalog rebuilt successfully");

                Ok::<_, Error>(())
            }
            .track(registration),
        );

        Ok(tracker)
    }

    /// Recover from a ReplayError by skipping replay
    pub async fn skip_replay(&self) -> Result<(), Error> {
        self.shared.config.write().skip_replay = true;

        match &**self.shared.state.read() {
            DatabaseState::ReplayError(_, _) | DatabaseState::RulesLoaded(_) => {}
            state => {
                return InvalidState {
                    db_name: &self.shared.config.read().name,
                    state: state.state_code(),
                    transition: "SkipReplay",
                }
                .fail()
            }
        }

        // wait for DB to leave a potential `ReplayError` state
        loop {
            // Register interest before checking to avoid race
            let notify = self.shared.state_notify.notified();

            match &**self.shared.state.read() {
                DatabaseState::ReplayError(_, _) | DatabaseState::RulesLoaded(_) => {}
                _ => break,
            }

            notify.await;
        }

        Ok(())
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        let db_name = self.name();
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
    config: RwLock<DatabaseConfig>,

    /// A token that is used to trigger shutdown of the background worker
    shutdown: CancellationToken,

    /// Application-global state
    application: Arc<ApplicationState>,

    /// The state of the `Database`, wrapped in a `Freezable` to
    /// ensure there is only one task with an outstanding intent to
    /// write at any time.
    state: RwLock<Freezable<DatabaseState>>,

    /// Notify that the database state has changed
    state_notify: Notify,
}

/// The background worker for `Database` - there should only ever be one
async fn background_worker(shared: Arc<DatabaseShared>) {
    let db_name = shared.config.read().name.clone();
    info!(%db_name, "started database background worker");

    // The background loop runs until `Database::shutdown` is called
    while !shared.shutdown.is_cancelled() {
        initialize_database(shared.as_ref()).await;

        if shared.shutdown.is_cancelled() {
            // TODO: Shutdown intermediate workers (#2813)
            info!(%db_name, "database shutdown before finishing initialization");
            break;
        }

        let DatabaseStateInitialized {
            db,
            write_buffer_consumer,
            lifecycle_worker,
            ..
        } = shared
            .state
            .read()
            .get_initialized()
            .expect("expected initialized")
            .clone();

        info!(%db_name, "database finished initialization - starting Db worker");

        crate::utils::panic_test(|| Some(format!("database background worker: {}", db_name,)));

        let db_shutdown = CancellationToken::new();
        let db_worker = db.background_worker(db_shutdown.clone()).fuse();
        futures::pin_mut!(db_worker);

        // Future that completes if the WriteBufferConsumer exits
        let consumer_join = match &write_buffer_consumer {
            Some(consumer) => futures::future::Either::Left(consumer.join()),
            None => futures::future::Either::Right(futures::future::pending()),
        }
        .fuse();
        futures::pin_mut!(consumer_join);

        // Future that completes if the LifecycleWorker exits
        let lifecycle_join = lifecycle_worker.join().fuse();
        futures::pin_mut!(lifecycle_join);

        // This inner loop runs until either:
        //
        // - Something calls `Database::shutdown`
        // - The Database transitions away from `DatabaseState::Initialized`
        //
        // In the later case it will restart the initialization procedure
        while !shared.shutdown.is_cancelled() {
            // Get notify before check to avoid race
            let notify = shared.state_notify.notified().fuse();
            futures::pin_mut!(notify);

            if shared.state.read().get_initialized().is_none() {
                info!(%db_name, "database no longer initialized");
                break;
            }

            let shutdown = shared.shutdown.cancelled().fuse();
            futures::pin_mut!(shutdown);

            // We must use `futures::select` as opposed to the often more ergonomic `tokio::select`
            // Because of the need to "re-use" the background worker future
            // TODO: Make Db own its own background loop (or remove it)
            futures::select! {
                _ = shutdown => info!("database shutting down"),
                _ = notify => info!("notified of state change"),
                _ = consumer_join => {
                    error!(%db_name, "unexpected shutdown of write buffer consumer - bailing out");
                    shared.shutdown.cancel();
                }
                _ = lifecycle_join => {
                    error!(%db_name, "unexpected shutdown of lifecycle worker - bailing out");
                    shared.shutdown.cancel();
                }
                _ = db_worker => {
                    error!(%db_name, "unexpected shutdown of db - bailing out");
                    shared.shutdown.cancel();
                }
            }
        }

        if let Some(consumer) = write_buffer_consumer {
            info!(%db_name, "shutting down write buffer consumer");
            consumer.shutdown();
            if let Err(e) = consumer.join().await {
                error!(%db_name, %e, "error shutting down write buffer consumer")
            }
        }

        if !lifecycle_join.is_terminated() {
            info!(%db_name, "shutting down lifecycle worker");
            lifecycle_worker.shutdown();
            if let Err(e) = lifecycle_worker.join().await {
                error!(%db_name, %e, "error shutting down lifecycle worker")
            }
        }

        if !db_worker.is_terminated() {
            info!(%db_name, "waiting for db worker shutdown");
            db_shutdown.cancel();
            db_worker.await
        }
    }

    info!(%db_name, "draining tasks");

    // Loop in case tasks are spawned during shutdown
    loop {
        use futures::stream::{FuturesUnordered, StreamExt};

        // We get a list of jobs from the global registry and filter them for this database
        let jobs = shared.application.job_registry().running();
        let mut futures: FuturesUnordered<_> = jobs
            .iter()
            .filter_map(|tracker| {
                let db_name2 = tracker.metadata().db_name()?;
                if db_name2.as_ref() != db_name.as_str() {
                    return None;
                }
                Some(tracker.join())
            })
            .collect();

        if futures.is_empty() {
            break;
        }

        info!(%db_name, count=futures.len(), "waiting for jobs");

        while futures.next().await.is_some() {}
    }

    info!(%db_name, "database worker finished");
}

enum TransactionOrWait {
    Transaction(DatabaseState, internal_types::freezable::FreezeHandle),
    Wait(Duration),
}

/// Try to drive the database to `DatabaseState::Initialized` returns when
/// this is achieved or the shutdown signal is triggered
async fn initialize_database(shared: &DatabaseShared) {
    let db_name = shared.config.read().name.clone();
    info!(%db_name, "database initialization started");

    // error throttle
    let mut throttled_error = false;
    let mut sleep = INIT_BACKOFF;

    while !shared.shutdown.is_cancelled() {
        // Acquire locks and determine if work to be done
        let maybe_transaction = {
            let state = shared.state.read();

            match &**state {
                // Already initialized
                DatabaseState::Initialized(_) => break,

                // No active database found, was probably deleted
                DatabaseState::NoActiveDatabase(_, _) => {
                    info!(%db_name, "no active database found");

                    // no exponential / jitter sleep
                    TransactionOrWait::Wait(INIT_BACKOFF)
                }

                // Can perform work
                _ if state.error().is_none() || (state.error().is_some() && throttled_error) => {
                    match state.try_freeze() {
                        Some(handle) => {
                            TransactionOrWait::Transaction(DatabaseState::clone(&state), handle)
                        }
                        None => {
                            // Backoff if there is already an in-progress initialization action (e.g. recovery)
                            info!(%db_name, %state, "init transaction already in progress");

                            // no exponential / jitter sleep
                            TransactionOrWait::Wait(INIT_BACKOFF)
                        }
                    }
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
                    sleep = Duration::from_secs_f64(MAX_BACKOFF.as_secs_f64().min(
                        rng.gen_range(INIT_BACKOFF.as_secs_f64()..(sleep.as_secs_f64() * 3.0)),
                    ));
                    TransactionOrWait::Wait(sleep)
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
            | DatabaseState::DatabaseObjectStoreLookupError(state, _) => {
                match state.advance(shared).await {
                    Ok(state) => DatabaseState::DatabaseObjectStoreFound(state),
                    Err(InitError::NoActiveDatabase) => DatabaseState::NoActiveDatabase(
                        state,
                        Arc::new(InitError::NoActiveDatabase),
                    ),
                    Err(e) => DatabaseState::DatabaseObjectStoreLookupError(state, Arc::new(e)),
                }
            }
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
            DatabaseState::ReplayError(state, _) => DatabaseState::RulesLoaded(state.rollback()),
            DatabaseState::Initialized(_) | DatabaseState::NoActiveDatabase(_, _) => {
                unreachable!("{:?}", state)
            }
        };

        if next_state.error().is_some() {
            // this is a new error that needs to be throttled
            throttled_error = false;
        } else {
            // reset backoff
            sleep = INIT_BACKOFF;
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
    CatalogLoad { source: crate::db::load::Error },

    #[snafu(display("error creating write buffer: {}", source))]
    CreateWriteBuffer {
        source: write_buffer::core::WriteBufferError,
    },

    #[snafu(display("error during replay: {}", source))]
    Replay { source: crate::db::Error },

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
    CannotCreatePreservedCatalog { source: crate::db::load::Error },
}

/// The Database startup state machine
///
/// ```text
///                      (end)
///                        ^
///                        |
///                 [NoActiveDatabase]
///                        ^
///                        |
///                        |
/// (start)------------>[Known]------------->[DatabaseObjectStoreLookupError]
///                        |                           |
///                        +---------------------------o
///                        |
///                        |
///                        V
///             [DatabaseObjectStoreFound]------>[OwnerInfoLoadError]
///                        |                           |
///                        +---------------------------o
///                        |
///                        |
///                        V
///                 [OwnerInfoLoaded]----------->[RulesLoadError]
///                        |                           |
///                        +---------------------------o
///                        |
///                        |
///                        V
///       o--------->[RulesLoaded]-------------->[CatalogLoadError]
///       |                |                           |
///       |                +---------------------------o
///       |                |
///       |                |
///       |                V
/// [ReplayError]<--[CatalogLoaded]---------->[WriteBufferCreationError]
///                        |                           |
///                        +---------------------------o
///                        |
///                        |
///                        V
///                  [Initialized]
///                        |
///                        V
///                      (end)
/// ```
///
/// A Database starts in [`DatabaseState::Known`] and advances through the
/// non error states in sequential order until either:
///
/// 1. It reaches [`DatabaseState::Initialized`]: Database is initialized
/// 2. It reaches [`DatabaseState::NoActiveDatabase`]: We cannot setup this database because we are not aware of any
///    active claim with this name.
/// 3. An error is encountered, in which case it transitions to one of
///    the error states. We try to recover from all of them. For all except [`DatabaseState::ReplayError`] this is a
///    rather cheap operation since we can just retry the actual operation. For [`DatabaseState::ReplayError`] we need
///    to dump the potentially half-modified in-memory catalog before retrying.
#[derive(Debug, Clone)]
enum DatabaseState {
    // Basic initialization sequence states:
    Known(DatabaseStateKnown),
    DatabaseObjectStoreFound(DatabaseStateDatabaseObjectStoreFound),
    OwnerInfoLoaded(DatabaseStateOwnerInfoLoaded),
    RulesLoaded(DatabaseStateRulesLoaded),
    CatalogLoaded(DatabaseStateCatalogLoaded),

    // Terminal state (success)
    Initialized(DatabaseStateInitialized),

    // Terminal state (failure)
    NoActiveDatabase(DatabaseStateKnown, Arc<InitError>),

    // Error states, we'll try to recover from them
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
    fn state_code(&self) -> DatabaseStateCode {
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

    fn error(&self) -> Option<&Arc<InitError>> {
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

    fn provided_rules(&self) -> Option<Arc<ProvidedDatabaseRules>> {
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

    fn uuid(&self) -> Option<Uuid> {
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

    fn owner_info(&self) -> Option<management::v1::OwnerInfo> {
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

    fn iox_object_store(&self) -> Option<Arc<IoxObjectStore>> {
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
    fn is_active(&self) -> bool {
        !matches!(self, DatabaseState::NoActiveDatabase(_, _))
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
        .context(DatabaseObjectStoreLookup)?;

        Ok(DatabaseStateDatabaseObjectStoreFound {
            iox_object_store: Arc::new(iox_object_store),
        })
    }
}

#[derive(Debug, Clone)]
struct DatabaseStateDatabaseObjectStoreFound {
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
            .context(FetchingOwnerInfo)?;

        let server_id = shared.config.read().server_id.get_u32();
        if owner_info.id != server_id {
            return DatabaseOwnerMismatch {
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

#[derive(Debug, Snafu)]
pub enum OwnerInfoFetchError {
    #[snafu(display("error loading owner info: {}", source))]
    Loading { source: object_store::Error },

    #[snafu(display("error decoding owner info: {}", source))]
    Decoding {
        source: generated_types::DecodeError,
    },
}

async fn fetch_owner_info(
    iox_object_store: &IoxObjectStore,
) -> Result<management::v1::OwnerInfo, OwnerInfoFetchError> {
    let raw_owner_info = iox_object_store.get_owner_file().await.context(Loading)?;

    generated_types::server_config::decode_database_owner_info(raw_owner_info).context(Decoding)
}

#[derive(Debug, Snafu)]
pub enum OwnerInfoCreateError {
    #[snafu(display("could not create new owner info file; it already exists"))]
    OwnerFileAlreadyExists,

    #[snafu(display("error creating database owner info file: {}", source))]
    CreatingOwnerFile { source: Box<object_store::Error> },
}

/// Create a new owner info file for this database. Existing content at this location in object
/// storage is an error.
async fn create_owner_info(
    server_id: ServerId,
    server_location: String,
    iox_object_store: &IoxObjectStore,
) -> Result<(), OwnerInfoCreateError> {
    ensure!(
        matches!(
            iox_object_store.get_owner_file().await,
            Err(object_store::Error::NotFound { .. })
        ),
        OwnerFileAlreadyExists,
    );

    let owner_info = management::v1::OwnerInfo {
        id: server_id.get_u32(),
        location: server_location,
        transactions: vec![],
    };
    let mut encoded = bytes::BytesMut::new();
    generated_types::server_config::encode_database_owner_info(&owner_info, &mut encoded)
        .expect("owner info serialization should be valid");
    let encoded = encoded.freeze();

    iox_object_store
        .put_owner_file(encoded)
        .await
        .map_err(Box::new)
        .context(CreatingOwnerFile)?;

    Ok(())
}

#[derive(Debug, Snafu)]
pub enum OwnerInfoUpdateError {
    #[snafu(display("could not fetch existing owner info: {}", source))]
    CouldNotFetch { source: OwnerInfoFetchError },

    #[snafu(display("error updating database owner info file: {}", source))]
    UpdatingOwnerFile { source: object_store::Error },
}

/// Fetch existing owner info, set the `id` and `location`, insert a new entry into the transaction
/// history, and overwrite the contents of the owner file. Errors if the owner info file does NOT
/// currently exist.
async fn update_owner_info(
    new_server_id: Option<ServerId>,
    new_server_location: Option<String>,
    timestamp: Time,
    iox_object_store: &IoxObjectStore,
) -> Result<(), OwnerInfoUpdateError> {
    let management::v1::OwnerInfo {
        id,
        location,
        mut transactions,
    } = fetch_owner_info(iox_object_store)
        .await
        .context(CouldNotFetch)?;

    let new_transaction = management::v1::OwnershipTransaction {
        id,
        location,
        timestamp: Some(timestamp.date_time().into()),
    };
    transactions.push(new_transaction);

    // TODO: only save latest 100 transactions

    let new_owner_info = management::v1::OwnerInfo {
        // 0 is not a valid server ID, so it indicates "unowned".
        id: new_server_id.map(|s| s.get_u32()).unwrap_or_default(),
        // Owner location is empty when the database is unowned.
        location: new_server_location.unwrap_or_default(),
        transactions,
    };

    let mut encoded = bytes::BytesMut::new();
    generated_types::server_config::encode_database_owner_info(&new_owner_info, &mut encoded)
        .expect("owner info serialization should be valid");
    let encoded = encoded.freeze();

    iox_object_store
        .put_owner_file(encoded)
        .await
        .context(UpdatingOwnerFile)?;
    Ok(())
}

#[derive(Debug, Clone)]
struct DatabaseStateOwnerInfoLoaded {
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
            .context(LoadingRules)?;

        let db_name = shared.config.read().name.clone();
        if rules.db_name() != &db_name {
            return RulesDatabaseNameMismatch {
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
struct DatabaseStateRulesLoaded {
    provided_rules: Arc<ProvidedDatabaseRules>,
    uuid: Uuid,
    owner_info: management::v1::OwnerInfo,
    iox_object_store: Arc<IoxObjectStore>,
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
            Arc::clone(&self.iox_object_store),
            Arc::clone(shared.application.metric_registry()),
            Arc::clone(shared.application.time_provider()),
            wipe_catalog_on_error,
            skip_replay,
        )
        .await
        .context(CatalogLoad)?;

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
struct DatabaseStateCatalogLoaded {
    db: Arc<Db>,
    lifecycle_worker: Arc<LifecycleWorker>,
    replay_plan: Arc<Option<ReplayPlan>>,
    provided_rules: Arc<ProvidedDatabaseRules>,
    uuid: Uuid,
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
                    .context(CreateWriteBuffer)?;

                let replay_plan = if skip_replay {
                    None
                } else {
                    self.replay_plan.as_ref().as_ref()
                };

                db.perform_replay(replay_plan, consumer.as_mut())
                    .await
                    .context(Replay)?;

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
    fn rollback(&self) -> DatabaseStateRulesLoaded {
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
pub struct DatabaseStateInitialized {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::make_application;
    use data_types::{
        database_rules::{PartitionTemplate, TemplatePart},
        sequence::Sequence,
        write_buffer::WriteBufferConnection,
    };
    use std::{num::NonZeroU32, time::Instant};
    use uuid::Uuid;
    use write_buffer::mock::MockBufferSharedState;

    #[tokio::test]
    async fn database_shutdown_waits_for_jobs() {
        let application = make_application();

        let database = Database::new(
            Arc::clone(&application),
            DatabaseConfig {
                name: DatabaseName::new("test").unwrap(),
                location: String::from("arbitrary"),
                server_id: ServerId::new(NonZeroU32::new(23).unwrap()),
                wipe_catalog_on_error: false,
                skip_replay: false,
            },
        );

        // Should have failed to load (this isn't important to the test)
        let err = database.wait_for_init().await.unwrap_err();
        assert!(
            matches!(err.as_ref(), InitError::DatabaseObjectStoreLookup { .. }),
            "got {:?}",
            err
        );

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

    async fn initialized_database() -> (Arc<ApplicationState>, Database) {
        let server_id = ServerId::try_from(1).unwrap();
        let application = make_application();

        let db_name = DatabaseName::new("test").unwrap();
        let uuid = Uuid::new_v4();
        let provided_rules = ProvidedDatabaseRules::new_empty(db_name.clone());

        let location = Database::create(Arc::clone(&application), uuid, provided_rules, server_id)
            .await
            .unwrap();

        let db_config = DatabaseConfig {
            name: db_name,
            location,
            server_id,
            wipe_catalog_on_error: false,
            skip_replay: false,
        };
        let database = Database::new(Arc::clone(&application), db_config.clone());
        database.wait_for_init().await.unwrap();
        (application, database)
    }

    #[tokio::test]
    async fn database_reinitialize() {
        let (_, database) = initialized_database().await;

        tokio::time::timeout(Duration::from_millis(1), database.join())
            .await
            .unwrap_err();

        database.shared.state_notify.notify_waiters();

        // Database should still be running
        tokio::time::timeout(Duration::from_millis(1), database.join())
            .await
            .unwrap_err();

        {
            let mut state = database.shared.state.write();
            let mut state = state.get_mut().unwrap();
            *state = DatabaseState::Known(DatabaseStateKnown {});
            database.shared.state_notify.notify_waiters();
        }

        // Database should still be running
        tokio::time::timeout(Duration::from_millis(1), database.join())
            .await
            .unwrap_err();

        // Database should re-initialize correctly
        tokio::time::timeout(Duration::from_millis(1), database.wait_for_init())
            .await
            .unwrap()
            .unwrap();

        database.shutdown();
        // Database should shutdown
        tokio::time::timeout(Duration::from_millis(1), database.join())
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn database_release() {
        let (application, database) = initialized_database().await;
        let server_id = database.shared.config.read().server_id;
        let server_location =
            IoxObjectStore::server_config_path(application.object_store(), server_id).to_string();
        let iox_object_store = database.iox_object_store().unwrap();

        database.release().await.unwrap();

        assert_eq!(database.state_code(), DatabaseStateCode::NoActiveDatabase);
        assert!(matches!(
            database.init_error().unwrap().as_ref(),
            InitError::NoActiveDatabase
        ));

        let owner_info = fetch_owner_info(&iox_object_store).await.unwrap();
        assert_eq!(owner_info.id, 0);
        assert_eq!(owner_info.location, "");
        assert_eq!(owner_info.transactions.len(), 1);

        let transaction = &owner_info.transactions[0];
        assert_eq!(transaction.id, server_id.get_u32());
        assert_eq!(transaction.location, server_location);
    }

    #[tokio::test]
    async fn database_claim() {
        let (application, database) = initialized_database().await;
        let db_name = &database.shared.config.read().name.clone();
        let server_id = database.shared.config.read().server_id;
        let server_location =
            IoxObjectStore::server_config_path(application.object_store(), server_id).to_string();
        let iox_object_store = database.iox_object_store().unwrap();
        let new_server_id = ServerId::try_from(2).unwrap();
        let new_server_location =
            IoxObjectStore::server_config_path(application.object_store(), new_server_id)
                .to_string();
        let uuid = database.release().await.unwrap();

        Database::claim(application, db_name, uuid, new_server_id, false)
            .await
            .unwrap();

        assert_eq!(database.state_code(), DatabaseStateCode::NoActiveDatabase);
        assert!(matches!(
            database.init_error().unwrap().as_ref(),
            InitError::NoActiveDatabase
        ));

        let owner_info = fetch_owner_info(&iox_object_store).await.unwrap();
        assert_eq!(owner_info.id, new_server_id.get_u32());
        assert_eq!(owner_info.location, new_server_location);
        assert_eq!(owner_info.transactions.len(), 2);

        let release_transaction = &owner_info.transactions[0];
        assert_eq!(release_transaction.id, server_id.get_u32());
        assert_eq!(release_transaction.location, server_location);

        let claim_transaction = &owner_info.transactions[1];
        assert_eq!(claim_transaction.id, 0);
        assert_eq!(claim_transaction.location, "");
    }

    #[tokio::test]
    async fn database_restart() {
        test_helpers::maybe_start_logging();
        let (_application, database) = initialized_database().await;

        // Restart successful
        database.restart().await.unwrap();

        // Delete the rules
        let iox_object_store = database.iox_object_store().unwrap();
        iox_object_store.delete_database_rules_file().await.unwrap();

        // Restart should fail
        let err = database.restart().await.unwrap_err();
        assert!(
            matches!(err.as_ref(), InitError::DatabaseObjectStoreLookup { .. }),
            "{:?}",
            err
        );
    }

    #[tokio::test]
    async fn skip_replay() {
        // create write buffer
        let state =
            MockBufferSharedState::empty_with_n_sequencers(NonZeroU32::try_from(1).unwrap());
        let partition_template = PartitionTemplate {
            parts: vec![TemplatePart::Column("partition_by".to_string())],
        };
        state.push_lp(Sequence::new(0, 10), "table_1,partition_by=a foo=1 10");
        state.push_lp(Sequence::new(0, 11), "table_1,partition_by=b foo=2 20");

        // setup application
        let application = make_application();
        application
            .write_buffer_factory()
            .register_mock("my_mock".to_string(), state.clone());

        let server_id = ServerId::try_from(1).unwrap();

        // setup DB
        let db_name = DatabaseName::new("test_db").unwrap();
        let uuid = Uuid::new_v4();
        let rules = data_types::database_rules::DatabaseRules {
            name: db_name.clone(),
            partition_template: partition_template.clone(),
            lifecycle_rules: data_types::database_rules::LifecycleRules {
                late_arrive_window_seconds: NonZeroU32::try_from(1).unwrap(),
                ..Default::default()
            },
            worker_cleanup_avg_sleep: Duration::from_secs(2),
            write_buffer_connection: Some(WriteBufferConnection {
                type_: "mock".to_string(),
                connection: "my_mock".to_string(),
                ..Default::default()
            }),
        };
        let location = Database::create(
            Arc::clone(&application),
            uuid,
            make_provided_rules(rules),
            server_id,
        )
        .await
        .unwrap();
        let db_config = DatabaseConfig {
            name: db_name,
            location,
            server_id,
            wipe_catalog_on_error: false,
            skip_replay: false,
        };
        let database = Database::new(Arc::clone(&application), db_config.clone());
        database.wait_for_init().await.unwrap();

        // wait for ingest
        let db = database.initialized_db().unwrap();
        let t_0 = Instant::now();
        loop {
            // use later partition here so that we can implicitly wait for both entries
            if db.partition_summary("table_1", "partition_by_b").is_some() {
                break;
            }

            assert!(t_0.elapsed() < Duration::from_secs(10));
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // partition a was forgotten, partition b is still persisted
        assert!(db.partition_summary("table_1", "partition_by_a").is_some());

        // persist one partition
        db.persist_partition("table_1", "partition_by_b", true)
            .await
            .unwrap();

        // shutdown first database
        database.shutdown();
        database.join().await.unwrap();

        // break write buffer by removing entries
        state.clear_messages(0);
        state.push_lp(Sequence::new(0, 12), "table_1,partition_by=c foo=3 30");

        // boot actual test database
        let database = Database::new(Arc::clone(&application), db_config.clone());

        // db is broken
        let err = database.wait_for_init().await.unwrap_err();
        assert!(matches!(err.as_ref(), InitError::Replay { .. }));

        // skip replay
        database.skip_replay().await.unwrap();
        database.wait_for_init().await.unwrap();

        // wait for ingest
        state.push_lp(Sequence::new(0, 13), "table_1,partition_by=d foo=4 40");
        let db = database.initialized_db().unwrap();
        let t_0 = Instant::now();
        loop {
            if db.partition_summary("table_1", "partition_by_d").is_some() {
                break;
            }

            assert!(t_0.elapsed() < Duration::from_secs(10));
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // partition a was forgotten, partition b is still persisted, partition c was skipped
        assert!(db.partition_summary("table_1", "partition_by_a").is_none());
        assert!(db.partition_summary("table_1", "partition_by_b").is_some());
        assert!(db.partition_summary("table_1", "partition_by_c").is_none());

        // cannot skip when database is initialized
        let res = database.skip_replay().await;
        assert!(matches!(res, Err(Error::InvalidState { .. })));

        // clean up
        database.shutdown();
        database.join().await.unwrap();
    }

    #[tokio::test]
    async fn write_buffer_creation_error() {
        // ensure that we're retrying write buffer creation (e.g. after connection errors or cluster issues)

        // setup application
        let application = make_application();
        application.write_buffer_factory();

        let server_id = ServerId::try_from(1).unwrap();

        // setup DB
        let db_name = DatabaseName::new("test_db").unwrap();
        let uuid = Uuid::new_v4();
        let rules = data_types::database_rules::DatabaseRules {
            name: db_name.clone(),
            lifecycle_rules: Default::default(),
            partition_template: Default::default(),
            worker_cleanup_avg_sleep: Duration::from_secs(2),
            write_buffer_connection: Some(WriteBufferConnection {
                type_: "mock".to_string(),
                connection: "my_mock".to_string(),
                ..Default::default()
            }),
        };
        let location = Database::create(
            Arc::clone(&application),
            uuid,
            make_provided_rules(rules),
            server_id,
        )
        .await
        .unwrap();
        let db_config = DatabaseConfig {
            name: db_name,
            location,
            server_id,
            wipe_catalog_on_error: false,
            skip_replay: false,
        };
        let database = Database::new(Arc::clone(&application), db_config.clone());

        // wait for a bit so the database fails because the mock is missing
        database.wait_for_init().await.unwrap_err();

        // create write buffer
        let state =
            MockBufferSharedState::empty_with_n_sequencers(NonZeroU32::try_from(1).unwrap());
        application
            .write_buffer_factory()
            .register_mock("my_mock".to_string(), state.clone());

        tokio::time::timeout(Duration::from_secs(10), async move {
            loop {
                if database.wait_for_init().await.is_ok() {
                    return;
                }

                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .unwrap();
    }

    /// Normally database rules are provided as grpc messages, but in
    /// tests they are constructed from database rules structures
    /// themselves.
    fn make_provided_rules(
        rules: data_types::database_rules::DatabaseRules,
    ) -> ProvidedDatabaseRules {
        let rules: generated_types::influxdata::iox::management::v1::DatabaseRules = rules
            .try_into()
            .expect("tests should construct valid DatabaseRules");
        ProvidedDatabaseRules::new_rules(rules)
            .expect("tests should construct valid ProvidedDatabaseRules")
    }
}
