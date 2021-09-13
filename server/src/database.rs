use crate::write_buffer::WriteBufferConsumer;
use crate::{
    db::{
        load::{create_preserved_catalog, load_or_create_preserved_catalog},
        DatabaseToCommit,
    },
    rules::ProvidedDatabaseRules,
    ApplicationState, Db,
};
use chrono::{DateTime, Utc};
use data_types::{database_rules::WriteBufferDirection, server_id::ServerId, DatabaseName};
use futures::{
    future::{BoxFuture, FusedFuture, Shared},
    FutureExt, TryFutureExt,
};
use generated_types::database_state::DatabaseState as DatabaseStateCode;
use internal_types::freezable::Freezable;
use iox_object_store::IoxObjectStore;
use observability_deps::tracing::{error, info, warn};
use parking_lot::{MappedRwLockReadGuard, RwLock, RwLockReadGuard};
use parquet_file::catalog::api::PreservedCatalog;
use persistence_windows::checkpoint::ReplayPlan;
use snafu::{ensure, OptionExt, ResultExt, Snafu};
use std::{future::Future, sync::Arc, time::Duration};
use tokio::{sync::Notify, task::JoinError};
use tokio_util::sync::CancellationToken;

const INIT_BACKOFF: Duration = Duration::from_secs(1);

mod metrics;

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
        source: Box<parquet_file::catalog::api::Error>,
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

    #[snafu(display("cannot mark database deleted: {}", source))]
    CannotMarkDatabaseDeleted {
        db_name: String,
        source: object_store::Error,
    },

    #[snafu(display("no active database named {} to delete", db_name))]
    NoActiveDatabaseToDelete { db_name: String },

    #[snafu(display("cannot restore database named {} that is already active", db_name))]
    CannotRestoreActiveDatabase { db_name: String },
}

#[derive(Debug, Snafu)]
pub enum WriteError {
    #[snafu(context(false))]
    DbError { source: super::db::Error },

    #[snafu(display("write buffer producer error: {}", source))]
    WriteBuffer {
        source: Box<dyn std::error::Error + Sync + Send>,
    },

    #[snafu(display("writing only allowed through write buffer"))]
    WritingOnlyAllowedThroughWriteBuffer,

    #[snafu(display("database not initialized: {}", state))]
    NotInitialized { state: DatabaseStateCode },

    #[snafu(display("Hard buffer size limit reached"))]
    HardLimitReached {},
}

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
    join: Shared<BoxFuture<'static, Result<(), Arc<JoinError>>>>,

    /// The state shared with the background worker
    shared: Arc<DatabaseShared>,
}

#[derive(Debug, Clone)]
/// Information about where a database is located on object store,
/// and how to perform startup activities.
pub struct DatabaseConfig {
    pub name: DatabaseName<'static>,
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

        let metrics =
            metrics::Metrics::new(application.metric_registry().as_ref(), config.name.as_str());

        let shared = Arc::new(DatabaseShared {
            config,
            application,
            shutdown: Default::default(),
            state: RwLock::new(Freezable::new(DatabaseState::Known(DatabaseStateKnown {}))),
            state_notify: Default::default(),
            metrics,
        });

        let handle = tokio::spawn(background_worker(Arc::clone(&shared)));
        let join = handle.map_err(Arc::new).boxed().shared();

        Self { join, shared }
    }

    /// Create fresh database without any any state.
    pub async fn create(
        application: Arc<ApplicationState>,
        provided_rules: &ProvidedDatabaseRules,
        server_id: ServerId,
    ) -> Result<(), InitError> {
        let db_name = provided_rules.db_name();
        let iox_object_store = Arc::new(
            match IoxObjectStore::new(Arc::clone(application.object_store()), server_id, db_name)
                .await
            {
                Ok(ios) => ios,
                Err(iox_object_store::IoxObjectStoreError::DatabaseAlreadyExists { name }) => {
                    return Err(InitError::DatabaseAlreadyExists { name })
                }
                Err(source) => return Err(InitError::IoxObjectStoreError { source }),
            },
        );

        provided_rules
            .persist(&iox_object_store)
            .await
            .context(SavingRules)?;

        create_preserved_catalog(
            Arc::clone(&iox_object_store),
            Arc::clone(application.metric_registry()),
            true,
        )
        .await
        .context(CannotCreatePreservedCatalog)?;

        Ok(())
    }

    /// Mark this database as deleted.
    pub async fn delete(&self) -> Result<(), Error> {
        let db_name = &self.shared.config.name;
        info!(%db_name, "marking database deleted");

        let handle = {
            let state = self.shared.state.read();

            // Can't delete an already deleted database.
            ensure!(state.is_active(), NoActiveDatabaseToDelete { db_name });

            state.try_freeze().context(TransitionInProgress {
                db_name,
                state: state.state_code(),
            })?
        };

        // If there is an object store for this database, write out a tombstone file.
        // If there isn't an object store, something is wrong and we shouldn't switch the
        // state without being able to write the tombstone file.
        self.iox_object_store()
            .with_context(|| {
                let state = self.shared.state.read();
                TransitionInProgress {
                    db_name,
                    state: state.state_code(),
                }
            })?
            .write_tombstone()
            .await
            .context(CannotMarkDatabaseDeleted { db_name })?;

        let shared = Arc::clone(&self.shared);

        {
            let mut state = shared.state.write();
            *state.unfreeze(handle) = DatabaseState::NoActiveDatabase(
                DatabaseStateKnown {},
                Arc::new(InitError::NoActiveDatabase),
            );
            shared.state_notify.notify_waiters();
        }

        self.shutdown();

        Ok(())
    }

    /// Mark this database as restored.
    pub async fn restore(&self, iox_object_store: IoxObjectStore) -> Result<(), Error> {
        let db_name = &self.shared.config.name;
        info!(
            %db_name,
            object_store_path=%iox_object_store.debug_database_path(),
            "restoring database"
        );

        let handle = {
            let state = self.shared.state.read();

            // Can't restore an already active database.
            ensure!(!state.is_active(), CannotRestoreActiveDatabase { db_name });

            state.try_freeze().context(TransitionInProgress {
                db_name,
                state: state.state_code(),
            })?
        };

        let shared = Arc::clone(&self.shared);

        {
            // Reset the state to initializing with the given iox object storage
            let mut state = shared.state.write();
            *state.unfreeze(handle) =
                DatabaseState::DatabaseObjectStoreFound(DatabaseStateDatabaseObjectStoreFound {
                    iox_object_store: Arc::new(iox_object_store),
                });
            info!(%db_name, "set database state to object store found");
        }

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

    /// Whether the database is active
    pub fn is_active(&self) -> bool {
        self.shared.state.read().is_active()
    }

    /// Returns the database rules if they're loaded
    pub fn provided_rules(&self) -> Option<Arc<ProvidedDatabaseRules>> {
        self.shared.state.read().provided_rules()
    }

    /// Update the database rules, panic'ing if the state is invalid
    pub async fn update_provided_rules(
        &self,
        new_provided_rules: Arc<ProvidedDatabaseRules>,
    ) -> Result<(), Error> {
        // get a handle to signal our intention to update the state
        let handle = self.shared.state.read().freeze();

        // wait for the freeze handle. Only one thread can ever have
        // it at any time so we know past this point no other thread
        // can change the DatabaseState (even though this code
        // doesn't hold a lock for the entire time)
        let handle = handle.await;

        // scope so we drop the read lock
        let iox_object_store = {
            let state = self.shared.state.read();
            let state_code = state.state_code();

            // A handle to the object store so we can update the rules
            // in object store prior to obtaining exclusive write
            // access to the `DatabaseState` (which we can't hold
            // across the await to write to the object store)
            let iox_object_store = state.iox_object_store().context(RulesNotUpdateable {
                db_name: new_provided_rules.db_name(),
                state: state_code,
            })?;

            // ensure the database is in initialized state (since we
            // hold the freeze handle, nothing can change this
            ensure!(
                state_code == DatabaseStateCode::Initialized,
                RulesNotUpdateable {
                    db_name: new_provided_rules.db_name(),
                    state: state_code,
                }
            );
            iox_object_store
        }; // drop read lock

        // Attempt to persist to object store, if that fails, roll
        // back the whole transaction (leave the rules unchanged).
        //
        // Even though we don't hold a lock here, the freeze handle
        // ensures the state can not be modified.
        new_provided_rules
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
            db.update_rules(Arc::clone(new_provided_rules.rules()));
            *provided_rules = new_provided_rules;
            Ok(())
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
                | DatabaseState::RulesLoaded(_)
                | DatabaseState::CatalogLoaded(_) => {} // Non-terminal state
                DatabaseState::Initialized(_) => return Ok(()),
                DatabaseState::DatabaseObjectStoreLookupError(_, e)
                | DatabaseState::NoActiveDatabase(_, e)
                | DatabaseState::RulesLoadError(_, e)
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
                    return InvalidState {
                        db_name,
                        state: state.state_code(),
                        transition: "WipePreservedCatalog",
                    }
                    .fail()
                }
            };

            let handle = state.try_freeze().context(TransitionInProgress {
                db_name,
                state: state.state_code(),
            })?;

            (current_state, handle)
        };

        let shared = Arc::clone(&self.shared);

        Ok(async move {
            let db_name = &shared.config.name;

            PreservedCatalog::wipe(&current_state.iox_object_store)
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

    /// Recover from a ReplayError by skipping replay
    pub fn skip_replay(&self) -> Result<impl Future<Output = Result<(), Error>>, Error> {
        let db_name = &self.shared.config.name;
        let (mut current_state, handle) = {
            let state = self.shared.state.read();
            let current_state = match &**state {
                DatabaseState::ReplayError(rules_loaded, _) => rules_loaded.clone(),
                _ => {
                    return InvalidState {
                        db_name,
                        state: state.state_code(),
                        transition: "SkipReplay",
                    }
                    .fail()
                }
            };

            let handle = state.try_freeze().context(TransitionInProgress {
                db_name,
                state: state.state_code(),
            })?;

            (current_state, handle)
        };

        let shared = Arc::clone(&self.shared);

        Ok(async move {
            let db_name = &shared.config.name;
            current_state.replay_plan = Arc::new(None);
            let current_state = current_state
                .advance(shared.as_ref())
                .await
                .map_err(Box::new)
                .context(SkipReplay { db_name })?;

            {
                let mut state = shared.state.write();
                *state.unfreeze(handle) = DatabaseState::Initialized(current_state);
            }

            Ok(())
        })
    }

    /// Writes an entry to this `Database` this will either:
    ///
    /// - write it to a write buffer
    /// - write it to a local `Db`
    ///
    pub async fn write_entry(
        &self,
        entry: entry::Entry,
        time_of_write: DateTime<Utc>,
    ) -> Result<(), WriteError> {
        let recorder = self.shared.metrics.entry_ingest(entry.data().len());

        let db = {
            let state = self.shared.state.read();
            match &**state {
                DatabaseState::Initialized(initialized) => match &initialized.write_buffer_consumer
                {
                    Some(_) => return Err(WriteError::WritingOnlyAllowedThroughWriteBuffer),
                    None => Arc::clone(&initialized.db),
                },
                state => {
                    return Err(WriteError::NotInitialized {
                        state: state.state_code(),
                    })
                }
            }
        };

        db.store_entry(entry, time_of_write).await.map_err(|e| {
            use super::db::Error;
            match e {
                // TODO: Pull write buffer producer out of Db
                Error::WriteBufferWritingError { source } => WriteError::WriteBuffer { source },
                Error::HardLimitReached {} => WriteError::HardLimitReached {},
                e => e.into(),
            }
        })?;

        recorder.success();
        Ok(())
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

    /// The state of the `Database`, wrapped in a `Freezable` to
    /// ensure there is only one task with an outstanding intent to
    /// write at any time.
    state: RwLock<Freezable<DatabaseState>>,

    /// Notify that the database state has changed
    state_notify: Notify,

    /// Metrics for this database
    metrics: metrics::Metrics,
}

/// The background worker for `Database` - there should only ever be one
async fn background_worker(shared: Arc<DatabaseShared>) {
    info!(db_name=%shared.config.name, "started database background worker");

    // The background loop runs until `Database::shutdown` is called
    while !shared.shutdown.is_cancelled() {
        initialize_database(shared.as_ref()).await;

        if shared.shutdown.is_cancelled() {
            info!(db_name=%shared.config.name, "database shutdown before finishing initialization");
            break;
        }

        let DatabaseStateInitialized {
            db,
            write_buffer_consumer,
            ..
        } = shared
            .state
            .read()
            .get_initialized()
            .expect("expected initialized")
            .clone();

        info!(db_name=%shared.config.name, "database finished initialization - starting Db worker");

        crate::utils::panic_test(|| {
            Some(format!(
                "database background worker: {}",
                shared.config.name
            ))
        });

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
                info!(db_name=%shared.config.name, "database no longer initialized");
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
                    error!(db_name=%shared.config.name, "unexpected shutdown of write buffer consumer - bailing out");
                    shared.shutdown.cancel();
                }
                _ = db_worker => {
                    error!(db_name=%shared.config.name, "unexpected shutdown of db - bailing out");
                    shared.shutdown.cancel();
                }
            }
        }

        if let Some(consumer) = write_buffer_consumer {
            info!(db_name=%shared.config.name, "shutting down write buffer consumer");
            consumer.shutdown();
            if let Err(e) = consumer.join().await {
                error!(db_name=%shared.config.name, %e, "error shutting down write buffer consumer")
            }
        }

        if !db_worker.is_terminated() {
            info!(db_name=%shared.config.name, "waiting for db worker shutdown");
            db_shutdown.cancel();
            db_worker.await
        }
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
                | DatabaseState::DatabaseObjectStoreFound(_)
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
                // No active database found, was probably deleted
                DatabaseState::NoActiveDatabase(_, _) => {
                    info!(%db_name, "no active database found");
                    None
                }
                // Operator intervention required
                DatabaseState::DatabaseObjectStoreLookupError(_, e)
                | DatabaseState::RulesLoadError(_, e)
                | DatabaseState::CatalogLoadError(_, e)
                | DatabaseState::ReplayError(_, e) => {
                    error!(
                        %db_name,
                        %e,
                        %state,
                        "database in error state - operator intervention required"
                    );
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
                Ok(state) => DatabaseState::DatabaseObjectStoreFound(state),
                Err(InitError::NoActiveDatabase) => {
                    DatabaseState::NoActiveDatabase(state, Arc::new(InitError::NoActiveDatabase))
                }
                Err(e) => DatabaseState::DatabaseObjectStoreLookupError(state, Arc::new(e)),
            },
            DatabaseState::DatabaseObjectStoreFound(state) => match state.advance(shared).await {
                Ok(state) => DatabaseState::RulesLoaded(state),
                Err(e) => DatabaseState::RulesLoadError(state, Arc::new(e)),
            },
            DatabaseState::RulesLoaded(state) => match state.advance(shared).await {
                Ok(state) => DatabaseState::CatalogLoaded(state),
                Err(e) => DatabaseState::CatalogLoadError(state, Arc::new(e)),
            },
            DatabaseState::CatalogLoaded(state) => match state.advance(shared).await {
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
    #[snafu(display(
        "error finding active generation directory in object storage: {}",
        source
    ))]
    DatabaseObjectStoreLookup {
        source: iox_object_store::IoxObjectStoreError,
    },

    #[snafu(display("no active generation directory found, not loading"))]
    NoActiveDatabase,

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

    #[snafu(display("error saving database rules: {}", source))]
    SavingRules { source: crate::rules::Error },

    #[snafu(display("error loading database rules: {}", source))]
    LoadingRules { source: crate::rules::Error },

    #[snafu(display("{}", source))]
    IoxObjectStoreError {
        source: iox_object_store::IoxObjectStoreError,
    },

    #[snafu(display("cannot create database `{}`; it already exists", name))]
    DatabaseAlreadyExists { name: String },

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
    DatabaseObjectStoreFound(DatabaseStateDatabaseObjectStoreFound),

    RulesLoaded(DatabaseStateRulesLoaded),
    CatalogLoaded(DatabaseStateCatalogLoaded),
    Initialized(DatabaseStateInitialized),

    DatabaseObjectStoreLookupError(DatabaseStateKnown, Arc<InitError>),
    NoActiveDatabase(DatabaseStateKnown, Arc<InitError>),
    RulesLoadError(DatabaseStateDatabaseObjectStoreFound, Arc<InitError>),
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
            DatabaseState::DatabaseObjectStoreFound(_) => {
                DatabaseStateCode::DatabaseObjectStoreFound
            }
            DatabaseState::RulesLoaded(_) => DatabaseStateCode::RulesLoaded,
            DatabaseState::CatalogLoaded(_) => DatabaseStateCode::CatalogLoaded,
            DatabaseState::Initialized(_) => DatabaseStateCode::Initialized,
            DatabaseState::DatabaseObjectStoreLookupError(_, _) => {
                DatabaseStateCode::DatabaseObjectStoreLookupError
            }
            DatabaseState::NoActiveDatabase(_, _) => DatabaseStateCode::NoActiveDatabase,
            DatabaseState::RulesLoadError(_, _) => DatabaseStateCode::RulesLoadError,
            DatabaseState::CatalogLoadError(_, _) => DatabaseStateCode::CatalogLoadError,
            DatabaseState::ReplayError(_, _) => DatabaseStateCode::ReplayError,
        }
    }

    fn error(&self) -> Option<&Arc<InitError>> {
        match self {
            DatabaseState::Known(_)
            | DatabaseState::DatabaseObjectStoreFound(_)
            | DatabaseState::RulesLoaded(_)
            | DatabaseState::CatalogLoaded(_)
            | DatabaseState::Initialized(_) => None,
            DatabaseState::DatabaseObjectStoreLookupError(_, e)
            | DatabaseState::NoActiveDatabase(_, e)
            | DatabaseState::RulesLoadError(_, e)
            | DatabaseState::CatalogLoadError(_, e)
            | DatabaseState::ReplayError(_, e) => Some(e),
        }
    }

    fn provided_rules(&self) -> Option<Arc<ProvidedDatabaseRules>> {
        match self {
            DatabaseState::Known(_)
            | DatabaseState::DatabaseObjectStoreFound(_)
            | DatabaseState::DatabaseObjectStoreLookupError(_, _)
            | DatabaseState::NoActiveDatabase(_, _)
            | DatabaseState::RulesLoadError(_, _) => None,
            DatabaseState::RulesLoaded(state) | DatabaseState::CatalogLoadError(state, _) => {
                Some(Arc::clone(&state.provided_rules))
            }
            DatabaseState::CatalogLoaded(state) | DatabaseState::ReplayError(state, _) => {
                Some(Arc::clone(&state.provided_rules))
            }
            DatabaseState::Initialized(state) => Some(Arc::clone(&state.provided_rules)),
        }
    }

    fn iox_object_store(&self) -> Option<Arc<IoxObjectStore>> {
        match self {
            DatabaseState::Known(_)
            | DatabaseState::DatabaseObjectStoreLookupError(_, _)
            | DatabaseState::NoActiveDatabase(_, _)
            | DatabaseState::RulesLoadError(_, _) => None,
            DatabaseState::DatabaseObjectStoreFound(state) => {
                Some(Arc::clone(&state.iox_object_store))
            }
            DatabaseState::RulesLoaded(state) | DatabaseState::CatalogLoadError(state, _) => {
                Some(Arc::clone(&state.iox_object_store))
            }
            DatabaseState::CatalogLoaded(state) | DatabaseState::ReplayError(state, _) => {
                Some(state.db.iox_object_store())
            }
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
        let iox_object_store = IoxObjectStore::find_existing(
            Arc::clone(shared.application.object_store()),
            shared.config.server_id,
            &shared.config.name,
        )
        .await
        .context(DatabaseObjectStoreLookup)?
        .context(NoActiveDatabase)?;

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
    /// Load database rules from object storage
    async fn advance(
        &self,
        shared: &DatabaseShared,
    ) -> Result<DatabaseStateRulesLoaded, InitError> {
        let rules = ProvidedDatabaseRules::load(&self.iox_object_store)
            .await
            .context(LoadingRules)?;

        if rules.db_name() != &shared.config.name {
            return RulesDatabaseNameMismatch {
                actual: rules.db_name(),
                expected: shared.config.name.as_str(),
            }
            .fail();
        }

        Ok(DatabaseStateRulesLoaded {
            provided_rules: Arc::new(rules),
            iox_object_store: Arc::clone(&self.iox_object_store),
        })
    }
}

#[derive(Debug, Clone)]
struct DatabaseStateRulesLoaded {
    provided_rules: Arc<ProvidedDatabaseRules>,
    iox_object_store: Arc<IoxObjectStore>,
}

impl DatabaseStateRulesLoaded {
    /// Load catalog from object storage
    async fn advance(
        &self,
        shared: &DatabaseShared,
    ) -> Result<DatabaseStateCatalogLoaded, InitError> {
        let (preserved_catalog, catalog, replay_plan) = load_or_create_preserved_catalog(
            shared.config.name.as_str(),
            Arc::clone(&self.iox_object_store),
            Arc::clone(shared.application.metric_registry()),
            shared.config.wipe_catalog_on_error,
            shared.config.skip_replay,
        )
        .await
        .context(CatalogLoad)?;

        let rules = self.provided_rules.rules();
        let write_buffer_factory = shared.application.write_buffer_factory();
        let producer = match rules.write_buffer_connection.as_ref() {
            Some(connection) if matches!(connection.direction, WriteBufferDirection::Write) => {
                let producer = write_buffer_factory
                    .new_config_write(shared.config.name.as_str(), connection)
                    .await
                    .context(CreateWriteBuffer)?;
                Some(producer)
            }
            _ => None,
        };

        let database_to_commit = DatabaseToCommit {
            server_id: shared.config.server_id,
            iox_object_store: Arc::clone(&self.iox_object_store),
            exec: Arc::clone(shared.application.executor()),
            rules: Arc::clone(rules),
            preserved_catalog,
            catalog,
            write_buffer_producer: producer,
            metric_registry: Arc::clone(shared.application.metric_registry()),
        };

        let db = Db::new(
            database_to_commit,
            Arc::clone(shared.application.job_registry()),
        );

        Ok(DatabaseStateCatalogLoaded {
            db,
            replay_plan: Arc::new(replay_plan),
            provided_rules: Arc::clone(&self.provided_rules),
        })
    }
}

#[derive(Debug, Clone)]
struct DatabaseStateCatalogLoaded {
    db: Arc<Db>,
    replay_plan: Arc<Option<ReplayPlan>>,
    provided_rules: Arc<ProvidedDatabaseRules>,
}

impl DatabaseStateCatalogLoaded {
    /// Perform replay
    async fn advance(
        &self,
        shared: &DatabaseShared,
    ) -> Result<DatabaseStateInitialized, InitError> {
        let db = Arc::clone(&self.db);

        // TODO: Pull write buffer and lifecycle out of Db
        db.unsuppress_persistence().await;

        let rules = self.provided_rules.rules();
        let write_buffer_factory = shared.application.write_buffer_factory();
        let write_buffer_consumer = match rules.write_buffer_connection.as_ref() {
            Some(connection) if matches!(connection.direction, WriteBufferDirection::Read) => {
                let mut consumer = write_buffer_factory
                    .new_config_read(
                        shared.config.server_id,
                        shared.config.name.as_str(),
                        connection,
                    )
                    .await
                    .context(CreateWriteBuffer)?;

                db.perform_replay(self.replay_plan.as_ref().as_ref(), consumer.as_mut())
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

        Ok(DatabaseStateInitialized {
            db,
            write_buffer_consumer,
            provided_rules: Arc::clone(&self.provided_rules),
        })
    }
}

#[derive(Debug, Clone)]
pub struct DatabaseStateInitialized {
    db: Arc<Db>,
    write_buffer_consumer: Option<Arc<WriteBufferConsumer>>,
    provided_rules: Arc<ProvidedDatabaseRules>,
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
    use chrono::Utc;
    use data_types::database_rules::{
        PartitionTemplate, TemplatePart, WriteBufferConnection, WriteBufferDirection,
    };
    use entry::{test_helpers::lp_to_entries, Sequence, SequencedEntry};
    use object_store::{ObjectStore, ObjectStoreApi};
    use write_buffer::{config::WriteBufferConfigFactory, mock::MockBufferSharedState};

    use super::*;
    use object_store::path::ObjectStorePath;
    use std::{
        convert::{TryFrom, TryInto},
        num::NonZeroU32,
        time::Instant,
    };

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
                wipe_catalog_on_error: false,
                skip_replay: false,
            },
        );

        // Should have failed to load (this isn't important to the test)
        let err = database.wait_for_init().await.unwrap_err();
        assert!(
            matches!(err.as_ref(), InitError::NoActiveDatabase { .. }),
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
        let application = Arc::new(ApplicationState::new(
            Arc::new(ObjectStore::new_in_memory()),
            None,
        ));

        let db_name = DatabaseName::new("test").unwrap();
        Database::create(
            Arc::clone(&application),
            &ProvidedDatabaseRules::new_empty(db_name.clone()),
            server_id,
        )
        .await
        .unwrap();

        let db_config = DatabaseConfig {
            name: db_name,
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
    async fn database_delete_restore() {
        let (application, database) = initialized_database().await;
        database.delete().await.unwrap();

        assert_eq!(database.state_code(), DatabaseStateCode::NoActiveDatabase);
        assert!(matches!(
            database.init_error().unwrap().as_ref(),
            InitError::NoActiveDatabase
        ));

        // TODO: Replace with recover logic
        let os = application.object_store();
        let mut path = os.new_path();
        path.push_dir("1");
        path.push_dir("test");
        path.push_dir("0");
        path.set_file_name("DELETED");

        os.delete(&path).await.unwrap();

        {
            let mut state = database.shared.state.write();
            let mut state = state.get_mut().unwrap();
            *state = DatabaseState::Known(DatabaseStateKnown {});
            database.shared.state_notify.notify_waiters();
        }

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
    async fn skip_replay() {
        // create write buffer
        let state =
            MockBufferSharedState::empty_with_n_sequencers(NonZeroU32::try_from(1).unwrap());
        let partition_template = PartitionTemplate {
            parts: vec![TemplatePart::Column("partition_by".to_string())],
        };
        let entry_a = lp_to_entries("table_1,partition_by=a foo=1 10", &partition_template)
            .pop()
            .unwrap();
        let entry_b = lp_to_entries("table_1,partition_by=b foo=2 20", &partition_template)
            .pop()
            .unwrap();
        state.push_entry(SequencedEntry::new_from_sequence(
            Sequence::new(0, 10),
            Utc::now(),
            entry_a,
        ));
        state.push_entry(SequencedEntry::new_from_sequence(
            Sequence::new(0, 11),
            Utc::now(),
            entry_b,
        ));

        // setup application
        let mut factory = WriteBufferConfigFactory::new();
        factory.register_mock("my_mock".to_string(), state.clone());
        let application = Arc::new(ApplicationState::with_write_buffer_factory(
            Arc::new(ObjectStore::new_in_memory()),
            Arc::new(factory),
            None,
        ));
        let server_id = ServerId::try_from(1).unwrap();

        // setup DB
        let db_name = DatabaseName::new("test_db").unwrap();
        let rules = data_types::database_rules::DatabaseRules {
            name: db_name.clone(),
            partition_template: partition_template.clone(),
            lifecycle_rules: data_types::database_rules::LifecycleRules {
                late_arrive_window_seconds: NonZeroU32::try_from(1).unwrap(),
                ..Default::default()
            },
            routing_rules: None,
            worker_cleanup_avg_sleep: Duration::from_secs(2),
            write_buffer_connection: Some(WriteBufferConnection {
                direction: WriteBufferDirection::Read,
                type_: "mock".to_string(),
                connection: "my_mock".to_string(),
                ..Default::default()
            }),
        };
        Database::create(
            Arc::clone(&application),
            &make_provided_rules(rules),
            server_id,
        )
        .await
        .unwrap();
        let db_config = DatabaseConfig {
            name: db_name,
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
        db.persist_partition(
            "table_1",
            "partition_by_b",
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();

        // shutdown first database
        database.shutdown();
        database.join().await.unwrap();

        // break write buffer by removing entries
        state.clear_messages(0);
        let entry_c = lp_to_entries("table_1,partition_by=c foo=3 30", &partition_template)
            .pop()
            .unwrap();
        state.push_entry(SequencedEntry::new_from_sequence(
            Sequence::new(0, 12),
            Utc::now(),
            entry_c,
        ));

        // boot actual test database
        let database = Database::new(Arc::clone(&application), db_config.clone());

        // db is broken
        let err = database.wait_for_init().await.unwrap_err();
        assert!(matches!(err.as_ref(), InitError::Replay { .. }));

        // skip replay
        database.skip_replay().unwrap().await.unwrap();
        database.wait_for_init().await.unwrap();

        // wait for ingest
        let entry_d = lp_to_entries("table_1,partition_by=d foo=4 40", &partition_template)
            .pop()
            .unwrap();
        state.push_entry(SequencedEntry::new_from_sequence(
            Sequence::new(0, 13),
            Utc::now(),
            entry_d,
        ));
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
        let res = database.skip_replay();
        assert!(matches!(res, Err(Error::InvalidState { .. })));

        // clean up
        database.shutdown();
        database.join().await.unwrap();
    }

    /// Normally database rules are provided as grpc messages, but in
    /// tests they are constructed from database rules structures
    /// themselves.
    fn make_provided_rules(
        rules: data_types::database_rules::DatabaseRules,
    ) -> ProvidedDatabaseRules {
        let rules: generated_types::influxdata::iox::management::v1::DatabaseRules =
            rules.try_into().unwrap();

        let provided_rules: ProvidedDatabaseRules = rules.try_into().unwrap();
        provided_rules
    }
}
