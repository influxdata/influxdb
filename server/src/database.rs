use std::{future::Future, sync::Arc};

use futures::{future::FusedFuture, FutureExt};
use parking_lot::{Mutex, RwLock, RwLockReadGuard};
use snafu::{ResultExt, Snafu};
use tokio::task::JoinError;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use data_types::error::ErrorLogger;
use data_types::{job::Job, DatabaseName};
use db::Db;
use generated_types::{
    database_state::DatabaseState as DatabaseStateCode, influxdata::iox::management,
};
use internal_types::freezable::Freezable;
use iox_object_store::IoxObjectStore;
use observability_deps::tracing::{error, info, warn};
use parquet_catalog::core::{PreservedCatalog, PreservedCatalogConfig};
use tracker::{TaskTracker, TrackedFutureExt};

use crate::{
    database::{
        init::{initialize_database, DatabaseState, InitError},
        state::{DatabaseConfig, DatabaseShared},
    },
    rules::ProvidedDatabaseRules,
    ApplicationState,
};

pub(crate) mod init;
pub(crate) mod state;

/// Matches an error [`DatabaseState`] and clones the contained state
macro_rules! error_state {
    ($s:expr, $transition: literal, $code: pat) => {
        match $s.state_code() {
            $code => {}
            state => {
                return InvalidStateSnafu {
                    db_name: &$s.shared.config.read().name,
                    state,
                    transition: $transition,
                }
                .fail()
            }
        }
    };
}

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
    CannotPersistUpdatedRules { source: crate::config::Error },

    #[snafu(display(
        "cannot release database named {} that has already been released",
        db_name
    ))]
    CannotReleaseUnowned { db_name: String },

    #[snafu(display("cannot release database {}: {}", db_name, source))]
    CannotRelease {
        db_name: String,
        source: crate::config::Error,
    },
}

/// A `Database` represents a single configured IOx database - i.e. an
/// entity with a corresponding set of `DatabaseRules`.
///
/// `Database` composes together the various subsystems responsible for implementing
/// `DatabaseRules` and handles their startup and shutdown. This includes instance-local
/// data storage (i.e. `Db`), the write buffer, request routing, data lifecycle, etc...
#[derive(Debug)]
pub struct Database {
    /// The state shared with the background worker
    shared: Arc<DatabaseShared>,

    /// The cancellation token for the current background worker
    shutdown: Mutex<CancellationToken>,
}

impl Database {
    #[allow(rustdoc::private_intra_doc_links)]
    /// Create in-mem database object.
    ///
    /// This is backed by an existing database, which was
    /// [created](crate::database::init::create_empty_db_in_object_store) some time in the
    /// past.
    pub fn new(application: Arc<ApplicationState>, config: DatabaseConfig) -> Self {
        info!(
            db_name=%config.name,
            "new database"
        );

        let path = IoxObjectStore::root_path_for(application.object_store(), config.database_uuid);
        // The database state machine handles the case of this path not existing, as it will end
        // up in [`DatabaseState::RulesLoadError`] or [`DatabaseState::OwnerInfoLoadError`]
        let iox_object_store = Arc::new(IoxObjectStore::existing(
            Arc::clone(application.object_store()),
            path,
        ));

        let shared = Arc::new(DatabaseShared {
            config: RwLock::new(config),
            application,
            iox_object_store,
            state: RwLock::new(Freezable::new(DatabaseState::Shutdown(None))),
            state_notify: Default::default(),
        });

        let shutdown = new_database_worker(Arc::clone(&shared));

        Self {
            shared,
            shutdown: Mutex::new(shutdown),
        }
    }

    /// Shutdown and release this database
    pub async fn release(&self) -> Result<Uuid, Error> {
        let db_name = self.name();
        let db_name = db_name.as_str();

        self.shutdown();
        let _ = self.join().await.log_if_error("releasing database");

        let uuid = self.uuid();

        self.shared
            .application
            .config_provider()
            .update_owner_info(None, uuid)
            .await
            .context(CannotReleaseSnafu { db_name })?;

        Ok(uuid)
    }

    /// Triggers shutdown of this `Database` if it is running
    pub fn shutdown(&self) {
        let db_name = self.name();
        info!(%db_name, "database shutting down");
        self.shutdown.lock().cancel()
    }

    /// Trigger a restart of this `Database` and wait for it to re-initialize
    pub async fn restart(&self) -> Result<(), Arc<InitError>> {
        self.restart_with_options(false).await
    }

    /// Trigger a restart of this `Database` and wait for it to re-initialize
    pub async fn restart_with_options(&self, skip_replay: bool) -> Result<(), Arc<InitError>> {
        let db_name = self.name();
        info!(%db_name, "restarting database");

        // Ensure database is shut down
        self.shutdown();
        let _ = self.join().await.log_if_error("restarting database");

        self.shared.config.write().skip_replay = skip_replay;

        {
            let mut shutdown = self.shutdown.lock();
            *shutdown = new_database_worker(Arc::clone(&self.shared));
        }

        self.wait_for_init().await
    }

    /// Waits for the background worker of this `Database` to exit
    ///
    /// TODO: Rename to wait_for_shutdown
    pub fn join(&self) -> impl Future<Output = Result<(), Arc<JoinError>>> {
        let shared = Arc::clone(&self.shared);
        async move {
            loop {
                // Register interest before checking to avoid race
                let notify = shared.state_notify.notified();

                match &**shared.state.read() {
                    DatabaseState::Shutdown(Some(e)) => return Err(Arc::clone(e)),
                    DatabaseState::Shutdown(None) => return Ok(()),
                    state => info!(%state, "waiting for database shutdown"),
                }

                notify.await;
            }
        }
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

    /// Returns true if this database is shutdown
    pub fn is_shutdown(&self) -> bool {
        self.shared.state.read().is_shutdown()
    }

    /// Returns the database rules if they're loaded
    pub fn provided_rules(&self) -> Option<Arc<ProvidedDatabaseRules>> {
        self.shared.state.read().provided_rules()
    }

    /// Returns the database UUID
    pub fn uuid(&self) -> Uuid {
        self.shared.config.read().database_uuid
    }

    /// Returns the info about the owning server if it has been loaded
    pub fn owner_info(&self) -> Option<management::v1::OwnerInfo> {
        self.shared.state.read().owner_info()
    }

    /// Database name
    pub fn name(&self) -> DatabaseName<'static> {
        self.shared.config.read().name.clone()
    }

    /// Update the database rules, panic'ing if the state is invalid
    pub async fn update_provided_rules(
        &self,
        provided_rules: ProvidedDatabaseRules,
    ) -> Result<Arc<ProvidedDatabaseRules>, Error> {
        // get a handle to signal our intention to update the state
        let handle = self.shared.state.read().freeze();

        // wait for the freeze handle. Only one thread can ever have
        // it at any time so we know past this point no other thread
        // can change the DatabaseState (even though this code
        // doesn't hold a lock for the entire time)
        let handle = handle.await;

        error_state!(self, "UpdateProvidedRules", DatabaseStateCode::Initialized);

        // Attempt to persist to object store, if that fails, roll
        // back the whole transaction (leave the rules unchanged).
        //
        // Even though we don't hold a lock here, the freeze handle
        // ensures the state can not be modified.
        self.shared
            .application
            .config_provider()
            .store_rules(self.uuid(), &provided_rules)
            .await
            .context(CannotPersistUpdatedRulesSnafu)?;

        let mut state = self.shared.state.write();

        // Exchange FreezeHandle for mutable access to DatabaseState
        // via WriteGuard
        let mut state = state.unfreeze(handle);

        if let DatabaseState::Initialized(initialized) = &mut *state {
            initialized
                .db()
                .update_rules(Arc::clone(provided_rules.rules()));

            let rules = Arc::new(provided_rules);
            initialized.set_provided_rules(Arc::clone(&rules));
            Ok(rules)
        } else {
            // The freeze handle should have prevented any changes to
            // the database state between when it was checked above
            // and now
            unreachable!()
        }
    }

    /// Returns the IoxObjectStore
    pub fn iox_object_store(&self) -> Arc<IoxObjectStore> {
        Arc::clone(&self.shared.iox_object_store)
    }

    /// Gets access to an initialized `Db`
    pub fn initialized_db(&self) -> Option<Arc<Db>> {
        let initialized =
            RwLockReadGuard::try_map(self.shared.state.read(), |state| state.get_initialized())
                .ok()?;
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
                | DatabaseState::OwnerInfoLoaded(_)
                | DatabaseState::RulesLoaded(_)
                | DatabaseState::CatalogLoaded(_) => {} // Non-terminal state
                DatabaseState::Initialized(_) => return Ok(()),
                DatabaseState::OwnerInfoLoadError(_, e)
                | DatabaseState::RulesLoadError(_, e)
                | DatabaseState::CatalogLoadError(_, e)
                | DatabaseState::WriteBufferCreationError(_, e)
                | DatabaseState::ReplayError(_, e) => return Err(Arc::clone(e)),
                DatabaseState::Shutdown(_) => return Err(Arc::new(InitError::Shutdown)),
            }

            notify.await;
        }
    }

    /// Recover from a CatalogLoadError by wiping the catalog
    pub async fn wipe_preserved_catalog(self: &Arc<Self>) -> Result<TaskTracker<Job>, Error> {
        let db_name = self.name();

        error_state!(
            self,
            "WipePreservedCatalog",
            DatabaseStateCode::CatalogLoadError
                | DatabaseStateCode::WriteBufferCreationError
                | DatabaseStateCode::ReplayError
                | DatabaseStateCode::Shutdown
        );

        // Shutdown database
        self.shutdown();
        let _ = self.join().await.log_if_error("wipe preserved catalog");

        // Hold a freeze handle to prevent other processes from restarting the database
        let handle = self.shared.state.read().freeze();
        let handle = handle.await;

        error_state!(self, "WipePreservedCatalog", DatabaseStateCode::Shutdown);

        let registry = self.shared.application.job_registry();
        let (tracker, registration) = registry.register(Job::WipePreservedCatalog {
            db_name: Arc::from(db_name.as_str()),
        });

        let this = Arc::clone(self);
        tokio::spawn(
            async move {
                // wipe the actual catalog
                PreservedCatalog::wipe(&this.shared.iox_object_store)
                    .await
                    .map_err(Box::new)
                    .context(WipePreservedCatalogSnafu { db_name: &db_name })?;

                info!(%db_name, "wiped preserved catalog");

                // Should be guaranteed by the freeze handle
                assert_eq!(this.state_code(), DatabaseStateCode::Shutdown);

                std::mem::drop(handle);

                let _ = this.restart().await;
                info!(%db_name, "restarted database following wipe");

                Ok::<_, Error>(())
            }
            .track(registration),
        );

        Ok(tracker)
    }

    /// Rebuilding the catalog from parquet files. This can be used to
    /// recover from a CatalogLoadError, or if new parquet files are
    /// added to the data directory
    pub async fn rebuild_preserved_catalog(
        self: &Arc<Self>,
        force: bool,
    ) -> Result<TaskTracker<Job>, Error> {
        if !force {
            error_state!(
                self,
                "RebuildPreservedCatalog",
                DatabaseStateCode::CatalogLoadError | DatabaseStateCode::Shutdown
            );
        }

        let shared = Arc::clone(&self.shared);
        let db_name = self.name();

        // Shutdown database
        self.shutdown();
        let _ = self.join().await.log_if_error("rebuilding catalog");

        // Obtain and hold a freeze handle to ensure nothing restarts the database
        let handle = self.shared.state.read().freeze();
        let handle = handle.await;

        error_state!(self, "RebuildPreservedCatalog", DatabaseStateCode::Shutdown);

        let registry = self.shared.application.job_registry();
        let (tracker, registration) = registry.register(Job::RebuildPreservedCatalog {
            db_name: Arc::from(db_name.as_str()),
        });

        let this = Arc::clone(self);
        tokio::spawn(
            async move {
                info!(%db_name, "rebuilding catalog from parquet files");

                // Now wipe the catalog and rebuild it from parquet files
                PreservedCatalog::wipe(&this.shared.iox_object_store)
                    .await
                    .map_err(Box::new)
                    .context(WipePreservedCatalogSnafu { db_name: &db_name })?;

                info!(%db_name, "wiped preserved catalog");

                let config = PreservedCatalogConfig::new(
                    Arc::clone(&this.shared.iox_object_store),
                    db_name.to_string(),
                    Arc::clone(shared.application.time_provider()),
                );

                parquet_catalog::rebuild::rebuild_catalog(config, false)
                    .await
                    .map_err(Box::new)
                    .context(RebuildPreservedCatalogSnafu { db_name: &db_name })?;

                // Double check the state hasn't changed (we hold the
                // freeze handle to make sure it does not)
                assert_eq!(this.state_code(), DatabaseStateCode::Shutdown);

                std::mem::drop(handle);

                info!(%db_name, "rebuilt preserved catalog");

                let _ = this.restart().await;
                info!(%db_name, "restarted following rebuild");

                Ok::<_, Error>(())
            }
            .track(registration),
        );

        Ok(tracker)
    }

    /// Recover from a ReplayError by skipping replay
    pub async fn skip_replay(&self) -> Result<(), Error> {
        error_state!(self, "SkipReplay", DatabaseStateCode::ReplayError);

        self.shared.config.write().skip_replay = true;

        // wait for DB to leave a potential `ReplayError` state
        loop {
            // Register interest before checking to avoid race
            let notify = self.shared.state_notify.notified();

            match &**self.shared.state.read() {
                DatabaseState::ReplayError(_, _) => {}
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
        let shutdown = self.shutdown.lock().clone();
        if !shutdown.is_cancelled() {
            warn!(%db_name, "database dropped without calling shutdown()");
            shutdown.cancel();
        }

        if !self.shared.state.read().is_shutdown() {
            warn!(%db_name, "database dropped without waiting for worker termination");
        }
    }
}

/// Spawn a new background worker for a database in the `shutdown` state
/// The state is reset to `Known` then the background worker attempts to drive the
/// Database through initialization
fn new_database_worker(shared: Arc<DatabaseShared>) -> CancellationToken {
    let shutdown = CancellationToken::new();

    let db_name = shared.config.read().name.clone();

    {
        let mut state = shared.state.write();
        if !state.is_shutdown() {
            panic!(
                "cannot spawn worker for database {} that is not shutdown!",
                db_name
            )
        }
        let handle = state.try_freeze().expect("restart race");
        *state.unfreeze(handle) = DatabaseState::new_known();
    }

    // Spawn a worker task
    let worker = tokio::spawn(background_worker(Arc::clone(&shared), shutdown.clone()));

    // We spawn a watchdog task to detect and respond to the background worker exiting
    let _ = tokio::spawn(async move {
        let error = match worker.await {
            Ok(_) => {
                info!(%db_name, "observed clean shutdown of database worker");
                None
            }
            Err(e) => {
                if e.is_panic() {
                    error!(
                        %db_name,
                        %e,
                        "panic in database worker"
                    );
                } else {
                    error!(
                        %db_name,
                        %e,
                        "unexpected database worker shut down - shutting down server"
                    );
                }

                Some(Arc::new(e))
            }
        };

        let handle_fut = shared.state.read().freeze();
        let handle = handle_fut.await;

        // This is the only place that sets the shutdown state ensuring that
        // the shutdown state is guaranteed to not have a background worker running
        *shared.state.write().unfreeze(handle) = DatabaseState::Shutdown(error);
        shared.state_notify.notify_waiters();
    });

    shutdown
}

/// The background worker for `Database` - there should only ever be one
async fn background_worker(shared: Arc<DatabaseShared>, shutdown: CancellationToken) {
    let db_name = shared.config.read().name.clone();
    info!(%db_name, "started database background worker");

    initialize_database(shared.as_ref(), shutdown.clone()).await;

    if shutdown.is_cancelled() {
        // TODO: Shutdown intermediate workers (#2813)
        info!(%db_name, "database shutdown before finishing initialization");
        return;
    }

    let (db, write_buffer_consumer, lifecycle_worker) = {
        let state = shared.state.read();
        let initialized = state.get_initialized().expect("expected initialized");

        (
            Arc::clone(initialized.db()),
            initialized.write_buffer_consumer().map(Arc::clone),
            Arc::clone(initialized.lifecycle_worker()),
        )
    };

    info!(%db_name, "database finished initialization - starting Db worker");

    db::utils::panic_test(|| Some(format!("database background worker: {}", db_name,)));

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
    while !shutdown.is_cancelled() {
        if shared.state.read().get_initialized().is_none() {
            info!(%db_name, "database no longer initialized");
            break;
        }

        let shutdown_fut = shutdown.cancelled().fuse();
        futures::pin_mut!(shutdown_fut);

        // We must use `futures::select` as opposed to the often more ergonomic `tokio::select`
        // Because of the need to "re-use" the background worker future
        // TODO: Make Db own its own background loop (or remove it)
        futures::select! {
            _ = shutdown_fut => info!("database shutting down"),
            _ = consumer_join => {
                error!(%db_name, "unexpected shutdown of write buffer consumer - bailing out");
                shutdown.cancel();
            }
            _ = lifecycle_join => {
                error!(%db_name, "unexpected shutdown of lifecycle worker - bailing out");
                shutdown.cancel();
            }
            _ = db_worker => {
                error!(%db_name, "unexpected shutdown of db - bailing out");
                shutdown.cancel();
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

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use std::{num::NonZeroU32, time::Instant};

    use uuid::Uuid;

    use data_types::server_id::ServerId;
    use data_types::{
        database_rules::{PartitionTemplate, TemplatePart},
        sequence::Sequence,
        write_buffer::WriteBufferConnection,
    };
    use object_store::{ObjectStore, ObjectStoreIntegration, ThrottleConfig};
    use test_helpers::assert_contains;
    use write_buffer::mock::MockBufferSharedState;

    use crate::test_utils::make_application;

    use super::init::{claim_database_in_object_store, create_empty_db_in_object_store};
    use super::*;

    #[tokio::test]
    async fn database_shutdown_waits_for_jobs() {
        let (application, database) = initialized_database().await;

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

        let name = DatabaseName::new("test").unwrap();
        let database_uuid = Uuid::new_v4();
        let provided_rules = ProvidedDatabaseRules::new_empty(name.clone());

        create_empty_db_in_object_store(
            Arc::clone(&application),
            database_uuid,
            provided_rules,
            server_id,
        )
        .await
        .unwrap();

        let db_config = DatabaseConfig {
            name,
            server_id,
            database_uuid,
            wipe_catalog_on_error: false,
            skip_replay: false,
        };
        let database = Database::new(Arc::clone(&application), db_config.clone());
        database.wait_for_init().await.unwrap();
        (application, database)
    }

    #[tokio::test]
    async fn database_release() {
        let (application, database) = initialized_database().await;
        let server_id = database.shared.config.read().server_id;
        let server_location =
            IoxObjectStore::server_config_path(application.object_store(), server_id).to_string();

        let uuid = database.release().await.unwrap();

        assert_eq!(database.state_code(), DatabaseStateCode::Shutdown);
        assert!(database.init_error().is_none());

        let owner_info = application
            .config_provider()
            .fetch_owner_info(server_id, uuid)
            .await
            .unwrap();

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
        let new_server_id = ServerId::try_from(2).unwrap();
        let new_server_location =
            IoxObjectStore::server_config_path(application.object_store(), new_server_id)
                .to_string();
        let uuid = database.release().await.unwrap();

        // database is in error state
        assert_eq!(database.state_code(), DatabaseStateCode::Shutdown);
        assert!(database.init_error().is_none());

        claim_database_in_object_store(
            Arc::clone(&application),
            db_name,
            uuid,
            new_server_id,
            false,
        )
        .await
        .unwrap();

        let owner_info = application
            .config_provider()
            .fetch_owner_info(server_id, uuid)
            .await
            .unwrap();

        assert_eq!(owner_info.id, new_server_id.get_u32());
        assert_eq!(owner_info.location, new_server_location);
        assert_eq!(owner_info.transactions.len(), 2);

        let release_transaction = &owner_info.transactions[0];
        assert_eq!(release_transaction.id, server_id.get_u32());
        assert_eq!(release_transaction.location, server_location);

        let claim_transaction = &owner_info.transactions[1];
        assert_eq!(claim_transaction.id, 0);
        assert_eq!(claim_transaction.location, "");

        // put it back to first DB
        let db_config = DatabaseConfig {
            server_id: new_server_id,
            ..database.shared.config.read().clone()
        };
        let new_database = Database::new(Arc::clone(&application), db_config.clone());
        new_database.wait_for_init().await.unwrap();
        new_database.release().await.unwrap();
        claim_database_in_object_store(application, db_name, uuid, server_id, false)
            .await
            .unwrap();

        // database should recover
        tokio::time::timeout(Duration::from_secs(10), database.restart())
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn database_restart() {
        test_helpers::maybe_start_logging();
        let (_application, database) = initialized_database().await;

        // Restart successful
        database.restart().await.unwrap();

        assert!(database.is_initialized());

        // Delete the rules
        let iox_object_store = database.iox_object_store();
        iox_object_store.delete_database_rules_file().await.unwrap();

        // Restart should fail
        let err = database.restart().await.unwrap_err().to_string();
        assert_contains!(&err, "error loading database rules");
        assert_contains!(&err, "not found");
    }

    #[tokio::test]
    async fn database_abort() {
        test_helpers::maybe_start_logging();

        // Create a throttled object store that will stall the init process
        let throttle_config = ThrottleConfig {
            wait_get_per_call: Duration::from_secs(100),
            ..Default::default()
        };

        let store = Arc::new(ObjectStore::new_in_memory_throttled(throttle_config));
        let application = Arc::new(ApplicationState::new(Arc::clone(&store), None, None, None));

        let db_config = DatabaseConfig {
            name: DatabaseName::new("test").unwrap(),
            database_uuid: Uuid::new_v4(),
            server_id: ServerId::try_from(1).unwrap(),
            wipe_catalog_on_error: false,
            skip_replay: false,
        };

        let database = Database::new(Arc::clone(&application), db_config.clone());

        // Should fail to initialize in a timely manner
        tokio::time::timeout(Duration::from_millis(10), database.wait_for_init())
            .await
            .expect_err("should timeout");

        assert_eq!(database.state_code(), DatabaseStateCode::Known);

        database.shutdown();
        database.join().await.unwrap();

        assert_eq!(database.state_code(), DatabaseStateCode::Shutdown);

        // Disable throttling
        match &store.integration {
            ObjectStoreIntegration::InMemoryThrottled(s) => {
                s.config_mut(|c| *c = Default::default())
            }
            _ => unreachable!(),
        }

        // Restart should recover from aborted state, but will now error due to missing config
        let error = tokio::time::timeout(Duration::from_secs(1), database.restart())
            .await
            .expect("no timeout")
            .unwrap_err()
            .to_string();

        assert_contains!(error, "error getting database owner info");
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

        create_empty_db_in_object_store(
            Arc::clone(&application),
            uuid,
            make_provided_rules(rules),
            server_id,
        )
        .await
        .unwrap();

        let db_config = DatabaseConfig {
            name: db_name,
            server_id,
            database_uuid: uuid,
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

        create_empty_db_in_object_store(
            Arc::clone(&application),
            uuid,
            make_provided_rules(rules),
            server_id,
        )
        .await
        .unwrap();

        let db_config = DatabaseConfig {
            name: db_name,
            server_id,
            database_uuid: uuid,
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

    #[tokio::test]
    async fn database_init_recovery() {
        let (application, database) = initialized_database().await;
        let iox_object_store = database.iox_object_store();
        let config = database.shared.config.read().clone();

        // shutdown first database
        database.shutdown();
        database.join().await.unwrap();

        // mess up owner file
        let owner_backup = iox_object_store.get_owner_file().await.unwrap();
        iox_object_store
            .delete_owner_file_for_testing()
            .await
            .unwrap();

        // create second database
        let database = Database::new(Arc::clone(&application), config);
        database.wait_for_init().await.unwrap_err();

        // recover database by fixing owner file
        iox_object_store.put_owner_file(owner_backup).await.unwrap();
        tokio::time::timeout(Duration::from_secs(10), async move {
            loop {
                if database.wait_for_init().await.is_ok() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
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
