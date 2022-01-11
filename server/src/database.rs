pub(crate) mod init;
mod owner;
pub(crate) mod state;

use crate::{
    database::{
        init::{initialize_database, DatabaseState, InitError},
        owner::{update_owner_info, OwnerInfoUpdateError},
        state::{DatabaseConfig, DatabaseShared},
    },
    rules::{PersistedDatabaseRules, ProvidedDatabaseRules},
    ApplicationState,
};
use data_types::{job::Job, DatabaseName};
use db::Db;
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
use parking_lot::{RwLock, RwLockReadGuard};
use parquet_catalog::core::{PreservedCatalog, PreservedCatalogConfig};
use snafu::{ensure, OptionExt, ResultExt, Snafu};
use std::{future::Future, sync::Arc};
use tokio::task::JoinError;
use tokio_util::sync::CancellationToken;
use tracker::{TaskTracker, TrackedFutureExt};
use uuid::Uuid;

/// Matches an error [`DatabaseState`] and clones the contained state
macro_rules! error_state {
    ($s:expr, $transition: literal, $variant:ident) => {
        match &**$s.shared.state.read() {
            DatabaseState::$variant(state, _) => state.clone(),
            state => {
                return InvalidStateSnafu {
                    db_name: &$s.shared.config.read().name,
                    state: state.state_code(),
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
        "database ({}) in invalid state ({:?}) for wiping preserved catalog. Expected {}",
        db_name,
        state,
        expected
    ))]
    InvalidStateForWipePreservedCatalog {
        db_name: String,
        state: DatabaseStateCode,
        expected: String,
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

        let shared = Arc::new(DatabaseShared {
            config: RwLock::new(config),
            application,
            shutdown: Default::default(),
            state: RwLock::new(Freezable::new(DatabaseState::new_known())),
            state_notify: Default::default(),
        });

        let handle = tokio::spawn(background_worker(Arc::clone(&shared)));
        let join = handle.map_err(Arc::new).boxed().shared();

        Self { join, shared }
    }

    /// Release this database from this server.
    pub async fn release(&self) -> Result<Uuid, Error> {
        let db_name = self.name();

        let handle = self.shared.state.read().freeze();
        let handle = handle.await;

        let (uuid, iox_object_store) = {
            let state = self.shared.state.read();
            // Can't release an already released database
            ensure!(state.is_active(), CannotReleaseUnownedSnafu { db_name });

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
        .context(CannotReleaseSnafu { db_name })?;

        let mut state = self.shared.state.write();
        let mut state = state.unfreeze(handle);
        *state = DatabaseState::new_no_active_database();
        self.shared.state_notify.notify_waiters();

        Ok(uuid)
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
            *state = DatabaseState::new_known();
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
            let initialized = state.get_initialized().context(RulesNotUpdateableSnafu {
                db_name,
                state: state_code,
            })?;

            // A handle to the object store and a copy of the UUID so we can update the rules
            // in object store prior to obtaining exclusive write access to the `DatabaseState`
            // (which we can't hold across the await to write to the object store)
            (initialized.db().iox_object_store(), initialized.uuid())
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
            .context(CannotPersistUpdatedRulesSnafu)?;

        let mut state = self.shared.state.write();

        // Exchange FreezeHandle for mutable access to DatabaseState
        // via WriteGuard
        let mut state = state.unfreeze(handle);

        if let DatabaseState::Initialized(initialized) = &mut *state {
            initialized
                .db()
                .update_rules(Arc::clone(rules_to_persist.rules()));
            initialized.set_provided_rules(rules_to_persist.provided_rules());
            Ok(Arc::clone(initialized.provided_rules()))
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
        let iox_object_store = match &**self.shared.state.read() {
            DatabaseState::CatalogLoadError(rules_loaded, err) => {
                warn!(%db_name, %err, "Requested wiping catalog in CatalogLoadError state");
                Arc::clone(rules_loaded.iox_object_store())
            }
            DatabaseState::WriteBufferCreationError(catalog_loaded, err) => {
                warn!(%db_name, %err, "Requested wiping catalog in WriteBufferCreationError state");
                catalog_loaded.iox_object_store()
            }
            DatabaseState::ReplayError(catalog_loaded, err) => {
                warn!(%db_name, %err, "Requested wiping catalog in ReplayError state");
                catalog_loaded.iox_object_store()
            }
            state => {
                return InvalidStateForWipePreservedCatalogSnafu {
                    db_name,
                    state: state.state_code(),
                    expected: "CatalogLoadError, WriteBufferCreationError, ReplayError",
                }
                .fail()
            }
        };

        let registry = self.shared.application.job_registry();
        let (tracker, registration) = registry.register(Job::WipePreservedCatalog {
            db_name: Arc::from(db_name.as_str()),
        });

        let shared = Arc::clone(&self.shared);

        tokio::spawn(
            async move {
                // wipe the actual catalog
                PreservedCatalog::wipe(&iox_object_store)
                    .await
                    .map_err(Box::new)
                    .context(WipePreservedCatalogSnafu { db_name })?;

                {
                    let mut state = shared.state.write();
                    let mut state = state.unfreeze(handle);

                    // Leave temporary `known` state, so we can use
                    // current state to compute the new one
                    let current_state = std::mem::replace(&mut *state, DatabaseState::new_known());
                    let rules_loaded = match current_state {
                        DatabaseState::CatalogLoadError(rules_loaded, _err) => rules_loaded,
                        DatabaseState::WriteBufferCreationError(catalog_loaded, _err) => {
                            catalog_loaded.rollback()
                        }
                        DatabaseState::ReplayError(catalog_loaded, _err) => {
                            catalog_loaded.rollback()
                        }
                        // as we have already wiped the preserved
                        // catalog, we ca not return an error but leave the
                        // state as is, thus panic ...
                        _ => unreachable!(
                            "Wiped preserved catalog and then found database in invalid state: {}",
                            current_state.state_code()
                        ),
                    };
                    // set the new state
                    *state = DatabaseState::RulesLoaded(rules_loaded);
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
                return InvalidStateForRebuildSnafu {
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
        let iox_object_store = self
            .iox_object_store()
            .context(InvalidStateForRebuildSnafu {
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
                    *state.unfreeze(handle) = DatabaseState::new_known();
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
                        UnexpectedTransitionForRebuildSnafu {
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
                    .context(WipePreservedCatalogSnafu { db_name: &db_name })?;

                let config = PreservedCatalogConfig::new(
                    Arc::clone(&iox_object_store),
                    db_name.to_string(),
                    Arc::clone(shared.application.time_provider()),
                );
                parquet_catalog::rebuild::rebuild_catalog(config, false)
                    .await
                    .map_err(Box::new)
                    .context(RebuildPreservedCatalogSnafu { db_name: &db_name })?;

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
        error_state!(self, "SkipReplay", ReplayError);

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
        if !self.shared.shutdown.is_cancelled() {
            warn!(%db_name, "database dropped without calling shutdown()");
            self.shared.shutdown.cancel();
        }

        if self.join.clone().now_or_never().is_none() {
            warn!(%db_name, "database dropped without waiting for worker termination");
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use super::{
        init::{claim_database_in_object_store, create_empty_db_in_object_store},
        owner::fetch_owner_info,
    };
    use crate::test_utils::make_application;
    use data_types::server_id::ServerId;
    use data_types::{
        database_rules::{PartitionTemplate, TemplatePart},
        sequence::Sequence,
        write_buffer::WriteBufferConnection,
    };
    use std::time::Duration;
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

        let location = create_empty_db_in_object_store(
            Arc::clone(&application),
            uuid,
            provided_rules,
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
            *state = DatabaseState::new_known();
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

        // database is in error state
        assert_eq!(database.state_code(), DatabaseStateCode::NoActiveDatabase);
        assert!(matches!(
            database.init_error().unwrap().as_ref(),
            InitError::NoActiveDatabase
        ));

        claim_database_in_object_store(
            Arc::clone(&application),
            db_name,
            uuid,
            new_server_id,
            false,
        )
        .await
        .unwrap();

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
        let location = create_empty_db_in_object_store(
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
        let location = create_empty_db_in_object_store(
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

    #[tokio::test]
    async fn database_init_recovery() {
        let (application, database) = initialized_database().await;
        let iox_object_store = database.iox_object_store().unwrap();
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
