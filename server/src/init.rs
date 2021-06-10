//! Routines to initialize a server.
use data_types::server_id::ServerId;
use futures::TryStreamExt;
use generated_types::database_rules::decode_database_rules;
use internal_types::once::OnceNonZeroU32;
use object_store::{
    path::{ObjectStorePath, Path},
    ObjectStore, ObjectStoreApi,
};
use observability_deps::tracing::{debug, error, info, warn};
use parking_lot::Mutex;
use query::exec::Executor;
use snafu::{OptionExt, ResultExt, Snafu};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::sync::Semaphore;

use crate::{
    config::{Config, DB_RULES_FILE_NAME},
    db::load_or_create_preserved_catalog,
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

    #[snafu(display("Cannot create DB: {}", source))]
    CreateDbError { source: Box<crate::Error> },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Default)]
pub struct CurrentServerId(OnceNonZeroU32);

impl CurrentServerId {
    pub fn set(&self, id: ServerId) -> Result<()> {
        self.0.set(id.get()).map_err(|id| Error::IdAlreadySet {
            id: ServerId::new(id),
        })
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
        // ordering here isn't that important since this method is not used to check-and-modify the flag
        self.initialized.load(Ordering::Relaxed)
    }

    /// Error occurred during generic server init (e.g. listing store content).
    pub fn error_generic(&self) -> Option<Arc<Error>> {
        let guard = self.error_generic.lock();
        guard.clone()
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
        // get the database names from the object store prefixes
        // TODO: update object store to pull back all common prefixes by
        //       following the next tokens.
        let list_result = store
            .list_with_delimiter(&self.root_path(&store)?)
            .await
            .context(StoreError)?;

        let handles: Vec<_> = list_result
            .common_prefixes
            .into_iter()
            .map(|mut path| {
                let store = Arc::clone(&store);
                let config = Arc::clone(&config);
                let exec = Arc::clone(&exec);

                path.set_file_name(DB_RULES_FILE_NAME);

                tokio::task::spawn(async move {
                    if let Err(e) =
                        Self::load_database_config(server_id, store, config, exec, path).await
                    {
                        error!(%e, "cannot load database");
                    }
                })
            })
            .collect();

        futures::future::join_all(handles).await;

        Ok(())
    }

    async fn load_database_config(
        server_id: ServerId,
        store: Arc<ObjectStore>,
        config: Arc<Config>,
        exec: Arc<Executor>,
        path: Path,
    ) -> Result<()> {
        let serialized_rules = loop {
            match get_database_config_bytes(&path, &store).await {
                Ok(data) => break data,
                Err(e) => {
                    if let Error::NoDatabaseConfigError { location } = &e {
                        warn!(?location, "{}", e);
                        return Ok(());
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

        let preserved_catalog = load_or_create_preserved_catalog(
            rules.db_name(),
            Arc::clone(&store),
            server_id,
            config.metrics_registry(),
        )
        .await
        .map_err(|e| Box::new(e) as _)
        .context(CatalogLoadError)?;

        let handle = config
            .create_db(rules)
            .map_err(Box::new)
            .context(CreateDbError)?;
        handle.commit(server_id, store, exec, preserved_catalog);

        Ok(())
    }
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
