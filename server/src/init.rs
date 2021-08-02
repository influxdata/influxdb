//! Routines to initialize a server.
use data_types::{
    database_rules::{DatabaseRules, WriteBufferConnection},
    database_state::DatabaseStateCode,
    error::ErrorLogger,
    DatabaseName,
};
use futures::TryStreamExt;
use generated_types::database_rules::decode_database_rules;
use object_store::{
    path::{parsed::DirsAndFileName, ObjectStorePath, Path},
    ObjectStore, ObjectStoreApi,
};
use observability_deps::tracing::{error, info, warn};
use parquet_file::catalog::PreservedCatalog;
use snafu::{ResultExt, Snafu};
use std::sync::Arc;
use write_buffer::config::WriteBufferConfig;

use crate::{
    config::{object_store_path_for_database_config, Config, DatabaseHandle, DB_RULES_FILE_NAME},
    db::load::load_or_create_preserved_catalog,
    DatabaseError,
};

const STORE_ERROR_PAUSE_SECONDS: u64 = 100;

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("cannot load catalog: {}", source))]
    CatalogLoadError { source: DatabaseError },

    #[snafu(display("error deserializing database rules from protobuf: {}", source))]
    ErrorDeserializingRulesProtobuf {
        source: generated_types::database_rules::DecodeError,
    },

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

    #[snafu(display("Cannot replay: {}", source))]
    ReplayError { source: Box<crate::db::Error> },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Loads the database configurations based on the databases in the
/// object store. Any databases in the config already won't be
/// replaced.
///
/// Returns a Vec containing the results of loading the contained databases
pub(crate) async fn initialize_server(
    config: Arc<Config>,
    wipe_on_error: bool,
    skip_replay_and_seek_instead: bool,
) -> Result<Vec<(DatabaseName<'static>, Result<()>)>> {
    let root = config.root_path();

    // get the database names from the object store prefixes
    // TODO: update object store to pull back all common prefixes by
    //       following the next tokens.
    let list_result = config
        .object_store()
        .list_with_delimiter(&root)
        .await
        .context(StoreError)?;

    let handles: Vec<_> = list_result
        .common_prefixes
        .into_iter()
        .filter_map(|mut path| {
            let config = Arc::clone(&config);
            let root = root.clone();
            path.set_file_name(DB_RULES_FILE_NAME);
            let db_name = db_name_from_rules_path(&path)
                .log_if_error("invalid database path")
                .ok()?;

            Some(async move {
                let result = initialize_database(
                    config,
                    root,
                    db_name.clone(),
                    wipe_on_error,
                    skip_replay_and_seek_instead,
                )
                .await;
                (db_name, result)
            })
        })
        .collect();

    Ok(futures::future::join_all(handles).await)
}

async fn initialize_database(
    config: Arc<Config>,
    root: Path,
    db_name: DatabaseName<'static>,
    wipe_on_error: bool,
    skip_replay_and_seek_instead: bool,
) -> Result<()> {
    // Reserve name before expensive IO (e.g. loading the preserved catalog)
    let mut handle = config
        .create_db(db_name)
        .map_err(Box::new)
        .context(InitDbError)?;

    match try_advance_database_init_process_until_complete(
        &mut handle,
        &root,
        wipe_on_error,
        skip_replay_and_seek_instead,
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

async fn load_database_rules(store: Arc<ObjectStore>, path: Path) -> Result<Option<DatabaseRules>> {
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
    config: Arc<Config>,
    db_name: &DatabaseName<'static>,
) -> Result<()> {
    let store = config.object_store();

    if config.has_uninitialized_database(db_name) {
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
        PreservedCatalog::wipe(&store, handle.server_id(), db_name)
            .await
            .map_err(Box::new)
            .context(PreservedCatalogWipeError)?;

        let root = config.root_path();

        let result =
            try_advance_database_init_process_until_complete(&mut handle, &root, true, true).await;

        // Commit changes even if failed
        handle.commit();
        result.map(|_| ())
    } else {
        let handle = config
            .block_db(db_name.clone())
            .map_err(|e| Arc::new(e) as _)
            .context(RecoverDbError)?;

        PreservedCatalog::wipe(&store, config.server_id(), db_name)
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
    skip_replay_and_seek_instead: bool,
) -> Result<bool> {
    loop {
        match try_advance_database_init_process(
            handle,
            root,
            wipe_on_error,
            skip_replay_and_seek_instead,
        )
        .await?
        {
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
    skip_replay_and_seek_instead: bool,
) -> Result<InitProgress> {
    match handle.state_code() {
        DatabaseStateCode::Known => {
            // known => load DB rules
            let path = object_store_path_for_database_config(root, &handle.db_name());
            match load_database_rules(handle.object_store(), path).await? {
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
            let (preserved_catalog, catalog, replay_plan) = load_or_create_preserved_catalog(
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
            let write_buffer = WriteBufferConfig::new(handle.server_id(), &rules)
                .await
                .context(CreateWriteBuffer {
                    config: rules.write_buffer_connection.clone(),
                })?;
            info!(write_buffer_enabled=?write_buffer.is_some(), db_name=rules.db_name(), "write buffer config");

            handle
                .advance_replay(preserved_catalog, catalog, replay_plan, write_buffer)
                .map_err(Box::new)
                .context(InitDbError)?;

            // there is still more work to do for this DB
            Ok(InitProgress::Unfinished)
        }
        DatabaseStateCode::Replay => {
            let db = handle
                .db_any_state()
                .expect("DB should be available in this state");
            let replay_plan = handle
                .replay_plan()
                .expect("replay plan should exist in this state");
            db.perform_replay(&replay_plan, skip_replay_and_seek_instead)
                .await
                .map_err(Box::new)
                .context(ReplayError)?;

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
    use object_store::path::ObjectStorePath;

    use super::*;

    #[tokio::test]
    async fn test_get_database_config_bytes() {
        let object_store = ObjectStore::new_in_memory();
        let mut rules_path = object_store.new_path();
        rules_path.push_all_dirs(&["1", "foo_bar"]);
        rules_path.set_file_name("rules.pb");

        let res = get_database_config_bytes(&rules_path, &object_store)
            .await
            .unwrap_err();
        assert!(matches!(res, Error::NoDatabaseConfigError { .. }));
    }
}
