mod v2;

use crate::object_store::{ObjectStoreCatalogError, PersistCatalogResult, versions as ostore};
use crate::serialize::versions::{v1 as serialize_v1, v2 as serialize_v2};
use anyhow::Context;
use object_store::{ObjectStore, PutMode, PutOptions};
use observability_deps::tracing::{error, info};
use std::sync::Arc;

/// Possible errors that can occur during catalog migrations.
#[derive(Debug, thiserror::Error)]
pub(crate) enum MigrationError {
    /// An error occurred while migrating the catalog.
    #[error(transparent)]
    ObjectStoreCatalog(#[from] ObjectStoreCatalogError),
    /// Failed to write the upgrade log entry, as an existing log with the same sequence number
    /// already exists.
    #[error("already exists")]
    UpgradeLogAlreadyExists,
    /// An unexpected error occurred.
    #[error(transparent)]
    Unexpected(#[from] anyhow::Error),
}

impl MigrationError {
    /// Return true if the error is recoverable and may be retried.
    pub(crate) fn is_retryable(&self) -> bool {
        matches!(self, MigrationError::UpgradeLogAlreadyExists)
    }
}

/// Check and perform a catalog migration from v1 → v2.
///
/// If a v2 catalog already exists or there is no v1 catalog to migrate from,
/// this function does nothing and returns `Ok`.
pub(crate) async fn check_and_migrate_v1_to_v2(
    prefix: Arc<str>,
    store: Arc<dyn ObjectStore>,
) -> Result<(), MigrationError> {
    {
        let v2_store =
            ostore::v2::ObjectStoreCatalog::new(Arc::clone(&prefix), u64::MAX, Arc::clone(&store));

        if v2_store.checkpoint_exists().await? {
            // There is already a v2 catalog present, so there is nothing to do.
            return Ok(());
        }
    }

    let v1_store = ostore::v1::ObjectStoreCatalog::new(Arc::clone(&prefix), Arc::clone(&store));

    if !v1_store.checkpoint_exists().await? {
        // There is no v1 catalog to migrate.
        return Ok(());
    }

    let v1_catalog = serialize_v1::load_catalog(Arc::clone(&prefix), Arc::clone(&store))
        .await?
        .expect("catalog should exist");

    let v2_catalog = v2::migrate(&v1_catalog).context("Failed to migrate catalog to v2")?;

    let v2_snapshot = {
        use crate::catalog::versions::v2::Snapshot;
        v2_catalog.snapshot()
    };

    let checkpoint_path = ostore::v2::CatalogFilePath::checkpoint(&prefix);

    let serialized_v2 = serialize_v2::serialize_catalog_file(&v2_snapshot)
        .context("Failed to serialize v2 snapshot")?;

    // Write the UpgradedLog to the prior catalog log, as once this is committed,
    // nodes running a prior version will no longer be able to load and / or mutate the v1 catalog.
    //
    // Further, the InnerCatalog is loaded, but the sequence number matches
    // the log entry of the one prior to the UpgradedLog, which prevents the catalog
    // from being mutated again. An attempt to persist a log entry will always return
    // PersistCatalogResult::AlreadyExists
    if !v1_catalog.has_upgraded {
        use ostore::v1::CatalogFilePath;
        let upgraded_log = serialize_v1::serialize_catalog_file(&crate::log::UpgradedLog)
            .expect("UpgradedLog should have serialized");
        let next_sequence = v1_catalog.sequence_number().next();
        let path = CatalogFilePath::log(prefix.as_ref(), next_sequence);
        if matches!(
            v1_store
                .catalog_update_if_not_exists(path, upgraded_log)
                .await?,
            PersistCatalogResult::AlreadyExists
        ) {
            // Another node must have raced and won, writing a log entry with the same sequence number.
            // We don't know if this is an upgrade log or the catalog was mutated, so we must fail
            // and the caller should retry the migration.
            return Err(MigrationError::UpgradeLogAlreadyExists);
        }
    } else {
        info!("Skip writing the UpgradeLog, as it is already present.")
    }

    info!(path = ?checkpoint_path, "Writing the v2 catalog checkpoint");
    match store
        .put_opts(
            &checkpoint_path,
            serialized_v2.into(),
            PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            },
        )
        .await
    {
        // AlreadyExists indicates another node raced and wrote the checkpoint before us.
        Ok(_) | Err(object_store::Error::AlreadyExists { .. }) => Ok(()),
        Err(error) => {
            error!(?error, "failed to persist catalog checkpoint file");
            Err(MigrationError::ObjectStoreCatalog(
                ObjectStoreCatalogError::ObjectStore(error),
            ))
        }
    }
}

#[cfg(test)]
mod tests;
