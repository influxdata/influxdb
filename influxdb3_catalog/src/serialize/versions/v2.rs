use anyhow::Context;
use backon::{ExponentialBuilder, Retryable};
use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use futures::stream::FuturesOrdered;
use object_store::ObjectStore;
use object_store::path::Path as ObjPath;
use observability_deps::tracing::{debug, trace, warn};
use serde::Serialize;
use std::sync::Arc;
use std::time::Duration;

type Result<T> = std::result::Result<T, ObjectStoreCatalogError>;

use crate::catalog::versions::v2::{
    InnerCatalog, Snapshot,
    log::{self},
    snapshot::CatalogSnapshot,
};
use crate::object_store::{ObjectStoreCatalogError, versions::v2::ObjectStoreCatalog};
use crate::serialize::{VersionedFileType, versions};

use crate::catalog::migrations::check_and_migrate_v1_to_v2;
use crate::object_store::versions::v2::CatalogFilePath;

const CHECKSUM_LEN: usize = size_of::<u32>();

pub async fn load_catalog(
    prefix: Arc<str>,
    store: &ObjectStoreCatalog,
) -> Result<Option<InnerCatalog>> {
    let os = store.object_store();
    (async || check_and_migrate_v1_to_v2(Arc::clone(&prefix), Arc::clone(&os)).await)
        .retry(
            ExponentialBuilder::new()
                .with_max_delay(Duration::from_secs(1))
                .with_max_times(5),
        )
        // Only certain errors can be retried
        .when(|e| e.is_retryable())
        .notify(|error, dur| warn!(?error, "Unable to migrate catalog; retrying in {dur:?}."))
        .await
        .context("Failed to migrate v1 to v2")?;

    // Find the log files that should be replayed after the checkpoint selected by the live
    // catalog prefix.
    let checkpoint_path = CatalogFilePath::checkpoint(&prefix);
    let checkpoint = match load_catalog_checkpoint(&checkpoint_path, store).await? {
        Some(checkpoint) => checkpoint,
        None => return Ok(None),
    };
    let sequence_start = checkpoint.sequence_number();
    let mut offset = CatalogFilePath::log(&prefix, sequence_start).into();
    let mut catalog_file_metas = Vec::new();
    loop {
        let mut list_items = os.list_with_offset(Some(&CatalogFilePath::dir(&prefix)), &offset);

        let mut objects = Vec::new();
        while let Some(item) = list_items.next().await {
            objects.push(item?);
        }
        if objects.is_empty() {
            break;
        }
        catalog_file_metas.append(&mut objects);
        catalog_file_metas.sort_unstable_by(|a, b| a.location.cmp(&b.location));

        if let Some(last) = catalog_file_metas.last() {
            offset = last.location.clone();
        } else {
            break;
        }
    }

    let log_paths = catalog_file_metas
        .into_iter()
        .map(|meta| meta.location)
        .collect::<Vec<_>>();

    load_catalog_from_paths_inner(checkpoint, log_paths, store).await
}

pub async fn load_catalog_from_paths(
    checkpoint_path: &ObjPath,
    log_paths: &[ObjPath],
    store: &ObjectStoreCatalog,
) -> Result<Option<InnerCatalog>> {
    let checkpoint = match load_catalog_checkpoint(checkpoint_path, store).await? {
        Some(checkpoint) => checkpoint,
        None => return Ok(None),
    };

    load_catalog_from_paths_inner(checkpoint, log_paths.to_vec(), store).await
}

async fn load_catalog_checkpoint(
    checkpoint_path: &ObjPath,
    store: &ObjectStoreCatalog,
) -> Result<Option<InnerCatalog>> {
    let os = store.object_store();
    match os.get(checkpoint_path).await {
        Ok(get_result) => {
            let bytes = get_result.bytes().await.context("Failed to read catalog")?;
            let snapshot = verify_and_deserialize_catalog_checkpoint_file(bytes).context(
                "there was a catalog checkpoint file on object store, but it \
                        could not be verified and deserialized",
            )?;
            Ok(Some(InnerCatalog::from_snapshot(snapshot)))
        }
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

async fn load_catalog_from_paths_inner(
    mut inner_catalog: InnerCatalog,
    mut log_paths: Vec<ObjPath>,
    store: &ObjectStoreCatalog,
) -> Result<Option<InnerCatalog>> {
    log_paths.sort_unstable();

    let mut futures = FuturesOrdered::from_iter(log_paths.into_iter().map(async |path| {
        let os = store.object_store();
        let bytes = os.get(&path).await?.bytes().await?;
        let catalog_file = verify_and_deserialize_catalog_file(bytes)?;
        Ok::<_, ObjectStoreCatalogError>((path, catalog_file))
    }));

    let mut catalog_files = Vec::new();
    while let Some(result) = futures.next().await {
        match result {
            Ok(entry) => catalog_files.push(entry),
            Err(ObjectStoreCatalogError::UpgradedLog) => {
                // NOTE: this log entry is not added to catalog_files,
                // and therefore does not update the sequence number of the
                // InnerCatalog. Any attempt to write another log entry will fail with
                // PersistCatalogResult::AlreadyExists, ensuring the v2 catalog can no
                // longer be changed once a v3 migration has stamped the upgrade log.
                //
                // Therefore, there should be no logs following this entry.
                inner_catalog.has_upgraded = true;
                break;
            }
            Err(err) => return Err(err),
        }
    }

    debug!(
        n_catalog_files = catalog_files.len(),
        "loaded catalog files since last checkpoint"
    );

    let mut expected_sequence = inner_catalog.sequence_number().next();
    for (path, ordered_catalog_batch) in catalog_files {
        let actual_sequence = ordered_catalog_batch.sequence_number();
        if actual_sequence != expected_sequence {
            return Err(ObjectStoreCatalogError::unexpected(format!(
                "catalog replay sequence mismatch for {path}: expected {}, got {}",
                expected_sequence.get(),
                actual_sequence.get(),
            )));
        }
        debug!(sequence = actual_sequence.get(), "processing catalog file");
        trace!(?ordered_catalog_batch, "processing catalog file");
        inner_catalog
            .apply_catalog_batch(
                ordered_catalog_batch.batch(),
                ordered_catalog_batch.sequence_number(),
                Some(store),
            )
            .context("failed to apply persisted catalog batch")?;
        expected_sequence = expected_sequence.next();
    }
    debug!("loaded the catalog");
    trace!(loaded_catalog = ?inner_catalog, "loaded the catalog");
    Ok(Some(inner_catalog))
}

pub fn verify_and_deserialize_catalog_file(bytes: Bytes) -> Result<log::OrderedCatalogBatch> {
    let version_id: &[u8; 10] = bytes
        .first_chunk()
        .ok_or(ObjectStoreCatalogError::unexpected(
            "file must contain at least 10 bytes",
        ))?;

    match *version_id {
        crate::log::versions::v4::OrderedCatalogBatch::VERSION_ID => {
            // V3 Deserialization:
            let checksum = bytes.slice(10..10 + CHECKSUM_LEN);
            let data = bytes.slice(10 + CHECKSUM_LEN..);
            versions::verify_checksum(&checksum, &data)?;
            let log =
                serde_json::from_slice::<crate::log::versions::v4::OrderedCatalogBatch>(&data)
                    .context("failed to deserialize v4 catalog log file contents")?;
            Ok(log)
        }
        crate::log::UpgradedLog::VERSION_ID => Err(ObjectStoreCatalogError::UpgradedLog),
        _ => Err(ObjectStoreCatalogError::unexpected(
            format! {"unrecognized catalog file format: {version_id:?}"},
        )),
    }
}

pub fn verify_and_deserialize_catalog_checkpoint_file(bytes: Bytes) -> Result<CatalogSnapshot> {
    let version_id: &[u8; 10] = bytes
        .first_chunk()
        .ok_or(ObjectStoreCatalogError::unexpected(
            "file must contain at least 10 bytes",
        ))?;

    match *version_id {
        crate::snapshot::versions::v4::CatalogSnapshot::VERSION_ID => {
            let checksum = bytes.slice(10..10 + CHECKSUM_LEN);
            let data = bytes.slice(10 + CHECKSUM_LEN..);
            versions::verify_checksum(&checksum, &data)?;
            let snapshot: crate::snapshot::versions::v4::CatalogSnapshot =
                serde_json::from_slice(&data)
                    .context("failed to deserialize v4 catalog snapshot file contents")?;
            Ok(snapshot)
        }
        _ => Err(ObjectStoreCatalogError::unexpected(
            "unrecognized catalog checkpoint file format",
        )),
    }
}

pub fn serialize_catalog_file<T: Serialize + VersionedFileType>(file: &T) -> Result<Bytes> {
    let mut buf = BytesMut::new();
    buf.extend_from_slice(&T::VERSION_ID);

    let data = serde_json::to_vec(file).context("failed to serialize catalog file")?;

    Ok(versions::hash_and_freeze(buf, data))
}

#[cfg(test)]
mod tests;
