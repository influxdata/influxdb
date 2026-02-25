use anyhow::Context;
use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use futures::stream::FuturesOrdered;
use object_store::ObjectStore;
use observability_deps::tracing::{debug, trace};
use serde::Serialize;
use std::sync::Arc;

type Result<T> = std::result::Result<T, ObjectStoreCatalogError>;

use crate::catalog::versions::v1::{
    InnerCatalog, Snapshot,
    log::{self},
    snapshot::CatalogSnapshot,
};
use crate::object_store::ObjectStoreCatalogError;
use crate::object_store::versions::v1::CatalogFilePath;
use crate::serialize::{VersionedFileType, versions};

const CHECKSUM_LEN: usize = size_of::<u32>();

pub async fn load_catalog(
    prefix: Arc<str>,
    store: Arc<dyn ObjectStore>,
) -> Result<Option<InnerCatalog>> {
    // get the checkpoint to initialize the catalog:
    let mut inner_catalog = match store.get(&CatalogFilePath::checkpoint(&prefix)).await {
        Ok(get_result) => {
            let bytes = get_result.bytes().await.context("Failed to read catalog")?;
            let snapshot = verify_and_deserialize_catalog_checkpoint_file(bytes).context(
                "there was a catalog checkpoint file on object store, but it \
                        could not be verified and deserialized",
            )?;
            InnerCatalog::from_snapshot(snapshot)
        }
        // there should always be a checkpoint if the server and catalog was successfully
        // initialized:
        Err(object_store::Error::NotFound { .. }) => return Ok(None),
        Err(error) => return Err(error.into()),
    };

    // fetch catalog files after the checkpoint
    //
    // catalog files are listed with offset and sorted for every group of listed items that
    // are added in the loop iteration; ideally, only one iteration would be needed if the
    // catalog is checkpointed frequently enough.
    let sequence_start = inner_catalog.sequence_number();
    let mut offset = CatalogFilePath::log(&prefix, sequence_start).into();
    let mut catalog_file_metas = Vec::new();
    loop {
        let mut list_items = store.list_with_offset(Some(&CatalogFilePath::dir(&prefix)), &offset);

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

    let mut futures = FuturesOrdered::from_iter(catalog_file_metas.into_iter().map(async |meta| {
        let bytes = store.get(&meta.location).await?.bytes().await?;
        verify_and_deserialize_catalog_file(bytes)
    }));

    let mut catalog_files = Vec::new();
    while let Some(result) = futures.next().await {
        match result {
            Ok(log) => catalog_files.push(log),
            Err(ObjectStoreCatalogError::UpgradedLog) => {
                // NOTE: this log entry is not added to catalog_files,
                // and therefore does not update the sequence number of the
                // InnerCatalog. Any attempt to write another log entry will fail with
                // PersistCatalogResult::AlreadyExists, ensuring the v1 catalog can no longer be
                // changed.
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

    for ordered_catalog_batch in catalog_files {
        debug!(
            sequence = ordered_catalog_batch.sequence_number().get(),
            "processing catalog file"
        );
        trace!(?ordered_catalog_batch, "processing catalog file");
        inner_catalog
            .apply_catalog_batch(
                ordered_catalog_batch.batch(),
                ordered_catalog_batch.sequence_number(),
            )
            .context("failed to apply persisted catalog batch")?;
    }
    debug!("loaded the catalog");
    trace!(loaded_catalog = ?inner_catalog, "loaded the catalog");
    Ok(Some(inner_catalog))
}

pub(crate) fn verify_and_deserialize_catalog_file(
    bytes: Bytes,
) -> Result<log::OrderedCatalogBatch> {
    let version_id: &[u8; 10] = bytes
        .first_chunk()
        .ok_or(ObjectStoreCatalogError::unexpected(
            "file must contain at least 10 bytes",
        ))?;

    match *version_id {
        // Version 1 uses the `bitcode` crate for serialization/deserialization
        crate::log::versions::v1::OrderedCatalogBatch::VERSION_ID => {
            // V1 Deserialization:
            let checksum = bytes.slice(10..10 + CHECKSUM_LEN);
            let data = bytes.slice(10 + CHECKSUM_LEN..);
            versions::verify_checksum(&checksum, &data)?;
            let log = bitcode::deserialize::<crate::log::versions::v1::OrderedCatalogBatch>(&data)
                .context("failed to deserialize v1 catalog log file contents")?;

            // explicit type annotations are needed once you start chaining `.into` (something to check
            // later to see if that could be avoided, then this can just be a loop with starting and
            // end point based on current log file's version)
            let log_v2: crate::log::versions::v2::OrderedCatalogBatch = log.into();
            let log_v3: crate::log::versions::v3::OrderedCatalogBatch = log_v2.into();
            Ok(log_v3)
        }
        // Version 2 uses the `serde_json` crate for serialization/deserialization
        crate::log::versions::v2::OrderedCatalogBatch::VERSION_ID => {
            // V2 Deserialization:
            let checksum = bytes.slice(10..10 + CHECKSUM_LEN);
            let data = bytes.slice(10 + CHECKSUM_LEN..);
            versions::verify_checksum(&checksum, &data)?;
            let log =
                serde_json::from_slice::<crate::log::versions::v2::OrderedCatalogBatch>(&data)
                    .context("failed to deserialize v2 catalog log file contents")?;
            Ok(log.into())
        }
        // Version 3 added a conversion function to map db:*:write tokens to be db:*:create,write
        // tokens, and because it's a one time migration it relies on the version in file the
        // version has to be updated.
        crate::log::versions::v3::OrderedCatalogBatch::VERSION_ID => {
            // V3 Deserialization:
            let checksum = bytes.slice(10..10 + CHECKSUM_LEN);
            let data = bytes.slice(10 + CHECKSUM_LEN..);
            versions::verify_checksum(&checksum, &data)?;
            let log =
                serde_json::from_slice::<crate::log::versions::v3::OrderedCatalogBatch>(&data)
                    .context("failed to deserialize v3 catalog log file contents")?;
            Ok(log)
        }
        crate::log::UpgradedLog::VERSION_ID => Err(ObjectStoreCatalogError::UpgradedLog),
        _ => Err(ObjectStoreCatalogError::unexpected(
            "unrecognized catalog file format",
        )),
    }
}

pub(crate) fn verify_and_deserialize_catalog_checkpoint_file(
    bytes: Bytes,
) -> Result<CatalogSnapshot> {
    let version_id: &[u8; 10] = bytes
        .first_chunk()
        .ok_or(ObjectStoreCatalogError::unexpected(
            "file must contain at least 10 bytes",
        ))?;

    match *version_id {
        // Version 1 uses the `bitcode` crate for serialization/deserialization
        crate::snapshot::versions::v1::CatalogSnapshot::VERSION_ID => {
            let checksum = bytes.slice(10..10 + CHECKSUM_LEN);
            let data = bytes.slice(10 + CHECKSUM_LEN..);
            versions::verify_checksum(&checksum, &data)?;
            let snapshot =
                bitcode::deserialize::<crate::snapshot::versions::v1::CatalogSnapshot>(&data);
            let snapshot =
                snapshot.context("failed to deserialize v1 catalog snapshot file contents")?;
            let snapshot_v2: crate::snapshot::versions::v2::CatalogSnapshot = snapshot.into();
            let snapshot_v3: crate::snapshot::versions::v3::CatalogSnapshot = snapshot_v2.into();
            Ok(snapshot_v3)
        }
        // Version 2 uses the `serde_json` crate for serialization/deserialization
        crate::snapshot::versions::v2::CatalogSnapshot::VERSION_ID => {
            let checksum = bytes.slice(10..10 + CHECKSUM_LEN);
            let data = bytes.slice(10 + CHECKSUM_LEN..);
            versions::verify_checksum(&checksum, &data)?;
            let snapshot: crate::snapshot::versions::v2::CatalogSnapshot =
                serde_json::from_slice(&data)
                    .context("failed to deserialize v2 catalog snapshot file contents")?;
            Ok(snapshot.into())
        }
        // Version 3 added a conversion function to map db:*:write tokens to be db:*:create,write
        // tokens, and because it's a one time migration it relies on the version in file the
        // version has to be updated.
        crate::snapshot::versions::v3::CatalogSnapshot::VERSION_ID => {
            let checksum = bytes.slice(10..10 + CHECKSUM_LEN);
            let data = bytes.slice(10 + CHECKSUM_LEN..);
            versions::verify_checksum(&checksum, &data)?;
            let snapshot: crate::snapshot::versions::v3::CatalogSnapshot =
                serde_json::from_slice(&data)
                    .context("failed to deserialize v3 catalog snapshot file contents")?;
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

    let data = match T::VERSION_ID {
        crate::snapshot::versions::v1::CatalogSnapshot::VERSION_ID
        | crate::log::versions::v1::OrderedCatalogBatch::VERSION_ID => {
            bitcode::serialize(file).context("failed to serialize catalog file")?
        }
        _ => serde_json::to_vec(file).context("failed to serialize catalog file")?,
    };

    Ok(versions::hash_and_freeze(buf, data))
}

#[cfg(test)]
mod v1_tests;
