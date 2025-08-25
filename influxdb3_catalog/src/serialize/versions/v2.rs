use anyhow::Context;
use backon::{ExponentialBuilder, Retryable};
use bytes::{Bytes, BytesMut};
use futures::stream::FuturesOrdered;
use futures::{StreamExt, TryStreamExt};
use object_store::ObjectStore;
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
use crate::object_store::ObjectStoreCatalogError;
use crate::serialize::{VersionedFileType, versions};

use crate::catalog::migrations::check_and_migrate_v1_to_v2;
use crate::object_store::versions::v2::CatalogFilePath;

const CHECKSUM_LEN: usize = size_of::<u32>();

pub async fn load_catalog(
    prefix: Arc<str>,
    store: Arc<dyn ObjectStore>,
) -> Result<Option<InnerCatalog>> {
    (async || check_and_migrate_v1_to_v2(Arc::clone(&prefix), Arc::clone(&store)).await)
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

    let catalog_files =
        FuturesOrdered::from_iter(catalog_file_metas.into_iter().map(async |meta| {
            let bytes = store.get(&meta.location).await?.bytes().await?;
            verify_and_deserialize_catalog_file(bytes)
        }))
        .try_collect::<Vec<_>>()
        .await
        .context("failed to deserialize catalog file")?;

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
