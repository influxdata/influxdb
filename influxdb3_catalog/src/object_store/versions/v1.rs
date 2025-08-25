use std::ops::Deref;
use std::sync::Arc;

use anyhow::Context;
use bytes::Bytes;
use object_store::ObjectStore;
use object_store::PutOptions;
use object_store::path::Path as ObjPath;
use observability_deps::tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::catalog::versions::v1::{InnerCatalog, Snapshot};
use crate::snapshot::versions::v3::CatalogSnapshot;
use crate::{
    catalog::CatalogSequenceNumber,
    object_store::{CATALOG_LOG_FILE_EXTENSION, PersistCatalogResult, Result},
    serialize::versions::v1::{load_catalog, serialize_catalog_file},
};

#[derive(Debug, Clone)]
pub(crate) struct ObjectStoreCatalog {
    pub(crate) prefix: Arc<str>,
    /// PUT a checkpoint file to the object store every `checkpoint_interval` sequenced log files
    pub(crate) store: Arc<dyn ObjectStore>,
}

impl ObjectStoreCatalog {
    pub(crate) fn new(prefix: impl Into<Arc<str>>, store: Arc<dyn ObjectStore>) -> Self {
        Self {
            prefix: prefix.into(),
            store,
        }
    }

    /// Try loading the catalog, if there is no catalog generate new
    /// instance id and create a new catalog and persist it immediately
    pub(crate) async fn load_or_create_catalog(&self) -> Result<InnerCatalog> {
        match self.load_catalog().await? {
            Some(inner_catalog) => Ok(inner_catalog),
            None => {
                let catalog_uuid = Uuid::new_v4();
                info!(catalog_uuid = ?catalog_uuid, "catalog not found, creating a new one");
                let new_catalog = InnerCatalog::new(Arc::clone(&self.prefix), catalog_uuid);
                match self
                    .persist_catalog_checkpoint(&new_catalog.snapshot())
                    .await?
                {
                    PersistCatalogResult::Success => Ok(new_catalog),
                    PersistCatalogResult::AlreadyExists => {
                        self.load_catalog().await.map(|catalog| {
                            catalog.expect(
                                "the catalog should have already been persisted for us to load",
                            )
                        })
                    }
                }
            }
        }
    }

    /// Loads all catalog files from object store to build the catalog
    pub(crate) async fn load_catalog(&self) -> Result<Option<InnerCatalog>> {
        load_catalog(Arc::clone(&self.prefix), Arc::clone(&self.store))
            .await
            .context("failed to load catalog")
            .map_err(Into::into)
    }

    /// Persist the `CatalogSnapshot` as a checkpoint and ensure that the operation succeeds unless
    /// the file already exists. Object Storage should handle the concurrent requests in order and
    /// make sure that only one of them gets the win in terms of who writes first
    pub(crate) async fn persist_catalog_checkpoint(
        &self,
        snapshot: &CatalogSnapshot,
    ) -> Result<PersistCatalogResult> {
        let sequence = snapshot.sequence_number().get();
        let catalog_path = CatalogFilePath::checkpoint(&self.prefix);

        let content =
            serialize_catalog_file(snapshot).context("failed to serialize catalog snapshot")?;

        // NOTE: not sure if this should be done in a loop, i.e., what error variants from
        // the object store would warrant a retry.
        match self
            .store
            .put_opts(
                &catalog_path,
                content.clone().into(),
                PutOptions {
                    mode: object_store::PutMode::Create,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(put_result) => {
                info!(sequence, "persisted catalog checkpoint file");
                debug!(put_result = ?put_result, "object store PUT result");
                Ok(PersistCatalogResult::Success)
            }
            Err(object_store::Error::AlreadyExists { .. }) => {
                Ok(PersistCatalogResult::AlreadyExists)
            }
            Err(err) => {
                error!(error = ?err, "failed to persist catalog checkpoint file");
                Err(err.into())
            }
        }
    }

    pub(crate) async fn catalog_update_if_not_exists(
        &self,
        path: CatalogFilePath,
        content: Bytes,
    ) -> Result<PersistCatalogResult> {
        match self
            .store
            .put_opts(
                &path,
                content.into(),
                PutOptions {
                    mode: object_store::PutMode::Create,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(put_result) => {
                info!(?put_result, object_path = ?path, "persisted next catalog sequence");
                Ok(PersistCatalogResult::Success)
            }
            Err(object_store::Error::AlreadyExists { path, source }) => {
                debug!(
                    object_path = ?path,
                    source_error = ?source,
                    "catalog sequence already exists on object store"
                );
                Ok(PersistCatalogResult::AlreadyExists)
            }
            Err(other) => {
                // TODO: should we retry based on error type?
                warn!(error = ?other, "failed to put next catalog sequence into object store");
                Err(other.into())
            }
        }
    }

    /// Check if the catalog checkpoint file is present in this store, which would indicate that
    /// the catalog has been initialized, and is used to check if there is a Core catalog on server
    /// start.
    pub(crate) async fn checkpoint_exists(&self) -> Result<bool> {
        match self
            .store
            .head(&CatalogFilePath::checkpoint(&self.prefix))
            .await
        {
            Ok(_) => Ok(true),
            // nothing there, so we don't need to migrate:
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(error) => Err(error)?,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogFilePath(ObjPath);

impl CatalogFilePath {
    /// Catalog files are persisted as a monotonically increasing sequence of files, whose name
    /// is `<sequence>.catalog`; `<sequence>` is zero-padded to 20 characters to fit a `u64::MAX`
    pub fn log(catalog_prefix: &str, catalog_sequence_number: CatalogSequenceNumber) -> Self {
        let num = catalog_sequence_number.get();
        let path = ObjPath::from(format!(
            "{catalog_prefix}/catalogs/{num:020}.{CATALOG_LOG_FILE_EXTENSION}",
        ));
        Self(path)
    }

    /// The Catalog checkpoint file is periodically persisted to object store at the same
    /// location
    pub fn checkpoint(catalog_prefix: &str) -> Self {
        let path = ObjPath::from(format!("{catalog_prefix}/_catalog_checkpoint",));
        Self(path)
    }

    pub fn dir(host_prefix: &str) -> Self {
        Self(ObjPath::from(format!("{host_prefix}/catalogs")))
    }
}

impl Deref for CatalogFilePath {
    type Target = ObjPath;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<ObjPath> for CatalogFilePath {
    fn as_ref(&self) -> &ObjPath {
        &self.0
    }
}

impl From<CatalogFilePath> for ObjPath {
    fn from(path: CatalogFilePath) -> Self {
        path.0
    }
}
