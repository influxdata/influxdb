use std::ops::Deref;
use std::sync::Arc;

use anyhow::Context;
use bytes::Bytes;
use futures::{StreamExt, stream::FuturesOrdered};
use object_store::ObjectStore;
use object_store::{PutOptions, path::Path as ObjPath};
use observability_deps::tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::catalog::InnerCatalog;
use crate::serialize::verify_and_deserialize_catalog_checkpoint_file;
use crate::{
    catalog::CatalogSequenceNumber,
    log::OrderedCatalogBatch,
    serialize::{
        serialize_catalog_log, serialize_catalog_snapshot, verify_and_deserialize_catalog_file,
    },
    snapshot::CatalogSnapshot,
};

#[derive(Debug, thiserror::Error)]
pub enum ObjectStoreCatalogError {
    #[error("object store error: {0:?}")]
    ObjectStore(#[from] object_store::Error),

    #[error("unexpected error: {0:?}")]
    Unexpected(#[from] anyhow::Error),
}

type Result<T, E = ObjectStoreCatalogError> = std::result::Result<T, E>;

#[derive(Debug, Clone)]
pub struct ObjectStoreCatalog {
    pub(crate) prefix: Arc<str>,
    /// PUT a checkpoint file to the object store every `checkpoint_interval` sequenced log files
    pub(crate) checkpoint_interval: u64,
    store: Arc<dyn ObjectStore>,
}

impl ObjectStoreCatalog {
    pub(crate) fn new(
        prefix: impl Into<Arc<str>>,
        checkpoint_interval: u64,
        store: Arc<dyn ObjectStore>,
    ) -> Self {
        Self {
            prefix: prefix.into(),
            checkpoint_interval,
            store,
        }
    }

    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.store)
    }

    /// Try loading the catalog, if there is no catalog generate new
    /// instance id and create a new catalog and persist it immediately
    pub async fn load_or_create_catalog(&self) -> Result<InnerCatalog> {
        match self.load_catalog().await? {
            Some(inner_catalog) => Ok(inner_catalog),
            None => {
                let catalog_uuid = Uuid::new_v4();
                info!(catalog_uuid = ?catalog_uuid, "catalog not found, creating a new one");
                let new_catalog = InnerCatalog::new(Arc::clone(&self.prefix), catalog_uuid);
                self.persist_catalog_checkpoint(&CatalogSnapshot::from(&new_catalog))
                    .await?;
                Ok(new_catalog)
            }
        }
    }

    /// Loads all catalog files from object store to build the catalog
    pub async fn load_catalog(&self) -> Result<Option<InnerCatalog>> {
        // get the checkpoint to initialize the catalog:
        let mut inner_catalog = match self
            .store
            .get(&CatalogFilePath::checkpoint(&self.prefix))
            .await
        {
            Ok(get_result) => {
                let bytes = get_result.bytes().await?;
                let snapshot = verify_and_deserialize_catalog_checkpoint_file(bytes).context(
                    "there was a catalog checkpoint file on object store, but it \
                        could not be verified and deserialized",
                )?;
                InnerCatalog::from(snapshot)
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
        let mut offset = CatalogFilePath::log(&self.prefix, sequence_start).into();
        let mut catalog_file_metas = Vec::new();
        loop {
            let mut list_items = self
                .store
                .list_with_offset(Some(&CatalogFilePath::dir(&self.prefix)), &offset);

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

        let mut futures = FuturesOrdered::new();
        async fn get_file(
            location: ObjPath,
            object_store: Arc<dyn ObjectStore>,
        ) -> Result<OrderedCatalogBatch> {
            let bytes = object_store.get(&location).await?.bytes().await?;
            verify_and_deserialize_catalog_file(bytes)
                .context("failed to deserialize catalog file")
                .map_err(Into::into)
        }

        for meta in catalog_file_metas {
            futures.push_back(get_file(meta.location, Arc::clone(&self.store)));
        }

        let mut catalog_files = Vec::new();
        while let Some(result) = futures.next().await {
            catalog_files.push(result?);
        }

        debug!(
            n_catalog_files = catalog_files.len(),
            "loaded catalog files since last checkpoint"
        );

        for ordered_catalog_batch in catalog_files {
            debug!(?ordered_catalog_batch, "processing catalog file");
            inner_catalog
                .apply_catalog_batch(
                    ordered_catalog_batch.batch(),
                    ordered_catalog_batch.sequence_number(),
                )
                .context("failed to apply persisted catalog batch")?;
        }
        debug!(loaded_catalog = ?inner_catalog, "loaded the catalog");
        Ok(Some(inner_catalog))
    }

    pub async fn load_catalog_sequenced_log(
        &self,
        sequence_number: CatalogSequenceNumber,
    ) -> Result<Option<OrderedCatalogBatch>> {
        debug!(
            sequence_number = sequence_number.get(),
            "load sequenced catalog file",
        );
        let catalog_path = CatalogFilePath::log(&self.prefix, sequence_number);
        match self.store.get(&catalog_path).await {
            Ok(get_result) => {
                let bytes = get_result.bytes().await?;
                Ok(Some(verify_and_deserialize_catalog_file(bytes).context(
                    "failed to verify and deserialize next catalog file",
                )?))
            }
            Err(object_store::Error::NotFound { .. }) => {
                debug!("catalog sequence not found");
                Ok(None)
            }
            Err(error) => {
                warn!(?error, "error when fetching next catalog sequenced log");
                Err(error.into())
            }
        }
    }

    pub(crate) async fn persist_catalog_sequenced_log(
        &self,
        batch: &OrderedCatalogBatch,
    ) -> Result<PersistCatalogResult> {
        let catalog_path = CatalogFilePath::log(&self.prefix, batch.sequence_number());

        let content = serialize_catalog_log(batch).context("failed to serialize catalog batch")?;

        self.catalog_update_if_not_exists(catalog_path, content)
            .await
    }

    /// Persist the `CatalogSnapshot` as a checkpoint and ensure that the operation succeeds
    pub(crate) async fn persist_catalog_checkpoint(
        &self,
        snapshot: &CatalogSnapshot,
    ) -> Result<()> {
        let sequence = snapshot.sequence_number().get();
        let catalog_path = CatalogFilePath::checkpoint(&self.prefix);

        let content =
            serialize_catalog_snapshot(snapshot).context("failed to serialize catalog snapshot")?;

        // NOTE: not sure if this should be done in a loop, i.e., what error variants from
        // the object store would warrant a retry.
        match self.store.put(&catalog_path, content.clone().into()).await {
            Ok(put_result) => {
                info!(sequence, "persisted catalog checkpoint file");
                debug!(put_result = ?put_result, "object store PUT result");
                return Ok(());
            }
            Err(object_store::Error::NotModified { .. }) => {
                return Ok(());
            }
            Err(err) => {
                error!(error = ?err, "failed to persist catalog checkpoint file");
                return Err(err.into());
            }
        }
    }

    /// Persist the `CatalogSnapshot` as a checkpoint in the background but don't check that the
    /// operation succeeds.
    pub(crate) fn background_persist_catalog_checkpoint(
        &self,
        snapshot: &CatalogSnapshot,
    ) -> Result<()> {
        let sequence = snapshot.sequence_number().get();
        debug!(sequence, "background persist of catalog checkpoint");
        let catalog_path = CatalogFilePath::checkpoint(&self.prefix);

        let content =
            serialize_catalog_snapshot(snapshot).context("failed to serialize catalog snapshot")?;

        let store = Arc::clone(&self.store);

        tokio::spawn(async move {
            // NOTE: not sure if this should be done in a loop, i.e., what error variants from
            // the object store would warrant a retry.
            match store.put(&catalog_path, content.clone().into()).await {
                Ok(put_result) => {
                    info!(sequence, "persisted catalog checkpoint file");
                    debug!(put_result = ?put_result, "object store PUT result");
                }
                Err(object_store::Error::NotModified { .. }) => {}
                Err(err) => {
                    error!(error = ?err, "failed to persist catalog checkpoint file");
                }
            }
        });

        Ok(())
    }

    async fn catalog_update_if_not_exists(
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
}

#[derive(Debug, Copy, Clone)]
pub enum PersistCatalogResult {
    Success,
    AlreadyExists,
}

/// File extension for catalog files
pub const CATALOG_LOG_FILE_EXTENSION: &str = "catalog";

/// File extension for catalog files
pub const CATALOG_SNAPSHOT_FILE_EXTENSION: &str = "catalog.snapshot";

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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::local::LocalFileSystem;

    use crate::object_store::ObjectStoreCatalog;

    #[test_log::test(tokio::test)]
    async fn load_or_create_catalog_new_catalog() {
        let local_disk =
            LocalFileSystem::new_with_prefix(test_helpers::tmp_dir().unwrap()).unwrap();
        let store = ObjectStoreCatalog::new("test_host", 10, Arc::new(local_disk));
        let _ = store.load_or_create_catalog().await.unwrap();
        assert!(store.load_catalog().await.unwrap().is_some());
    }

    #[test_log::test(tokio::test)]
    async fn load_or_create_catalog_existing_catalog() {
        let local_disk =
            LocalFileSystem::new_with_prefix(test_helpers::tmp_dir().unwrap()).unwrap();
        let store = ObjectStoreCatalog::new("test_host", 10, Arc::new(local_disk));
        let expected = store.load_or_create_catalog().await.unwrap().catalog_id;

        // initialize again to ensure it uses the same as above
        let actual = store.load_or_create_catalog().await.unwrap().catalog_id;

        // check the instance ids match
        assert_eq!(expected, actual);
    }

    // TODO: more test cases
    // - check that only files until most recent snapshot are laoded on start
    // - verify all catalog ops can be serialized and deserialized

    // NOTE(trevor/catalog-refactor): the below three tests are all from the persister module in
    // influxdb3_write, which I am putting here to preserve, but will need to all be replaced/refactored.

    // NOTE(trevor/catalog-refactor): this used the old persist_catalog method, so needs to be refactored.
    // This test also doesn't really do anything other than check via unwrap that things dont fail
    #[tokio::test]
    #[ignore = "this used the old persist_catalog method, so needs to be refactored"]
    async fn persist_catalog() {
        unimplemented!();
        // let node_id = Arc::from("sample-host-id");
        // let instance_id = Arc::from("sample-instance-id");
        // let local_disk =
        //     LocalFileSystem::new_with_prefix(test_helpers::tmp_dir().unwrap()).unwrap();
        // let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        // let persister = Persister::new(
        //     Arc::new(local_disk),
        //     "test_host",
        //     Arc::clone(&time_provider) as _,
        // );
        // let catalog = Catalog::new(node_id, instance_id);
        // let _ = catalog.db_or_create("my_db");

        // persister.persist_catalog(&catalog).await.unwrap();
    }

    // NOTE(trevor/catalog-refactor): this used the old persist_catalog method, so needs to be refactored.
    // old catalog cleanup might not be necessary with the way the new catalog is persisted as a
    // log, and only snapshotting semi-regularly
    #[tokio::test]
    #[ignore = "this used the old persist_catalog method, so needs to be refactored"]
    async fn persist_catalog_with_cleanup() {
        unimplemented!();
        // let node_id = Arc::from("sample-host-id");
        // let instance_id = Arc::from("sample-instance-id");
        // let prefix = test_helpers::tmp_dir().unwrap();
        // let local_disk = LocalFileSystem::new_with_prefix(prefix).unwrap();
        // let obj_store: Arc<dyn ObjectStore> = Arc::new(local_disk);
        // let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        // let persister = Persister::new(Arc::clone(&obj_store), "test_host", time_provider);
        // let catalog = Catalog::new(Arc::clone(&node_id), instance_id);
        // persister.persist_catalog(&catalog).await.unwrap();
        // let db_schema = catalog.db_or_create("my_db_1").unwrap();
        // persister.persist_catalog(&catalog).await.unwrap();
        // let _ = catalog.db_or_create("my_db_2").unwrap();
        // persister.persist_catalog(&catalog).await.unwrap();
        // let _ = catalog.db_or_create("my_db_3").unwrap();
        // persister.persist_catalog(&catalog).await.unwrap();
        // let _ = catalog.db_or_create("my_db_4").unwrap();
        // persister.persist_catalog(&catalog).await.unwrap();
        // let _ = catalog.db_or_create("my_db_5").unwrap();
        // persister.persist_catalog(&catalog).await.unwrap();

        // let batch = |name: &str, num: u32| {
        //     let _ = catalog.apply_catalog_batch(&CatalogBatch {
        //         database_id: db_schema.id,
        //         database_name: Arc::clone(&db_schema.name),
        //         time_ns: 5000,
        //         ops: vec![CatalogOp::CreateTable(CreateTableLog {
        //             database_id: db_schema.id,
        //             database_name: Arc::clone(&db_schema.name),
        //             table_name: name.into(),
        //             table_id: TableId::from(num),
        //             field_definitions: vec![FieldDefinition {
        //                 name: "column".into(),
        //                 id: ColumnId::from(num),
        //                 data_type: FieldDataType::String,
        //             }],
        //             key: vec![num.into()],
        //         })],
        //     });
        // };

        // batch("table_zero", 0);
        // persister.persist_catalog(&catalog).await.unwrap();
        // batch("table_one", 1);
        // persister.persist_catalog(&catalog).await.unwrap();
        // batch("table_two", 2);
        // persister.persist_catalog(&catalog).await.unwrap();
        // batch("table_three", 3);
        // persister.persist_catalog(&catalog).await.unwrap();

        // // We've persisted the catalog 10 times and nothing has changed
        // // So now we need to persist the catalog two more times and we should
        // // see the first 2 catalogs be dropped.
        // batch("table_four", 4);
        // persister.persist_catalog(&catalog).await.unwrap();
        // batch("table_five", 5);
        // persister.persist_catalog(&catalog).await.unwrap();

        // // Make sure the deletions have all ocurred
        // sleep(Duration::from_secs(2)).await;

        // let mut stream = obj_store.list(None);
        // let mut items = Vec::new();
        // while let Some(item) = stream.next().await {
        //     items.push(item.unwrap());
        // }

        // // Sort by oldest fisrt
        // items.sort_by(|a, b| b.location.cmp(&a.location));

        // assert_eq!(items.len(), 10);
        // // The first path should contain this number meaning we've
        // // eliminated the first two items
        // assert_eq!(18446744073709551613, u64::MAX - 2);

        // // Assert that we have 10 catalogs of decreasing number
        // assert_eq!(
        //     items[0].location,
        //     "test_host/catalogs/18446744073709551613.catalog".into()
        // );
        // assert_eq!(
        //     items[1].location,
        //     "test_host/catalogs/18446744073709551612.catalog".into()
        // );
        // assert_eq!(
        //     items[2].location,
        //     "test_host/catalogs/18446744073709551611.catalog".into()
        // );
        // assert_eq!(
        //     items[3].location,
        //     "test_host/catalogs/18446744073709551610.catalog".into()
        // );
        // assert_eq!(
        //     items[4].location,
        //     "test_host/catalogs/18446744073709551609.catalog".into()
        // );
        // assert_eq!(
        //     items[5].location,
        //     "test_host/catalogs/18446744073709551608.catalog".into()
        // );
        // assert_eq!(
        //     items[6].location,
        //     "test_host/catalogs/18446744073709551607.catalog".into()
        // );
        // assert_eq!(
        //     items[7].location,
        //     "test_host/catalogs/18446744073709551606.catalog".into()
        // );
        // assert_eq!(
        //     items[8].location,
        //     "test_host/catalogs/18446744073709551605.catalog".into()
        // );
        // assert_eq!(
        //     items[9].location,
        //     "test_host/catalogs/18446744073709551604.catalog".into()
        // );
    }

    // NOTE(trevor/catalog-refactor): this used the old persist_catalog method, so needs to be refactored.
    #[tokio::test]
    #[ignore = "this used the old persist_catalog method, so needs to be refactored"]
    async fn persist_and_load_newest_catalog() {
        unimplemented!();
        // let node_id: Arc<str> = Arc::from("sample-host-id");
        // let instance_id: Arc<str> = Arc::from("sample-instance-id");
        // let local_disk =
        //     LocalFileSystem::new_with_prefix(test_helpers::tmp_dir().unwrap()).unwrap();
        // let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        // let persister = Persister::new(Arc::new(local_disk), "test_host", time_provider);
        // let catalog = Catalog::new(Arc::clone(&node_id), Arc::clone(&instance_id));
        // let _ = catalog.db_or_create("my_db");

        // persister.persist_catalog(&catalog).await.unwrap();

        // let catalog = Catalog::new(Arc::clone(&node_id), Arc::clone(&instance_id));
        // let _ = catalog.db_or_create("my_second_db");

        // persister.persist_catalog(&catalog).await.unwrap();

        // let catalog = persister
        //     .load_catalog()
        //     .await
        //     .expect("loading the catalog did not cause an error")
        //     .expect("there was a catalog to load");

        // // my_second_db
        // assert!(catalog.db_exists(DbId::from(1)));
        // // my_db
        // assert!(!catalog.db_exists(DbId::from(0)));
    }
}
