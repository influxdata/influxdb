use chrono::{DateTime, Duration, Utc};
use iox_catalog::interface::{Catalog, ParquetFileRepo};
use object_store::ObjectMeta;
use observability_deps::tracing::*;
use snafu::prelude::*;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::timeout;
use uuid::Uuid;

#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum Error {
    #[snafu(display("Expected a file name"))]
    FileNameMissing,

    #[snafu(display("Channel closed unexpectedly"))]
    ChannelClosed,

    #[snafu(display("The catalog could not be queried for {object_store_id}"))]
    GetFile {
        source: iox_catalog::interface::Error,
        object_store_id: uuid::Uuid,
    },

    #[snafu(display("The catalog could not be queried for the batch"))]
    FileExists {
        source: iox_catalog::interface::Error,
    },

    #[snafu(display("The deleter task exited unexpectedly"))]
    DeleterExited {
        source: tokio::sync::mpsc::error::SendError<ObjectMeta>,
    },
}

/// The number of parquet files we will ask the catalog to look for at once.
// todo(pjb): I have no idea what's a good value here to amortize the request. More than 1 is a start.
// Here's the idea: group everything you can for 100ms `RECEIVE_TIMEOUT`, because that's not so much
// of a delay that it would cause issues, but if you manage to get a huge number of file ids, stop
// accumulating at 100 `CATALOG_BATCH_SIZE`.
const CATALOG_BATCH_SIZE: usize = 100;
const RECEIVE_TIMEOUT: core::time::Duration = core::time::Duration::from_millis(100); // This may not be long enough to collect many objects.

pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;

pub(crate) async fn perform(
    catalog: Arc<dyn Catalog>,
    cutoff: Duration,
    items: mpsc::Receiver<ObjectMeta>,
    deleter: mpsc::Sender<ObjectMeta>,
) -> Result<()> {
    let mut repositories = catalog.repositories().await;
    let parquet_files = repositories.parquet_files();

    perform_inner(parquet_files, cutoff, items, deleter).await
}

/// Allows easier mocking of just `ParquetFileRepo` in tests.
async fn perform_inner(
    parquet_files: &mut dyn ParquetFileRepo,
    cutoff: Duration,
    mut items: mpsc::Receiver<ObjectMeta>,
    deleter: mpsc::Sender<ObjectMeta>,
) -> Result<()> {
    let mut batch = Vec::with_capacity(CATALOG_BATCH_SIZE);
    loop {
        let maybe_item = timeout(RECEIVE_TIMEOUT, items.recv()).await;

        // if we have an error, we timed out.
        let timedout = maybe_item.is_err();
        if let Ok(res) = maybe_item {
            match res {
                Some(item) => {
                    batch.push(item);
                }
                None => {
                    // The channel has been closed unexpectedly
                    return Err(Error::ChannelClosed);
                }
            }
        };

        if batch.len() >= CATALOG_BATCH_SIZE || timedout {
            let older_than = chrono::offset::Utc::now() - cutoff;
            for item in should_delete(batch, older_than, parquet_files).await {
                deleter.send(item).await.context(DeleterExitedSnafu)?;
            }
            batch = Vec::with_capacity(100);
        }
    }
}

/// [should_delete] processes a list of object store file information to see if the object for this
/// [ObjectMeta] can be deleted.
/// It can be deleted if it is old enough AND there isn't a reference in the catalog for it anymore (or ever)
/// It will also say the file can be deleted if it isn't a parquet file or the uuid isn't valid.
/// [should_delete] returns a subset of the input, which are the items that "should" be deleted.
// It first processes the easy checks, age, uuid, file suffix, and other parse/data input errors. This
// checking is cheap. For the files that need to be checked against the catalog, it batches them to
// reduce the number of requests on the wire and amortize the catalog overhead. Setting the batch size
// to 1 will return this method to its previous behavior (1 request per file) and resource usage.
async fn should_delete(
    items: Vec<ObjectMeta>,
    cutoff: DateTime<Utc>,
    parquet_files: &mut dyn ParquetFileRepo,
) -> Vec<ObjectMeta> {
    // to_delete is the vector we will return to the caller containing ObjectMeta we think should be deleted.
    // it is never longer than `items`
    let mut to_delete = Vec::with_capacity(items.len());
    // After filtering out potential errors and non-parquet files, this vector accumulates the objects
    // that need to be checked against the catalog to see if we can delete them.
    let mut to_check_in_catalog = Vec::with_capacity(items.len());

    for candidate in items {
        if cutoff < candidate.last_modified {
            // expected to be a common reason to skip a file
            debug!(
                location = %candidate.location,
                deleting = false,
                reason = "too new",
                cutoff = %cutoff,
                last_modified = %candidate.last_modified,
                "Ignoring object",
            );
            // Not old enough; do not delete
            continue;
        }

        let file_name = candidate.location.parts().last();
        if file_name.is_none() {
            warn!(
                location = %candidate.location,
                deleting = true,
                reason = "bad location",
                "Ignoring object",
            );
            // missing file name entirely! likely not a valid object store file entry
            // skip it
            continue;
        }

        // extract the file suffix, delete it if it isn't a parquet file
        if let Some(uuid) = file_name.unwrap().as_ref().strip_suffix(".parquet") {
            if let Ok(object_store_id) = uuid.parse::<Uuid>() {
                // add it to the list to check against the catalog
                // push a tuple that maps the uuid to the object meta struct so we don't have generate the uuid again
                to_check_in_catalog.push((object_store_id, candidate))
            } else {
                // expected to be a rare situation so warn.
                warn!(
                    location = %candidate.location,
                    deleting = true,
                    uuid,
                    reason = "not a valid UUID",
                    "Scheduling file for deletion",
                );
                to_delete.push(candidate)
            }
        } else {
            // expected to be a rare situation so warn.
            warn!(
                location = %candidate.location,
                deleting = true,
                reason = "not a .parquet file",
                "Scheduling file for deletion",
            );
            to_delete.push(candidate)
        }
    }

    // do_not_delete contains the items that are present in the catalog
    let mut do_not_delete: HashSet<Uuid> = HashSet::with_capacity(to_check_in_catalog.len());
    for batch in to_check_in_catalog.chunks(CATALOG_BATCH_SIZE) {
        let just_uuids: Vec<_> = batch.iter().map(|id| id.0).collect();
        match check_ids_exists_in_catalog(just_uuids.clone(), parquet_files).await {
            Ok(present_uuids) => {
                do_not_delete.extend(present_uuids.iter());
            }
            Err(e) => {
                // on error assume all the uuids in this batch are present in the catalog
                do_not_delete.extend(just_uuids.iter());
                warn!(
                    error = %e,
                    reason = "error querying catalog",
                    "Ignoring batch and continuing",
                );
            }
        }
    }

    if enabled!(Level::DEBUG) {
        do_not_delete.iter().for_each(|uuid| {
            debug!(
                deleting = false,
                uuid = %uuid,
                reason = "Object is present in catalog, not deleting",
                "Ignoring object",
            )
        });
    }

    // we have a Vec of uuids for the files we _do not_ want to delete (present in the catalog)
    // remove these uuids from the Vec of all uuids we checked, adding the remainder to the delete list
    to_check_in_catalog
        .iter()
        .filter(|c| !do_not_delete.contains(&c.0))
        .for_each(|c| to_delete.push(c.1.clone()));

    to_delete
}

/// helper to check a batch of ids for presence in the catalog.
/// returns a list of the ids (from the original batch) that exist (or catalog error).
async fn check_ids_exists_in_catalog(
    candidates: Vec<Uuid>,
    parquet_files: &mut dyn ParquetFileRepo,
) -> Result<Vec<Uuid>> {
    parquet_files
        .exists_by_object_store_id_batch(candidates)
        .await
        .context(FileExistsSnafu)
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use data_types::{
        ColumnId, ColumnSet, CompactionLevel, NamespaceId, ParquetFile, ParquetFileId,
        ParquetFileParams, PartitionId, TableId, Timestamp, TransitionPartitionId,
    };
    use iox_catalog::{
        interface::Catalog,
        mem::MemCatalog,
        test_helpers::{arbitrary_namespace, arbitrary_table},
    };
    use object_store::path::Path;
    use once_cell::sync::Lazy;
    use parquet_file::ParquetFilePath;
    use std::{assert_eq, vec};
    use uuid::Uuid;

    static OLDER_TIME: Lazy<DateTime<Utc>> = Lazy::new(|| {
        DateTime::parse_from_str("2022-01-01T00:00:00z", "%+")
            .unwrap()
            .naive_utc()
            .and_utc()
    });
    static NEWER_TIME: Lazy<DateTime<Utc>> = Lazy::new(|| {
        DateTime::parse_from_str("2022-02-02T00:00:00z", "%+")
            .unwrap()
            .naive_utc()
            .and_utc()
    });

    async fn create_catalog_and_file() -> (Arc<dyn Catalog>, ParquetFile) {
        let metric_registry = Arc::new(metric::Registry::new());
        let catalog = Arc::new(MemCatalog::new(Arc::clone(&metric_registry)));
        create_schema_and_file(catalog).await
    }

    async fn create_schema_and_file(catalog: Arc<dyn Catalog>) -> (Arc<dyn Catalog>, ParquetFile) {
        let mut repos = catalog.repositories().await;
        let namespace = arbitrary_namespace(&mut *repos, "namespace_parquet_file_test").await;
        let table = arbitrary_table(&mut *repos, "test_table", &namespace).await;
        let partition = repos
            .partitions()
            .create_or_get("one".into(), table.id)
            .await
            .unwrap();

        let parquet_file_params = ParquetFileParams {
            namespace_id: namespace.id,
            table_id: partition.table_id,
            partition_id: partition.transition_partition_id(),
            object_store_id: Uuid::new_v4(),
            min_time: Timestamp::new(1),
            max_time: Timestamp::new(10),
            file_size_bytes: 1337,
            row_count: 0,
            compaction_level: CompactionLevel::Initial,
            created_at: Timestamp::new(1),
            column_set: ColumnSet::new([ColumnId::new(1), ColumnId::new(2)]),
            max_l0_created_at: Timestamp::new(1),
        };

        let parquet_file = repos
            .parquet_files()
            .create(parquet_file_params)
            .await
            .unwrap();

        (catalog, parquet_file)
    }

    #[tokio::test]
    async fn dont_delete_new_file_in_catalog() {
        let (catalog, file_in_catalog) = create_catalog_and_file().await;
        let mut repositories = catalog.repositories().await;
        let parquet_files = repositories.parquet_files();

        let location = ParquetFilePath::new(
            file_in_catalog.namespace_id,
            file_in_catalog.table_id,
            &file_in_catalog.partition_id.clone(),
            file_in_catalog.object_store_id,
        )
        .object_store_path();

        let cutoff = *OLDER_TIME;
        let last_modified = *NEWER_TIME;

        let item = ObjectMeta {
            location,
            last_modified,
            size: 0,
            e_tag: None,
        };

        let results = should_delete(vec![item], cutoff, parquet_files).await;
        assert_eq!(results.len(), 0);
    }

    #[tokio::test]
    async fn dont_delete_new_file_not_in_catalog() {
        let metric_registry = Arc::new(metric::Registry::new());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metric_registry)));
        let mut repositories = catalog.repositories().await;
        let parquet_files = repositories.parquet_files();

        let location = ParquetFilePath::new(
            NamespaceId::new(1),
            TableId::new(2),
            &TransitionPartitionId::Deprecated(PartitionId::new(4)),
            Uuid::new_v4(),
        )
        .object_store_path();

        let cutoff = *OLDER_TIME;
        let last_modified = *NEWER_TIME;

        let item = ObjectMeta {
            location,
            last_modified,
            size: 0,
            e_tag: None,
        };

        let results = should_delete(vec![item], cutoff, parquet_files).await;
        assert_eq!(results.len(), 0);
    }

    #[tokio::test]
    async fn dont_delete_new_file_with_unparseable_path() {
        let metric_registry = Arc::new(metric::Registry::new());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metric_registry)));
        let mut repositories = catalog.repositories().await;
        let parquet_files = repositories.parquet_files();

        let cutoff = *OLDER_TIME;
        let last_modified = *NEWER_TIME;

        let item = ObjectMeta {
            location: Path::from("not-a-uuid.parquet"),
            last_modified,
            size: 0,
            e_tag: None,
        };

        let results = should_delete(vec![item], cutoff, parquet_files).await;
        assert_eq!(results.len(), 0);
    }

    #[tokio::test]
    async fn dont_delete_old_file_in_catalog() {
        let (catalog, file_in_catalog) = create_catalog_and_file().await;
        let mut repositories = catalog.repositories().await;
        let parquet_files = repositories.parquet_files();

        let location = ParquetFilePath::new(
            file_in_catalog.namespace_id,
            file_in_catalog.table_id,
            &file_in_catalog.partition_id.clone(),
            file_in_catalog.object_store_id,
        )
        .object_store_path();

        let cutoff = *NEWER_TIME;
        let last_modified = *OLDER_TIME;

        let item = ObjectMeta {
            location,
            last_modified,
            size: 0,
            e_tag: None,
        };

        let results = should_delete(vec![item], cutoff, parquet_files).await;
        assert_eq!(results.len(), 0);
    }

    #[tokio::test]
    async fn delete_old_file_not_in_catalog() {
        let metric_registry = Arc::new(metric::Registry::new());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metric_registry)));
        let mut repositories = catalog.repositories().await;
        let parquet_files = repositories.parquet_files();

        let location = ParquetFilePath::new(
            NamespaceId::new(1),
            TableId::new(2),
            &TransitionPartitionId::Deprecated(PartitionId::new(4)),
            Uuid::new_v4(),
        )
        .object_store_path();

        let cutoff = *NEWER_TIME;
        let last_modified = *OLDER_TIME;

        let item = ObjectMeta {
            location,
            last_modified,
            size: 0,
            e_tag: None,
        };
        let results = should_delete(vec![item.clone()], cutoff, parquet_files).await;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], item);
    }

    #[tokio::test]
    async fn delete_old_file_with_unparseable_path() {
        let metric_registry = Arc::new(metric::Registry::new());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metric_registry)));
        let mut repositories = catalog.repositories().await;
        let parquet_files = repositories.parquet_files();

        let cutoff = *NEWER_TIME;
        let last_modified = *OLDER_TIME;

        let item = ObjectMeta {
            location: Path::from("not-a-uuid.parquet"),
            last_modified,
            size: 0,
            e_tag: None,
        };

        let results = should_delete(vec![item.clone()], cutoff, parquet_files).await;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], item);
    }

    /// The garbage collector checks the catalog for files it _should not delete_. If we can't reach
    /// the catalog (some error), assume we are keeping all the files we are checking.
    /// [do_not_delete_on_catalog_error] tests that.
    #[tokio::test]
    async fn do_not_delete_on_catalog_error() {
        let metric_registry = Arc::new(metric::Registry::new());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metric_registry)));
        let (catalog, file_in_catalog) = create_schema_and_file(catalog).await;

        let mut repositories = catalog.repositories().await;
        let parquet_files = repositories.parquet_files();

        // A ParquetFileRepo that returns an error in the one method [should_delete] uses.
        let mut mocked_parquet_files = MockParquetFileRepo {
            inner: parquet_files,
        };

        let cutoff = *NEWER_TIME;
        let last_modified = *OLDER_TIME;

        let loc = ParquetFilePath::new(
            file_in_catalog.namespace_id,
            file_in_catalog.table_id,
            &file_in_catalog.partition_id.clone(),
            file_in_catalog.object_store_id,
        )
        .object_store_path();

        let item = ObjectMeta {
            location: loc,
            last_modified,
            size: 0,
            e_tag: None,
        };

        // check precondition, file exists in catalog
        let pf = mocked_parquet_files
            .get_by_object_store_id(file_in_catalog.object_store_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(pf, file_in_catalog);

        // because of the db error, there should be no results
        let results = should_delete(vec![item.clone()], cutoff, &mut mocked_parquet_files).await;
        assert_eq!(results.len(), 0);
    }

    struct MockParquetFileRepo<'a> {
        inner: &'a mut dyn ParquetFileRepo,
    }

    #[async_trait]
    impl ParquetFileRepo for MockParquetFileRepo<'_> {
        async fn create(
            &mut self,
            parquet_file_params: ParquetFileParams,
        ) -> iox_catalog::interface::Result<ParquetFile> {
            self.inner.create(parquet_file_params).await
        }

        async fn list_all(&mut self) -> iox_catalog::interface::Result<Vec<ParquetFile>> {
            self.inner.list_all().await
        }

        async fn flag_for_delete_by_retention(
            &mut self,
        ) -> iox_catalog::interface::Result<Vec<ParquetFileId>> {
            self.inner.flag_for_delete_by_retention().await
        }

        async fn list_by_namespace_not_to_delete(
            &mut self,
            namespace_id: NamespaceId,
        ) -> iox_catalog::interface::Result<Vec<ParquetFile>> {
            self.inner
                .list_by_namespace_not_to_delete(namespace_id)
                .await
        }

        async fn list_by_table_not_to_delete(
            &mut self,
            table_id: TableId,
        ) -> iox_catalog::interface::Result<Vec<ParquetFile>> {
            self.inner.list_by_table_not_to_delete(table_id).await
        }

        async fn delete_old_ids_only(
            &mut self,
            older_than: Timestamp,
        ) -> iox_catalog::interface::Result<Vec<ParquetFileId>> {
            self.inner.delete_old_ids_only(older_than).await
        }

        async fn list_by_partition_not_to_delete(
            &mut self,
            partition_id: &TransitionPartitionId,
        ) -> iox_catalog::interface::Result<Vec<ParquetFile>> {
            self.inner
                .list_by_partition_not_to_delete(partition_id)
                .await
        }

        async fn get_by_object_store_id(
            &mut self,
            object_store_id: Uuid,
        ) -> iox_catalog::interface::Result<Option<ParquetFile>> {
            self.inner.get_by_object_store_id(object_store_id).await
        }

        async fn exists_by_object_store_id_batch(
            &mut self,
            _object_store_ids: Vec<Uuid>,
        ) -> iox_catalog::interface::Result<Vec<Uuid>> {
            Err(iox_catalog::interface::Error::SqlxError {
                source: sqlx::Error::WorkerCrashed,
            })
        }

        async fn create_upgrade_delete(
            &mut self,
            delete: &[ParquetFileId],
            upgrade: &[ParquetFileId],
            create: &[ParquetFileParams],
            target_level: CompactionLevel,
        ) -> iox_catalog::interface::Result<Vec<ParquetFileId>> {
            self.create_upgrade_delete(delete, upgrade, create, target_level)
                .await
        }
    }
}
