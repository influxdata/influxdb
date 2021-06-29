//! Methods to cleanup the object store.
use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};

use crate::{
    catalog::{CatalogParquetInfo, CatalogState, PreservedCatalog},
    storage::data_location,
};
use futures::TryStreamExt;
use object_store::{
    path::{parsed::DirsAndFileName, ObjectStorePath, Path},
    ObjectStore, ObjectStoreApi,
};
use observability_deps::tracing::info;
use snafu::{ResultExt, Snafu};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error from read operation while cleaning object store: {}", source))]
    ReadError {
        source: <ObjectStore as ObjectStoreApi>::Error,
    },

    #[snafu(display("Error from write operation while cleaning object store: {}", source))]
    WriteError {
        source: <ObjectStore as ObjectStoreApi>::Error,
    },

    #[snafu(display("Error from catalog loading while cleaning object store: {}", source))]
    CatalogLoadError { source: crate::catalog::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Get unreferenced parquet files.
///
/// The resulting vector is in no particular order. It may be passed to [`delete_files`].
///
/// # Locking / Concurrent Actions
/// While this method is running you MUST NOT create any new parquet files or modify the preserved catalog in any other
/// way. Hence this method needs exclusive access to the preserved catalog and the parquet file. Otherwise this method
/// may report files for deletion that you are about to write to the catalog!
///
/// **This method does NOT acquire the transaction lock!**
///
/// To limit the time the exclusive access is required use `max_files` which will limit the number of files to be
/// detected in this cleanup round.
///
/// The exclusive access can be dropped after this method returned and before calling [`delete_files`].
pub async fn get_unreferenced_parquet_files(
    catalog: &PreservedCatalog,
    max_files: usize,
) -> Result<Vec<Path>> {
    let store = catalog.object_store();
    let server_id = catalog.server_id();
    let db_name = catalog.db_name();
    let all_known = {
        // replay catalog transactions to track ALL (even dropped) files that are referenced
        let (_catalog, state) = PreservedCatalog::load::<TracerCatalogState>(
            Arc::clone(&store),
            server_id,
            db_name.to_string(),
            (),
        )
        .await
        .context(CatalogLoadError)?
        .expect("catalog gone while reading it?");

        state.files.into_inner().expect("lock poissened?")
    };

    let prefix = data_location(&store, server_id, db_name);

    // gather a list of "files to remove" eagerly so we do not block transactions on the catalog for too long
    let mut to_remove = vec![];
    let mut stream = store.list(Some(&prefix)).await.context(ReadError)?;

    'outer: while let Some(paths) = stream.try_next().await.context(ReadError)? {
        for path in paths {
            if to_remove.len() >= max_files {
                info!(%max_files, "reached limit of number of files to cleanup in one go");
                break 'outer;
            }
            let path_parsed: DirsAndFileName = path.clone().into();

            // only delete if all of the following conditions are met:
            // - filename ends with `.parquet`
            // - file is not tracked by the catalog
            if path_parsed
                .file_name
                .as_ref()
                .map(|part| part.encoded().ends_with(".parquet"))
                .unwrap_or(false)
                && !all_known.contains(&path_parsed)
            {
                to_remove.push(path);
            }
        }
    }

    info!(
        n_files = to_remove.len(),
        "Found files to delete"
    );

    Ok(to_remove)
}

/// Delete all `files` from the store linked to the preserved catalog.
///
/// A file might already be deleted (or entirely absent) when this method is called. This will NOT result in an error.
///
/// # Locking / Concurrent Actions
/// File creation and catalog modifications can be done while calling this method. Even
/// [`get_unreferenced_parquet_files`] can be called while is method is in-progress.
pub async fn delete_files(catalog: &PreservedCatalog, files: &[Path]) -> Result<()> {
    let store = catalog.object_store();

    for path in files {
        info!(path = %path.display(), "Delete file");
        store.delete(&path).await.context(WriteError)?;
    }

    info!(n_files = files.len(), "Finished deletion, removed files.");

    Ok(())
}

/// Catalog state that traces all used parquet files.
struct TracerCatalogState {
    files: Mutex<HashSet<DirsAndFileName>>,
}

impl CatalogState for TracerCatalogState {
    type EmptyInput = ();

    fn new_empty(_db_name: &str, _data: Self::EmptyInput) -> Self {
        Self {
            files: Default::default(),
        }
    }

    fn add(
        &mut self,
        _object_store: Arc<ObjectStore>,
        info: CatalogParquetInfo,
    ) -> crate::catalog::Result<()> {
        self.files
            .lock()
            .expect("lock poissened?")
            .insert(info.path);
        Ok(())
    }

    fn remove(&mut self, _path: DirsAndFileName) -> crate::catalog::Result<()> {
        // Do NOT remove the file since we still need it for time travel
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, num::NonZeroU32, sync::Arc};

    use bytes::Bytes;
    use data_types::server_id::ServerId;
    use object_store::path::{parsed::DirsAndFileName, ObjectStorePath, Path};
    use tokio::sync::RwLock;

    use super::*;
    use crate::{
        catalog::test_helpers::TestCatalogState,
        test_utils::{chunk_addr, db_name, make_metadata, make_object_store},
    };

    #[tokio::test]
    async fn test_cleanup_empty() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let db_name = "db1";

        let (catalog, _state) = PreservedCatalog::new_empty::<TestCatalogState>(
            Arc::clone(&object_store),
            server_id,
            db_name.to_string(),
            (),
        )
        .await
        .unwrap();

        // run clean-up
        let files = get_unreferenced_parquet_files(&catalog, 1_000)
            .await
            .unwrap();
        delete_files(&catalog, &files).await.unwrap();
    }

    #[tokio::test]
    async fn test_cleanup_rules() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let db_name = db_name();

        let (catalog, _state) = PreservedCatalog::new_empty::<TestCatalogState>(
            Arc::clone(&object_store),
            server_id,
            db_name.to_string(),
            (),
        )
        .await
        .unwrap();

        // create some data
        let mut paths_keep = vec![];
        let mut paths_delete = vec![];
        {
            let mut transaction = catalog.open_transaction().await;

            // an ordinary tracked parquet file => keep
            let (path, md) = make_metadata(&object_store, "foo", chunk_addr(1)).await;
            transaction.add_parquet(&path.clone().into(), &md).unwrap();
            paths_keep.push(path.display());

            // another ordinary tracked parquet file that was added and removed => keep (for time travel)
            let (path, md) = make_metadata(&object_store, "foo", chunk_addr(2)).await;
            transaction.add_parquet(&path.clone().into(), &md).unwrap();
            transaction.remove_parquet(&path.clone().into()).unwrap();
            paths_keep.push(path.display());

            // not a parquet file => keep
            let mut path: DirsAndFileName = path.into();
            path.file_name = Some("foo.txt".into());
            let path = object_store.path_from_dirs_and_filename(path);
            create_empty_file(&object_store, &path).await;
            paths_keep.push(path.display());

            // an untracked parquet file => delete
            let (path, _md) = make_metadata(&object_store, "foo", chunk_addr(3)).await;
            paths_delete.push(path.display());

            transaction.commit().await.unwrap();
        }

        // run clean-up
        let files = get_unreferenced_parquet_files(&catalog, 1_000)
            .await
            .unwrap();
        delete_files(&catalog, &files).await.unwrap();

        // deleting a second time should just work
        delete_files(&catalog, &files).await.unwrap();

        // list all files
        let all_files = list_all_files(&object_store).await;
        for p in paths_keep {
            assert!(dbg!(&all_files).contains(dbg!(&p)));
        }
        for p in paths_delete {
            assert!(!dbg!(&all_files).contains(dbg!(&p)));
        }
    }

    #[tokio::test]
    async fn test_cleanup_with_parallel_transaction() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let db_name = db_name();
        let lock: RwLock<()> = Default::default();

        let (catalog, _state) = PreservedCatalog::new_empty::<TestCatalogState>(
            Arc::clone(&object_store),
            server_id,
            db_name.to_string(),
            (),
        )
        .await
        .unwrap();

        // try multiple times to provoke a conflict
        for i in 0..100 {
            // Every so often try to create a file with the same ChunkAddr beforehand. This should not trick the cleanup
            // logic to remove the actual file because file paths contains a UUIDv4 part.
            if i % 2 == 0 {
                make_metadata(&object_store, "foo", chunk_addr(i)).await;
            }

            let (path, _) = tokio::join!(
                async {
                    let guard = lock.read().await;
                    let (path, md) = make_metadata(&object_store, "foo", chunk_addr(i)).await;

                    let mut transaction = catalog.open_transaction().await;
                    transaction.add_parquet(&path.clone().into(), &md).unwrap();
                    transaction.commit().await.unwrap();

                    drop(guard);

                    path.display()
                },
                async {
                    let guard = lock.write().await;
                    let files = get_unreferenced_parquet_files(&catalog, 1_000)
                        .await
                        .unwrap();
                    drop(guard);

                    delete_files(&catalog, &files).await.unwrap();
                },
            );

            let all_files = list_all_files(&object_store).await;
            assert!(all_files.contains(&path));
        }
    }

    #[tokio::test]
    async fn test_cleanup_max_files() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let db_name = db_name();

        let (catalog, _state) = PreservedCatalog::new_empty::<TestCatalogState>(
            Arc::clone(&object_store),
            server_id,
            db_name.to_string(),
            (),
        )
        .await
        .unwrap();

        // create some files
        let mut to_remove: HashSet<String> = Default::default();
        for chunk_id in 0..3 {
            let (path, _md) = make_metadata(&object_store, "foo", chunk_addr(chunk_id)).await;
            to_remove.insert(path.display());
        }

        // run clean-up
        let files = get_unreferenced_parquet_files(&catalog, 2).await.unwrap();
        assert_eq!(files.len(), 2);
        delete_files(&catalog, &files).await.unwrap();

        // should only delete 2
        let all_files = list_all_files(&object_store).await;
        let leftover: HashSet<_> = all_files.intersection(&to_remove).collect();
        assert_eq!(leftover.len(), 1);

        // run clean-up again
        let files = get_unreferenced_parquet_files(&catalog, 2).await.unwrap();
        assert_eq!(files.len(), 1);
        delete_files(&catalog, &files).await.unwrap();

        // should delete remaining file
        let all_files = list_all_files(&object_store).await;
        let leftover: HashSet<_> = all_files.intersection(&to_remove).collect();
        assert_eq!(leftover.len(), 0);
    }

    fn make_server_id() -> ServerId {
        ServerId::new(NonZeroU32::new(1).unwrap())
    }

    async fn create_empty_file(object_store: &ObjectStore, path: &Path) {
        let data = Bytes::default();
        let len = data.len();

        object_store
            .put(
                &path,
                futures::stream::once(async move { Ok(data) }),
                Some(len),
            )
            .await
            .unwrap();
    }

    async fn list_all_files(object_store: &ObjectStore) -> HashSet<String> {
        object_store
            .list(None)
            .await
            .unwrap()
            .try_concat()
            .await
            .unwrap()
            .iter()
            .map(|p| p.display())
            .collect()
    }
}
