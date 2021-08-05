//! Methods to cleanup the object store.
use std::{collections::HashSet, sync::Arc};

use crate::catalog::{CatalogParquetInfo, CatalogState, PreservedCatalog};
use futures::TryStreamExt;
use iox_object_store::IoxObjectStore;
use object_store::{
    path::{parsed::DirsAndFileName, ObjectStorePath, Path},
    ObjectStore, ObjectStoreApi,
};
use observability_deps::tracing::info;
use parking_lot::Mutex;
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
    let iox_object_store = catalog.iox_object_store();
    let all_known = {
        // replay catalog transactions to track ALL (even dropped) files that are referenced
        let (_catalog, state) =
            PreservedCatalog::load::<TracerCatalogState>(Arc::clone(&iox_object_store), ())
                .await
                .context(CatalogLoadError)?
                .expect("catalog gone while reading it?");

        state.files.into_inner()
    };

    let prefix = iox_object_store.data_path();

    // gather a list of "files to remove" eagerly so we do not block transactions on the catalog for too long
    let mut to_remove = vec![];
    let mut stream = iox_object_store
        .list(Some(&prefix))
        .await
        .context(ReadError)?;

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

    info!(n_files = to_remove.len(), "Found files to delete");

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
    let store = catalog.iox_object_store();

    for path in files {
        info!(path = %path.display(), "Delete file");
        store.delete(path).await.context(WriteError)?;
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
        _iox_object_store: Arc<IoxObjectStore>,
        info: CatalogParquetInfo,
    ) -> crate::catalog::Result<()> {
        self.files.lock().insert(info.path);
        Ok(())
    }

    fn remove(&mut self, _path: DirsAndFileName) -> crate::catalog::Result<()> {
        // Do NOT remove the file since we still need it for time travel
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, sync::Arc};

    use bytes::Bytes;
    use object_store::path::{ObjectStorePath, Path};
    use tokio::sync::RwLock;

    use super::*;
    use crate::{
        catalog::test_helpers::TestCatalogState,
        test_utils::{chunk_addr, make_iox_object_store, make_metadata},
    };

    #[tokio::test]
    async fn test_cleanup_empty() {
        let iox_object_store = make_iox_object_store();

        let (catalog, _state) =
            PreservedCatalog::new_empty::<TestCatalogState>(Arc::clone(&iox_object_store), ())
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
        let iox_object_store = make_iox_object_store();

        let (catalog, _state) =
            PreservedCatalog::new_empty::<TestCatalogState>(Arc::clone(&iox_object_store), ())
                .await
                .unwrap();

        // create some data
        let mut paths_keep = vec![];
        let mut paths_delete = vec![];
        {
            let mut transaction = catalog.open_transaction().await;

            // an ordinary tracked parquet file => keep
            let (path, metadata) = make_metadata(&iox_object_store, "foo", chunk_addr(1)).await;
            let metadata = Arc::new(metadata);
            let path = path.into();
            let info = CatalogParquetInfo {
                path,
                file_size_bytes: 33,
                metadata,
            };

            transaction.add_parquet(&info).unwrap();
            paths_keep.push(info.path.display());

            // another ordinary tracked parquet file that was added and removed => keep (for time travel)
            let (path, metadata) = make_metadata(&iox_object_store, "foo", chunk_addr(2)).await;
            let metadata = Arc::new(metadata);
            let path = path.into();
            let info = CatalogParquetInfo {
                path,
                file_size_bytes: 33,
                metadata,
            };
            transaction.add_parquet(&info).unwrap();
            transaction.remove_parquet(&info.path);
            paths_keep.push(info.path.display());

            // not a parquet file => keep
            let mut path = info.path;
            path.file_name = Some("foo.txt".into());
            let path = iox_object_store.path_from_dirs_and_filename(path);
            create_empty_file(&iox_object_store, &path).await;
            paths_keep.push(path.display());

            // an untracked parquet file => delete
            let (path, _md) = make_metadata(&iox_object_store, "foo", chunk_addr(3)).await;
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
        let all_files = list_all_files(&iox_object_store).await;
        for p in paths_keep {
            assert!(dbg!(&all_files).contains(dbg!(&p)));
        }
        for p in paths_delete {
            assert!(!dbg!(&all_files).contains(dbg!(&p)));
        }
    }

    #[tokio::test]
    async fn test_cleanup_with_parallel_transaction() {
        let iox_object_store = make_iox_object_store();
        let lock: RwLock<()> = Default::default();

        let (catalog, _state) =
            PreservedCatalog::new_empty::<TestCatalogState>(Arc::clone(&iox_object_store), ())
                .await
                .unwrap();

        // try multiple times to provoke a conflict
        for i in 0..100 {
            // Every so often try to create a file with the same ChunkAddr beforehand. This should not trick the cleanup
            // logic to remove the actual file because file paths contains a UUIDv4 part.
            if i % 2 == 0 {
                make_metadata(&iox_object_store, "foo", chunk_addr(i)).await;
            }

            let (path, _) = tokio::join!(
                async {
                    let guard = lock.read().await;
                    let (path, md) = make_metadata(&iox_object_store, "foo", chunk_addr(i)).await;

                    let metadata = Arc::new(md);
                    let path = path.into();
                    let info = CatalogParquetInfo {
                        path,
                        file_size_bytes: 33,
                        metadata,
                    };

                    let mut transaction = catalog.open_transaction().await;
                    transaction.add_parquet(&info).unwrap();
                    transaction.commit().await.unwrap();

                    drop(guard);

                    info.path.display()
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

            let all_files = list_all_files(&iox_object_store).await;
            assert!(all_files.contains(&path));
        }
    }

    #[tokio::test]
    async fn test_cleanup_max_files() {
        let iox_object_store = make_iox_object_store();

        let (catalog, _state) =
            PreservedCatalog::new_empty::<TestCatalogState>(Arc::clone(&iox_object_store), ())
                .await
                .unwrap();

        // create some files
        let mut to_remove: HashSet<String> = Default::default();
        for chunk_id in 0..3 {
            let (path, _md) = make_metadata(&iox_object_store, "foo", chunk_addr(chunk_id)).await;
            to_remove.insert(path.display());
        }

        // run clean-up
        let files = get_unreferenced_parquet_files(&catalog, 2).await.unwrap();
        assert_eq!(files.len(), 2);
        delete_files(&catalog, &files).await.unwrap();

        // should only delete 2
        let all_files = list_all_files(&iox_object_store).await;
        let leftover: HashSet<_> = all_files.intersection(&to_remove).collect();
        assert_eq!(leftover.len(), 1);

        // run clean-up again
        let files = get_unreferenced_parquet_files(&catalog, 2).await.unwrap();
        assert_eq!(files.len(), 1);
        delete_files(&catalog, &files).await.unwrap();

        // should delete remaining file
        let all_files = list_all_files(&iox_object_store).await;
        let leftover: HashSet<_> = all_files.intersection(&to_remove).collect();
        assert_eq!(leftover.len(), 0);
    }

    async fn create_empty_file(iox_object_store: &IoxObjectStore, path: &Path) {
        let data = Bytes::default();
        let len = data.len();

        iox_object_store
            .put(
                path,
                futures::stream::once(async move { Ok(data) }),
                Some(len),
            )
            .await
            .unwrap();
    }

    async fn list_all_files(iox_object_store: &IoxObjectStore) -> HashSet<String> {
        iox_object_store
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
