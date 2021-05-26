//! Methods to cleanup the object store.

use futures::TryStreamExt;
use object_store::{path::parsed::DirsAndFileName, ObjectStore, ObjectStoreApi};
use observability_deps::tracing::info;
use snafu::{ResultExt, Snafu};

use crate::{
    catalog::{CatalogState, PreservedCatalog},
    storage::data_location,
};
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
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Delete all unreferenced parquet files.
pub async fn cleanup_unreferenced_parquet_files<S>(catalog: &PreservedCatalog<S>) -> Result<()>
where
    S: CatalogState,
{
    // Create a transaction to prevent parallel modifications of the catalog. This avoids that we delete files there
    // that are about to get added to the catalog.
    let transaction = catalog.open_transaction().await;

    let all_known = catalog.state().tracked_parquet_files();
    let store = catalog.object_store();
    let prefix = data_location(&store, catalog.server_id(), catalog.db_name());

    let mut stream = store.list(Some(&prefix)).await.context(ReadError)?;
    let mut files_removed = 0;
    while let Some(paths) = stream.try_next().await.context(ReadError)? {
        for path in paths {
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
                store.delete(&path).await.context(WriteError)?;
                files_removed += 1;
            }
        }
    }

    transaction.abort();
    info!("Removed {} files during clean-up.", files_removed);

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, num::NonZeroU32, sync::Arc};

    use bytes::Bytes;
    use data_types::server_id::ServerId;
    use object_store::path::{parsed::DirsAndFileName, ObjectStorePath, Path};

    use super::*;
    use crate::{
        catalog::test_helpers::TestCatalogState,
        utils::{make_metadata, make_object_store},
    };

    #[tokio::test]
    async fn test_cleanup_rules() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let db_name = "db1";

        let catalog = PreservedCatalog::<TestCatalogState>::new_empty(
            Arc::clone(&object_store),
            server_id,
            db_name,
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
            let (path, md) = make_metadata(&object_store, "foo", 1).await;
            transaction.add_parquet(&path.clone().into(), &md).unwrap();
            paths_keep.push(path.display());

            // another ordinary tracked parquet file => keep
            let (path, md) = make_metadata(&object_store, "foo", 2).await;
            transaction.add_parquet(&path.clone().into(), &md).unwrap();
            paths_keep.push(path.display());

            // not a parquet file => keep
            let mut path: DirsAndFileName = path.into();
            path.file_name = Some("foo.txt".into());
            let path = object_store.path_from_dirs_and_filename(path);
            create_empty_file(&object_store, &path).await;
            paths_keep.push(path.display());

            // an untracked parquet file => delete
            let (path, _md) = make_metadata(&object_store, "foo", 3).await;
            paths_delete.push(path.display());

            transaction.commit().await.unwrap();
        }

        // run clean-up
        cleanup_unreferenced_parquet_files(&catalog).await.unwrap();

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
        let db_name = "db1";

        let catalog = PreservedCatalog::<TestCatalogState>::new_empty(
            Arc::clone(&object_store),
            server_id,
            db_name,
            (),
        )
        .await
        .unwrap();

        // try multiple times to provoke a conflict
        for i in 0..100 {
            let (path, _) = tokio::join!(
                async {
                    let mut transaction = catalog.open_transaction().await;

                    let (path, md) = make_metadata(&object_store, "foo", i).await;
                    transaction.add_parquet(&path.clone().into(), &md).unwrap();

                    transaction.commit().await.unwrap();

                    path.display()
                },
                async {
                    cleanup_unreferenced_parquet_files(&catalog).await.unwrap();
                },
            );

            let all_files = list_all_files(&object_store).await;
            assert!(all_files.contains(&path));
        }
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
