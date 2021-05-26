//! Methods to cleanup the object store.

use futures::TryStreamExt;
use object_store::{ObjectStore, ObjectStoreApi};
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
            if !all_known.contains(&path.clone().into()) {
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

    use data_types::server_id::ServerId;
    use object_store::path::ObjectStorePath;

    use super::*;
    use crate::{
        catalog::test_helpers::TestCatalogState,
        utils::{make_metadata, make_object_store},
    };

    #[tokio::test]
    async fn test_cleanup() {
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
        let mut paths_tracked = vec![];
        let mut paths_untracked = vec![];
        {
            let mut transaction = catalog.open_transaction().await;

            let (path, md) = make_metadata(&object_store, "foo", 1).await;
            transaction.add_parquet(&path.clone().into(), &md).unwrap();
            paths_tracked.push(path.display());

            let (path, md) = make_metadata(&object_store, "foo", 2).await;
            transaction.add_parquet(&path.clone().into(), &md).unwrap();
            paths_tracked.push(path.display());

            let (path, _md) = make_metadata(&object_store, "foo", 3).await;
            paths_untracked.push(path.display());

            transaction.commit().await.unwrap();
        }

        // run clean-up
        cleanup_unreferenced_parquet_files(&catalog).await.unwrap();

        // list all files
        let all_files: HashSet<_> = object_store
            .list(None)
            .await
            .unwrap()
            .try_concat()
            .await
            .unwrap()
            .iter()
            .map(|p| p.display())
            .collect();
        for p in paths_tracked {
            assert!(dbg!(&all_files).contains(dbg!(&p)));
        }
        for p in paths_untracked {
            assert!(!dbg!(&all_files).contains(dbg!(&p)));
        }
    }

    fn make_server_id() -> ServerId {
        ServerId::new(NonZeroU32::new(1).unwrap())
    }
}
