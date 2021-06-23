//! Contains code to rebuild a catalog from files.
use std::{
    collections::{hash_map::Entry, HashMap},
    fmt::Debug,
    sync::Arc,
};

use data_types::server_id::ServerId;
use futures::TryStreamExt;
use object_store::{
    path::{parsed::DirsAndFileName, Path},
    ObjectStore, ObjectStoreApi,
};
use observability_deps::tracing::error;
use snafu::{ResultExt, Snafu};
use uuid::Uuid;

use crate::{
    catalog::{CatalogParquetInfo, CatalogState, CheckpointData, PreservedCatalog},
    metadata::{IoxMetadata, IoxParquetMetaData},
};
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Cannot create new empty catalog: {}", source))]
    NewEmptyFailure { source: crate::catalog::Error },

    #[snafu(display("Cannot read store: {}", source))]
    ReadFailure { source: object_store::Error },

    #[snafu(display("Cannot read IOx metadata from parquet file ({:?}): {}", path, source))]
    MetadataReadFailure {
        source: crate::metadata::Error,
        path: Path,
    },

    #[snafu(display(
        "Found multiple transaction for revision {}: {} and {}",
        revision_counter,
        uuid1,
        uuid2
    ))]
    MultipleTransactionsFailure {
        revision_counter: u64,
        uuid1: Uuid,
        uuid2: Uuid,
    },

    #[snafu(display(
        "Internal error: Revision cannot be zero (this transaction is always empty): {:?}",
        path
    ))]
    RevisionZeroFailure { path: Path },

    #[snafu(display("Cannot add file to transaction: {}", source))]
    FileRecordFailure { source: crate::catalog::Error },

    #[snafu(display("Cannot commit transaction: {}", source))]
    CommitFailure { source: crate::catalog::Error },

    #[snafu(display("Cannot create checkpoint: {}", source))]
    CheckpointFailure { source: crate::catalog::Error },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Creates a new catalog from parquet files.
///
/// Users are required to [wipe](PreservedCatalog::wipe) the existing catalog before running this
/// procedure (**after creating a backup!**).
///
/// This will create a catalog checkpoint for the very last transaction.
///
/// # Limitations
/// Compared to an intact catalog, wiping a catalog and rebuilding it from Parquet files has the following drawbacks:
///
/// - **Garbage Susceptibility:** The rebuild process will stumble over garbage parquet files (i.e. files being present
///   in the object store but that were not part of the catalog). For files that where not written by IOx it will likely
///   report [`Error::MetadataReadFailure`]. For files that are left-overs from previous transactions it will likely
///   report [`Error::MultipleTransactionsFailure`]. Crafted files (i.e. files with the correct metadata and matching
///   transaction UUIDs) will blindly be included into the new catalog, because we have no way to distinguish them from
///   the actual catalog content.
/// - **No Removals:** The rebuild system cannot recover the fact that files where removed from the catalog during some
///   transaction. This might not always be an issue due to "deduplicate while read"-logic in the query engine, but also
///   might have unwanted side effects (e.g. performance issues).
///
/// # Error Handling
/// This routine will fail if:
///
/// - **Metadata Read Failure:** There is a parquet file with metadata that cannot be read. Set
///   `ignore_metadata_read_failure` to `true` to ignore these cases.
/// - **Parquet With Revision Zero:** One of the parquet files reports it belongs to revision `0`. This should never
///   happen since the first transaction is always an empty one. This was likely causes by a bug or a file created by
///   3rd party tooling.
/// - **Multiple Transactions:** If there are multiple transaction with the same revision but different UUIDs, this
///   routine cannot reconstruct a single linear revision history. Make sure to
//    [clean up](crate::cleanup::cleanup_unreferenced_parquet_files) regularly to avoid this case.
pub async fn rebuild_catalog<S>(
    object_store: Arc<ObjectStore>,
    search_location: &Path,
    server_id: ServerId,
    db_name: String,
    catalog_empty_input: S::EmptyInput,
    ignore_metadata_read_failure: bool,
) -> Result<(PreservedCatalog, Arc<S>)>
where
    S: CatalogState + Debug + Send + Sync,
{
    // collect all revisions from parquet files
    let mut revisions =
        collect_revisions(&object_store, search_location, ignore_metadata_read_failure).await?;

    // create new empty catalog
    let (catalog, state) = PreservedCatalog::new_empty::<S>(
        Arc::clone(&object_store),
        server_id,
        db_name,
        catalog_empty_input,
    )
    .await
    .context(NewEmptyFailure)?;
    let mut state = Arc::try_unwrap(state).expect("dangling Arc?");

    // trace all files for final checkpoint
    let mut collected_files = HashMap::new();

    // simulate all transactions
    if let Some(max_revision) = revisions.keys().max().cloned() {
        for revision_counter in 1..=max_revision {
            assert_eq!(
                catalog.revision_counter() + 1,
                revision_counter,
                "revision counter during transaction simulation out-of-sync"
            );

            if let Some((uuid, entries)) = revisions.remove(&revision_counter) {
                // we have files for this particular transaction
                let mut transaction = catalog.open_transaction_with_uuid(uuid).await;
                for (path, metadata) in entries {
                    let path: DirsAndFileName = path.clone().into();

                    state
                        .add(
                            Arc::clone(&object_store),
                            CatalogParquetInfo {
                                path: path.clone(),
                                metadata: Arc::new(metadata.clone()),
                            },
                        )
                        .context(FileRecordFailure)?;
                    transaction
                        .add_parquet(&path, &metadata)
                        .context(FileRecordFailure)?;
                    collected_files.insert(path, Arc::new(metadata));
                }

                let ckpt_handle = transaction.commit().await.context(CommitFailure)?;
                if revision_counter == max_revision {
                    ckpt_handle
                        .create_checkpoint(CheckpointData {
                            files: collected_files.clone(),
                        })
                        .await
                        .context(CommitFailure)?;
                }
            } else {
                // we do not have any files for this transaction (there might have been other actions though or it was
                // an empty transaction) => create new empty transaction
                // Note that this can never be the last transaction, so we don't need to create a checkpoint here.
                let transaction = catalog.open_transaction().await;
                transaction.commit().await.context(CheckpointFailure)?;
            }
        }
    }

    Ok((catalog, Arc::new(state)))
}

/// Collect all files under the given locations.
///
/// Returns a map of revisions to their UUIDs and a vector of file-metadata tuples.
///
/// The file listing is recursive.
async fn collect_revisions(
    object_store: &ObjectStore,
    search_location: &Path,
    ignore_metadata_read_failure: bool,
) -> Result<HashMap<u64, (Uuid, Vec<(Path, IoxParquetMetaData)>)>> {
    let mut stream = object_store
        .list(Some(search_location))
        .await
        .context(ReadFailure)?;

    // revision -> (uuid, [file])
    let mut revisions: HashMap<u64, (Uuid, Vec<(Path, IoxParquetMetaData)>)> = HashMap::new();

    while let Some(paths) = stream.try_next().await.context(ReadFailure)? {
        for path in paths.into_iter().filter(is_parquet) {
            let (iox_md, parquet_md) = match read_parquet(object_store, &path).await {
                Ok(res) => res,
                Err(e @ Error::MetadataReadFailure { .. }) if ignore_metadata_read_failure => {
                    error!("error while reading metdata from parquet, ignoring: {}", e);
                    continue;
                }
                Err(e) => return Err(e),
            };

            // revision 0 can never occur because it is always empty
            if iox_md.transaction_revision_counter == 0 {
                return Err(Error::RevisionZeroFailure { path });
            }

            match revisions.entry(iox_md.transaction_revision_counter) {
                Entry::Vacant(v) => {
                    // revision not known yet => create it
                    v.insert((iox_md.transaction_uuid, vec![(path, parquet_md)]));
                }
                Entry::Occupied(mut o) => {
                    // already exist => check UUID
                    let (uuid, entries) = o.get_mut();

                    if *uuid != iox_md.transaction_uuid {
                        // found multiple transactions for this revision => cannot rebuild cleanly

                        // sort UUIDs for deterministic error messages
                        let (uuid1, uuid2) = if *uuid < iox_md.transaction_uuid {
                            (*uuid, iox_md.transaction_uuid)
                        } else {
                            (iox_md.transaction_uuid, *uuid)
                        };
                        return Err(Error::MultipleTransactionsFailure {
                            revision_counter: iox_md.transaction_revision_counter,
                            uuid1,
                            uuid2,
                        });
                    }

                    entries.push((path, parquet_md));
                }
            }
        }
    }

    Ok(revisions)
}

/// Checks if the given path is (likely) a parquet file.
fn is_parquet(path: &Path) -> bool {
    let path: DirsAndFileName = path.clone().into();
    if let Some(filename) = path.file_name {
        filename.encoded().ends_with(".parquet")
    } else {
        false
    }
}

/// Read Parquet and IOx metadata from given path.
async fn read_parquet(
    object_store: &ObjectStore,
    path: &Path,
) -> Result<(IoxMetadata, IoxParquetMetaData)> {
    let data = object_store
        .get(path)
        .await
        .context(ReadFailure)?
        .map_ok(|bytes| bytes.to_vec())
        .try_concat()
        .await
        .context(ReadFailure)?;

    let parquet_metadata = IoxParquetMetaData::from_file_bytes(data)
        .context(MetadataReadFailure { path: path.clone() })?;
    let iox_metadata = parquet_metadata
        .read_iox_metadata()
        .context(MetadataReadFailure { path: path.clone() })?;
    Ok((iox_metadata, parquet_metadata))
}

#[cfg(test)]
mod tests {
    use data_types::chunk_metadata::ChunkAddr;
    use datafusion::physical_plan::SendableRecordBatchStream;
    use datafusion_util::MemoryStream;
    use parquet::arrow::ArrowWriter;
    use tokio_stream::StreamExt;

    use super::*;
    use std::num::NonZeroU32;

    use crate::{catalog::test_helpers::TestCatalogState, storage::MemWriter};
    use crate::{
        catalog::PreservedCatalog,
        storage::Storage,
        test_utils::{make_object_store, make_record_batch},
    };

    #[tokio::test]
    async fn test_rebuild_successfull() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let db_name = "db1";

        // build catalog with some data
        let (catalog, state) = PreservedCatalog::new_empty::<TestCatalogState>(
            Arc::clone(&object_store),
            server_id,
            db_name.to_string(),
            (),
        )
        .await
        .unwrap();
        let mut state = Arc::try_unwrap(state).unwrap();
        {
            let mut transaction = catalog.open_transaction().await;

            let (path, md) = create_parquet_file(
                &object_store,
                server_id,
                db_name,
                transaction.revision_counter(),
                transaction.uuid(),
                0,
            )
            .await;
            state
                .parquet_files
                .insert(path.clone(), Arc::new(md.clone()));
            transaction.add_parquet(&path, &md).unwrap();

            let (path, md) = create_parquet_file(
                &object_store,
                server_id,
                db_name,
                transaction.revision_counter(),
                transaction.uuid(),
                1,
            )
            .await;
            state
                .parquet_files
                .insert(path.clone(), Arc::new(md.clone()));
            transaction.add_parquet(&path, &md).unwrap();

            transaction.commit().await.unwrap();
        }
        {
            // empty transaction
            let transaction = catalog.open_transaction().await;
            transaction.commit().await.unwrap();
        }
        {
            let mut transaction = catalog.open_transaction().await;

            let (path, md) = create_parquet_file(
                &object_store,
                server_id,
                db_name,
                transaction.revision_counter(),
                transaction.uuid(),
                2,
            )
            .await;
            state
                .parquet_files
                .insert(path.clone(), Arc::new(md.clone()));
            transaction.add_parquet(&path, &md).unwrap();

            transaction.commit().await.unwrap();
        }

        // store catalog state
        let paths_expected = {
            let mut tmp: Vec<_> = state.parquet_files.keys().cloned().collect();
            tmp.sort();
            tmp
        };

        // wipe catalog
        drop(catalog);
        PreservedCatalog::wipe(&object_store, server_id, db_name)
            .await
            .unwrap();

        // rebuild
        let path = object_store.new_path();
        let (catalog, state) = rebuild_catalog::<TestCatalogState>(
            object_store,
            &path,
            server_id,
            db_name.to_string(),
            (),
            false,
        )
        .await
        .unwrap();

        // check match
        let paths_actual = {
            let mut tmp: Vec<_> = state.parquet_files.keys().cloned().collect();
            tmp.sort();
            tmp
        };
        assert_eq!(paths_actual, paths_expected);
        assert_eq!(catalog.revision_counter(), 3);
    }

    #[tokio::test]
    async fn test_rebuild_empty() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let db_name = "db1";

        // build empty catalog
        let (catalog, _state) = PreservedCatalog::new_empty::<TestCatalogState>(
            Arc::clone(&object_store),
            server_id,
            db_name.to_string(),
            (),
        )
        .await
        .unwrap();

        // wipe catalog
        drop(catalog);
        PreservedCatalog::wipe(&object_store, server_id, db_name)
            .await
            .unwrap();

        // rebuild
        let path = object_store.new_path();
        let (catalog, state) = rebuild_catalog::<TestCatalogState>(
            object_store,
            &path,
            server_id,
            db_name.to_string(),
            (),
            false,
        )
        .await
        .unwrap();

        // check match
        assert!(state.parquet_files.is_empty());
        assert_eq!(catalog.revision_counter(), 0);
    }

    #[tokio::test]
    async fn test_rebuild_fail_transaction_zero() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let db_name = "db1";

        // build catalog with same data
        let catalog = PreservedCatalog::new_empty::<TestCatalogState>(
            Arc::clone(&object_store),
            server_id,
            db_name.to_string(),
            (),
        )
        .await
        .unwrap();

        // file with illegal revision counter (zero is always an empty transaction)
        create_parquet_file(&object_store, server_id, db_name, 0, Uuid::new_v4(), 0).await;

        // wipe catalog
        drop(catalog);
        PreservedCatalog::wipe(&object_store, server_id, db_name)
            .await
            .unwrap();

        // rebuild
        let path = object_store.new_path();
        let res = rebuild_catalog::<TestCatalogState>(
            object_store,
            &path,
            server_id,
            db_name.to_string(),
            (),
            false,
        )
        .await;
        assert!(dbg!(res.unwrap_err().to_string()).starts_with(
            "Internal error: Revision cannot be zero (this transaction is always empty):"
        ));
    }

    #[tokio::test]
    async fn test_rebuild_fail_duplicate_transaction_uuid() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let db_name = "db1";

        // build catalog with same data
        let (catalog, _state) = PreservedCatalog::new_empty::<TestCatalogState>(
            Arc::clone(&object_store),
            server_id,
            db_name.to_string(),
            (),
        )
        .await
        .unwrap();
        {
            let mut transaction = catalog.open_transaction().await;

            let (path, md) = create_parquet_file(
                &object_store,
                server_id,
                db_name,
                transaction.revision_counter(),
                transaction.uuid(),
                0,
            )
            .await;
            transaction.add_parquet(&path, &md).unwrap();

            // create parquet file with wrong UUID
            create_parquet_file(
                &object_store,
                server_id,
                db_name,
                transaction.revision_counter(),
                Uuid::new_v4(),
                1,
            )
            .await;

            transaction.commit().await.unwrap();
        }

        // wipe catalog
        drop(catalog);
        PreservedCatalog::wipe(&object_store, server_id, db_name)
            .await
            .unwrap();

        // rebuild
        let path = object_store.new_path();
        let res = rebuild_catalog::<TestCatalogState>(
            object_store,
            &path,
            server_id,
            db_name.to_string(),
            (),
            false,
        )
        .await;
        assert!(dbg!(res.unwrap_err().to_string())
            .starts_with("Found multiple transaction for revision 1:"));
    }

    #[tokio::test]
    async fn test_rebuild_no_metadata() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let db_name = "db1";

        // build catalog with same data
        let catalog = PreservedCatalog::new_empty::<TestCatalogState>(
            Arc::clone(&object_store),
            server_id,
            db_name.to_string(),
            (),
        )
        .await
        .unwrap();

        // file w/o metadata
        create_parquet_file_without_metadata(&object_store, server_id, db_name, 0).await;

        // wipe catalog
        drop(catalog);
        PreservedCatalog::wipe(&object_store, server_id, db_name)
            .await
            .unwrap();

        // rebuild (do not ignore errors)
        let path = object_store.new_path();
        let res = rebuild_catalog::<TestCatalogState>(
            Arc::clone(&object_store),
            &path,
            server_id,
            db_name.to_string(),
            (),
            false,
        )
        .await;
        assert!(dbg!(res.unwrap_err().to_string())
            .starts_with("Cannot read IOx metadata from parquet file"));

        // rebuild (ignore errors)
        let (catalog, state) = rebuild_catalog::<TestCatalogState>(
            object_store,
            &path,
            server_id,
            db_name.to_string(),
            (),
            true,
        )
        .await
        .unwrap();
        assert!(state.parquet_files.is_empty());
        assert_eq!(catalog.revision_counter(), 0);
    }

    #[tokio::test]
    async fn test_rebuild_creates_file_checkpoint() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let db_name = "db1";

        // build catalog with some data (2 transactions + initial empty one)
        let (catalog, state) = PreservedCatalog::new_empty::<TestCatalogState>(
            Arc::clone(&object_store),
            server_id,
            db_name.to_string(),
            (),
        )
        .await
        .unwrap();
        let mut state = Arc::try_unwrap(state).unwrap();
        {
            let mut transaction = catalog.open_transaction().await;

            let (path, md) = create_parquet_file(
                &object_store,
                server_id,
                db_name,
                transaction.revision_counter(),
                transaction.uuid(),
                0,
            )
            .await;
            state
                .parquet_files
                .insert(path.clone(), Arc::new(md.clone()));
            transaction.add_parquet(&path, &md).unwrap();

            transaction.commit().await.unwrap();
        }
        {
            let mut transaction = catalog.open_transaction().await;

            let (path, md) = create_parquet_file(
                &object_store,
                server_id,
                db_name,
                transaction.revision_counter(),
                transaction.uuid(),
                2,
            )
            .await;
            state
                .parquet_files
                .insert(path.clone(), Arc::new(md.clone()));
            transaction.add_parquet(&path, &md).unwrap();

            transaction.commit().await.unwrap();
        }
        assert_eq!(catalog.revision_counter(), 2);

        // store catalog state
        let paths_expected = {
            let mut tmp: Vec<_> = state.parquet_files.keys().cloned().collect();
            tmp.sort();
            tmp
        };

        // wipe catalog
        drop(catalog);
        PreservedCatalog::wipe(&object_store, server_id, db_name)
            .await
            .unwrap();

        // rebuild
        let path = object_store.new_path();
        let catalog = rebuild_catalog::<TestCatalogState>(
            Arc::clone(&object_store),
            &path,
            server_id,
            db_name.to_string(),
            (),
            false,
        )
        .await
        .unwrap();
        drop(catalog);

        // delete transaction files
        let paths = object_store
            .list(None)
            .await
            .unwrap()
            .try_concat()
            .await
            .unwrap();
        let mut deleted = false;
        for path in paths {
            let parsed: DirsAndFileName = path.clone().into();
            if parsed
                .file_name
                .map_or(false, |part| part.encoded().ends_with(".txn"))
            {
                object_store.delete(&path).await.unwrap();
                deleted = true;
            }
        }
        assert!(deleted);

        // load catalog
        let (catalog, state) = PreservedCatalog::load::<TestCatalogState>(
            Arc::clone(&object_store),
            server_id,
            db_name.to_string(),
            (),
        )
        .await
        .unwrap()
        .unwrap();

        // check match
        let paths_actual = {
            let mut tmp: Vec<_> = state.parquet_files.keys().cloned().collect();
            tmp.sort();
            tmp
        };
        assert_eq!(paths_actual, paths_expected);
        assert_eq!(catalog.revision_counter(), 2);
    }

    /// Creates new test server ID
    fn make_server_id() -> ServerId {
        ServerId::new(NonZeroU32::new(1).unwrap())
    }

    pub async fn create_parquet_file(
        object_store: &Arc<ObjectStore>,
        server_id: ServerId,
        db_name: &str,
        transaction_revision_counter: u64,
        transaction_uuid: Uuid,
        chunk_id: u32,
    ) -> (DirsAndFileName, IoxParquetMetaData) {
        let table_name = "table1";
        let partition_key = "part1";
        let (record_batches, _schema, _column_summaries, _num_rows) = make_record_batch("foo");

        let storage = Storage::new(Arc::clone(object_store), server_id);
        let metadata = IoxMetadata {
            transaction_revision_counter,
            transaction_uuid,
            table_name: table_name.to_string(),
            partition_key: partition_key.to_string(),
            chunk_id,
        };
        let stream: SendableRecordBatchStream = Box::pin(MemoryStream::new(record_batches));
        let (path, parquet_md) = storage
            .write_to_object_store(
                ChunkAddr {
                    db_name: Arc::from(db_name),
                    table_name: Arc::from(table_name),
                    partition_key: Arc::from(partition_key),
                    chunk_id,
                },
                stream,
                metadata,
            )
            .await
            .unwrap();

        let path: DirsAndFileName = path.into();
        (path, parquet_md)
    }

    pub async fn create_parquet_file_without_metadata(
        object_store: &Arc<ObjectStore>,
        server_id: ServerId,
        db_name: &str,
        chunk_id: u32,
    ) -> (DirsAndFileName, IoxParquetMetaData) {
        let (record_batches, schema, _column_summaries, _num_rows) = make_record_batch("foo");
        let mut stream: SendableRecordBatchStream = Box::pin(MemoryStream::new(record_batches));

        let mem_writer = MemWriter::default();
        {
            let mut writer =
                ArrowWriter::try_new(mem_writer.clone(), Arc::clone(schema.inner()), None).unwrap();
            while let Some(batch) = stream.next().await {
                let batch = batch.unwrap();
                writer.write(&batch).unwrap();
            }
            writer.close().unwrap();
        } // drop the reference to the MemWriter that the SerializedFileWriter has

        let data = mem_writer.into_inner().unwrap();
        let md = IoxParquetMetaData::from_file_bytes(data.clone()).unwrap();
        let storage = Storage::new(Arc::clone(object_store), server_id);
        let path = storage.location(&ChunkAddr {
            db_name: Arc::from(db_name),
            table_name: Arc::from("table1"),
            partition_key: Arc::from("part1"),
            chunk_id,
        });
        storage.to_object_store(data, &path).await.unwrap();

        let path: DirsAndFileName = path.into();
        (path, md)
    }
}
