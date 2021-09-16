use std::{
    collections::{
        hash_map::Entry::{Occupied, Vacant},
        HashMap,
    },
    fmt::Debug,
    sync::Arc,
};

use iox_object_store::{IoxObjectStore, ParquetFilePath, TransactionFilePath};
use snafu::ResultExt;

use crate::{
    catalog::{
        core::PreservedCatalog,
        interface::{
            CatalogParquetInfo, CatalogState, CatalogStateAddError, CatalogStateRemoveError,
            CheckpointData,
        },
        internals::{
            proto_io::{load_transaction_proto, store_transaction_proto},
            types::TransactionKey,
        },
    },
    metadata::IoxParquetMetaData,
    test_utils::{chunk_addr, make_iox_object_store, make_metadata, TestSize},
};

#[derive(Clone, Debug, Default)]
pub struct Table {
    pub partitions: HashMap<Arc<str>, Partition>,
}

#[derive(Clone, Debug, Default)]
pub struct Partition {
    pub chunks: HashMap<u32, CatalogParquetInfo>,
}

/// In-memory catalog state, for testing.
#[derive(Clone, Debug, Default)]
pub struct TestCatalogState {
    /// Map of all parquet files that are currently pregistered.
    pub tables: HashMap<Arc<str>, Table>,
}

impl TestCatalogState {
    /// Simple way to create [`CheckpointData`].
    pub fn checkpoint_data(&self) -> CheckpointData {
        CheckpointData {
            files: self
                .files()
                .map(|info| (info.path.clone(), info.clone()))
                .collect(),
        }
    }

    /// Returns an iterator over the files in this catalog state
    pub fn files(&self) -> impl Iterator<Item = &CatalogParquetInfo> {
        self.tables.values().flat_map(|table| {
            table
                .partitions
                .values()
                .flat_map(|partition| partition.chunks.values())
        })
    }

    /// Inserts a file into this catalog state
    pub fn insert(&mut self, info: CatalogParquetInfo) -> Result<(), CatalogStateAddError> {
        use crate::catalog::interface::MetadataExtractFailed;

        let iox_md = info
            .metadata
            .decode()
            .context(MetadataExtractFailed {
                path: info.path.clone(),
            })?
            .read_iox_metadata()
            .context(MetadataExtractFailed {
                path: info.path.clone(),
            })?;

        let table = self.tables.entry(iox_md.table_name).or_default();
        let partition = table.partitions.entry(iox_md.partition_key).or_default();

        match partition.chunks.entry(iox_md.chunk_id) {
            Occupied(o) => {
                return Err(CatalogStateAddError::ParquetFileAlreadyExists {
                    path: o.get().path.clone(),
                });
            }
            Vacant(v) => v.insert(info),
        };

        Ok(())
    }
}

impl CatalogState for TestCatalogState {
    type EmptyInput = ();

    fn new_empty(_db_name: &str, _data: Self::EmptyInput) -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    fn add(
        &mut self,
        _iox_object_store: Arc<IoxObjectStore>,
        info: CatalogParquetInfo,
    ) -> Result<(), CatalogStateAddError> {
        self.insert(info)
    }

    fn remove(&mut self, path: &ParquetFilePath) -> Result<(), CatalogStateRemoveError> {
        let partitions = self
            .tables
            .values_mut()
            .flat_map(|table| table.partitions.values_mut());
        let mut removed = 0;

        for partition in partitions {
            let to_remove: Vec<_> = partition
                .chunks
                .iter()
                .filter_map(|(id, chunk)| {
                    if &chunk.path == path {
                        return Some(*id);
                    }
                    None
                })
                .collect();

            for id in to_remove {
                removed += 1;
                partition.chunks.remove(&id).unwrap();
            }
        }

        match removed {
            0 => Err(CatalogStateRemoveError::ParquetFileDoesNotExist { path: path.clone() }),
            _ => Ok(()),
        }
    }
}

/// Break preserved catalog by moving one of the transaction files into a weird unknown version.
pub async fn break_catalog_with_weird_version(catalog: &PreservedCatalog) {
    let tkey = get_tkey(catalog);
    let path = TransactionFilePath::new_transaction(tkey.revision_counter, tkey.uuid);
    let mut proto = load_transaction_proto(&catalog.iox_object_store(), &path)
        .await
        .unwrap();
    proto.version = 42;
    store_transaction_proto(&catalog.iox_object_store(), &path, &proto)
        .await
        .unwrap();
}

/// Helper function to ensure that guards don't leak into the future state machine.
fn get_tkey(catalog: &PreservedCatalog) -> TransactionKey {
    let revision_counter = catalog.revision_counter();
    let uuid = catalog.revision_uuid();
    TransactionKey {
        revision_counter,
        uuid,
    }
}

/// Torture-test implementations for [`CatalogState`].
///
/// A function to extract [`CheckpointData`] from the [`CatalogState`] must be provided.
pub async fn assert_catalog_state_implementation<S, F>(state_data: S::EmptyInput, f: F)
where
    S: CatalogState + Debug + Send + Sync,
    F: Fn(&S) -> CheckpointData + Send,
{
    // empty state
    let iox_object_store = make_iox_object_store().await;
    let (_catalog, mut state) =
        PreservedCatalog::new_empty::<S>(Arc::clone(&iox_object_store), state_data)
            .await
            .unwrap();

    // The expected state of the catalog
    let mut expected: HashMap<u32, (ParquetFilePath, Arc<IoxParquetMetaData>)> = HashMap::new();
    assert_checkpoint(&state, &f, &expected);

    // add files
    {
        for chunk_id in 0..5 {
            let (path, metadata) = make_metadata(
                &iox_object_store,
                "ok",
                chunk_addr(chunk_id),
                TestSize::Full,
            )
            .await;
            state
                .add(
                    Arc::clone(&iox_object_store),
                    CatalogParquetInfo {
                        path: path.clone(),
                        file_size_bytes: 33,
                        metadata: Arc::new(metadata.clone()),
                    },
                )
                .unwrap();
            expected.insert(chunk_id, (path, Arc::new(metadata)));
        }
    }
    assert_checkpoint(&state, &f, &expected);

    // remove files
    {
        let (path, _) = expected.remove(&1).unwrap();
        state.remove(&path).unwrap();
    }
    assert_checkpoint(&state, &f, &expected);

    // add and remove in the same transaction
    {
        let (path, metadata) =
            make_metadata(&iox_object_store, "ok", chunk_addr(5), TestSize::Full).await;
        state
            .add(
                Arc::clone(&iox_object_store),
                CatalogParquetInfo {
                    path: path.clone(),
                    file_size_bytes: 33,
                    metadata: Arc::new(metadata),
                },
            )
            .unwrap();
        state.remove(&path).unwrap();
    }
    assert_checkpoint(&state, &f, &expected);

    // remove and add in the same transaction
    {
        let (path, metadata) = expected.get(&3).unwrap();
        state.remove(path).unwrap();
        state
            .add(
                Arc::clone(&iox_object_store),
                CatalogParquetInfo {
                    path: path.clone(),
                    file_size_bytes: 33,
                    metadata: Arc::clone(metadata),
                },
            )
            .unwrap();
    }
    assert_checkpoint(&state, &f, &expected);

    // add, remove, add in the same transaction
    {
        let (path, metadata) =
            make_metadata(&iox_object_store, "ok", chunk_addr(6), TestSize::Full).await;
        state
            .add(
                Arc::clone(&iox_object_store),
                CatalogParquetInfo {
                    path: path.clone(),
                    file_size_bytes: 33,
                    metadata: Arc::new(metadata.clone()),
                },
            )
            .unwrap();
        state.remove(&path).unwrap();
        state
            .add(
                Arc::clone(&iox_object_store),
                CatalogParquetInfo {
                    path: path.clone(),
                    file_size_bytes: 33,
                    metadata: Arc::new(metadata.clone()),
                },
            )
            .unwrap();
        expected.insert(6, (path, Arc::new(metadata)));
    }
    assert_checkpoint(&state, &f, &expected);

    // remove, add, remove in same transaction
    {
        let (path, metadata) = expected.remove(&4).unwrap();
        state.remove(&path).unwrap();
        state
            .add(
                Arc::clone(&iox_object_store),
                CatalogParquetInfo {
                    path: path.clone(),
                    file_size_bytes: 33,
                    metadata: Arc::clone(&metadata),
                },
            )
            .unwrap();
        state.remove(&path).unwrap();
    }
    assert_checkpoint(&state, &f, &expected);

    // error handling, no real opt
    {
        // TODO: Error handling should disambiguate between chunk collision and filename collision

        // chunk with same ID already exists (should also not change the metadata)
        let (path, metadata) =
            make_metadata(&iox_object_store, "fail", chunk_addr(0), TestSize::Full).await;
        let err = state
            .add(
                Arc::clone(&iox_object_store),
                CatalogParquetInfo {
                    path: path.clone(),
                    file_size_bytes: 33,
                    metadata: Arc::new(metadata),
                },
            )
            .unwrap_err();
        assert!(matches!(
            err,
            CatalogStateAddError::ParquetFileAlreadyExists { .. }
        ));

        // does not exist as has a different UUID
        let err = state.remove(&path).unwrap_err();
        assert!(matches!(
            err,
            CatalogStateRemoveError::ParquetFileDoesNotExist { .. }
        ));
    }
    assert_checkpoint(&state, &f, &expected);

    // error handling, still something works
    {
        // already exists (should also not change the metadata)
        let (_, metadata) = expected.get(&0).unwrap();
        let err = state
            .add(
                Arc::clone(&iox_object_store),
                CatalogParquetInfo {
                    // Intentionally "incorrect" path
                    path: ParquetFilePath::new(&chunk_addr(10)),
                    file_size_bytes: 33,
                    metadata: Arc::clone(metadata),
                },
            )
            .unwrap_err();
        assert!(matches!(
            err,
            CatalogStateAddError::ParquetFileAlreadyExists { .. }
        ));

        // this transaction will still work
        let (path, metadata) =
            make_metadata(&iox_object_store, "ok", chunk_addr(7), TestSize::Full).await;
        let metadata = Arc::new(metadata);
        state
            .add(
                Arc::clone(&iox_object_store),
                CatalogParquetInfo {
                    path: path.clone(),
                    file_size_bytes: 33,
                    metadata: Arc::clone(&metadata),
                },
            )
            .unwrap();
        expected.insert(7, (path.clone(), Arc::clone(&metadata)));

        // recently added
        let err = state
            .add(
                Arc::clone(&iox_object_store),
                CatalogParquetInfo {
                    path,
                    file_size_bytes: 33,
                    metadata: Arc::clone(&metadata),
                },
            )
            .unwrap_err();
        assert!(matches!(
            err,
            CatalogStateAddError::ParquetFileAlreadyExists { .. }
        ));

        // does not exist - as different UUID
        let path = ParquetFilePath::new(&chunk_addr(7));
        let err = state.remove(&path).unwrap_err();
        assert!(matches!(
            err,
            CatalogStateRemoveError::ParquetFileDoesNotExist { .. }
        ));

        // this still works
        let (path, _) = expected.remove(&7).unwrap();
        state.remove(&path).unwrap();

        // recently removed
        let err = state.remove(&path).unwrap_err();
        assert!(matches!(
            err,
            CatalogStateRemoveError::ParquetFileDoesNotExist { .. }
        ));
    }
    assert_checkpoint(&state, &f, &expected);
}

/// Assert that tracked files and their linked metadata are equal.
fn assert_checkpoint<S, F>(
    state: &S,
    f: &F,
    expected_files: &HashMap<u32, (ParquetFilePath, Arc<IoxParquetMetaData>)>,
) where
    F: Fn(&S) -> CheckpointData,
{
    let actual_files: HashMap<ParquetFilePath, CatalogParquetInfo> = f(state).files;

    let sorted_keys_actual = get_sorted_keys(actual_files.keys());
    let sorted_keys_expected = get_sorted_keys(expected_files.values().map(|(path, _)| path));
    assert_eq!(sorted_keys_actual, sorted_keys_expected);

    for (path, md_expected) in expected_files.values() {
        let md_actual = &actual_files[path].metadata;

        let md_actual = md_actual.decode().unwrap();
        let md_expected = md_expected.decode().unwrap();

        let iox_md_actual = md_actual.read_iox_metadata().unwrap();
        let iox_md_expected = md_expected.read_iox_metadata().unwrap();
        assert_eq!(iox_md_actual, iox_md_expected);

        let schema_actual = md_actual.read_schema().unwrap();
        let schema_expected = md_expected.read_schema().unwrap();
        assert_eq!(schema_actual, schema_expected);

        let stats_actual = md_actual.read_statistics(&schema_actual).unwrap();
        let stats_expected = md_expected.read_statistics(&schema_expected).unwrap();
        assert_eq!(stats_actual, stats_expected);
    }
}

/// Get a sorted list of keys from an iterator.
fn get_sorted_keys<'a>(
    keys: impl Iterator<Item = &'a ParquetFilePath>,
) -> Vec<&'a ParquetFilePath> {
    let mut keys: Vec<_> = keys.collect();
    keys.sort();
    keys
}
