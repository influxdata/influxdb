//! Functionality to load a [`Catalog`](crate::db::catalog::Catalog) and other information from a
//! [`PreservedCatalog`](parquet_file::catalog::core::PreservedCatalog).

use super::catalog::{chunk::ChunkStage, table::TableSchemaUpsertHandle, Catalog};
use iox_object_store::{IoxObjectStore, ParquetFilePath};
use observability_deps::tracing::{error, info};
use parquet_file::{
    catalog::{
        core::PreservedCatalog,
        interface::{
            CatalogParquetInfo, CatalogState, CatalogStateAddError, CatalogStateRemoveError,
            ChunkCreationFailed,
        },
    },
    chunk::{ChunkMetrics as ParquetChunkMetrics, ParquetChunk},
};
use persistence_windows::checkpoint::{ReplayPlan, ReplayPlanner};
use predicate::predicate::Predicate;
use snafu::{ResultExt, Snafu};
use std::sync::Arc;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Cannot build replay plan: {}", source))]
    CannotBuildReplayPlan {
        source: persistence_windows::checkpoint::Error,
    },

    #[snafu(display("Cannot create new empty preserved catalog: {}", source))]
    CannotCreateCatalog {
        source: parquet_file::catalog::core::Error,
    },

    #[snafu(display("Cannot load preserved catalog: {}", source))]
    CannotLoadCatalog {
        source: parquet_file::catalog::core::Error,
    },

    #[snafu(display("Cannot wipe preserved catalog: {}", source))]
    CannotWipeCatalog {
        source: parquet_file::catalog::core::Error,
    },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Load preserved catalog state from store.
///
/// If no catalog exists yet, a new one will be created.
///
/// **For now, if the catalog is broken, it will be wiped!**
/// <https://github.com/influxdata/influxdb_iox/issues/1522>
pub async fn load_or_create_preserved_catalog(
    db_name: &str,
    iox_object_store: Arc<IoxObjectStore>,
    metric_registry: Arc<::metric::Registry>,
    wipe_on_error: bool,
    skip_replay: bool,
) -> Result<(PreservedCatalog, Catalog, Option<ReplayPlan>)> {
    // first try to load existing catalogs
    match PreservedCatalog::load(
        Arc::clone(&iox_object_store),
        LoaderEmptyInput::new(Arc::clone(&metric_registry), skip_replay),
    )
    .await
    {
        Ok(Some((preserved_catalog, loader))) => {
            // successfull load
            info!("Found existing catalog for DB {}", db_name);
            let Loader {
                catalog, planner, ..
            } = loader;
            let plan = planner
                .map(|planner| planner.build())
                .transpose()
                .context(CannotBuildReplayPlan)?;
            Ok((preserved_catalog, catalog, plan))
        }
        Ok(None) => {
            // no catalog yet => create one
            info!(
                "Found NO existing catalog for DB {}, creating new one",
                db_name
            );

            create_preserved_catalog(Arc::clone(&iox_object_store), metric_registry, skip_replay)
                .await
        }
        Err(e) => {
            if wipe_on_error {
                // https://github.com/influxdata/influxdb_iox/issues/1522)
                // broken => wipe for now (at least during early iterations)
                error!("cannot load catalog, so wipe it: {}", e);

                PreservedCatalog::wipe(&iox_object_store)
                    .await
                    .context(CannotWipeCatalog)?;

                create_preserved_catalog(
                    Arc::clone(&iox_object_store),
                    metric_registry,
                    skip_replay,
                )
                .await
            } else {
                Err(Error::CannotLoadCatalog { source: e })
            }
        }
    }
}

/// Create new empty in-mem and perserved catalog.
///
/// This will fail if a preserved catalog already exists.
pub async fn create_preserved_catalog(
    iox_object_store: Arc<IoxObjectStore>,
    metric_registry: Arc<metric::Registry>,
    skip_replay: bool,
) -> Result<(PreservedCatalog, Catalog, Option<ReplayPlan>)> {
    let (preserved_catalog, loader) = PreservedCatalog::new_empty(
        Arc::clone(&iox_object_store),
        LoaderEmptyInput::new(metric_registry, skip_replay),
    )
    .await
    .context(CannotCreateCatalog)?;

    let Loader {
        catalog, planner, ..
    } = loader;
    let plan = planner
        .map(|planner| planner.build())
        .transpose()
        .context(CannotBuildReplayPlan)?;
    Ok((preserved_catalog, catalog, plan))
}

/// All input required to create an empty [`Loader`]
#[derive(Debug)]
struct LoaderEmptyInput {
    metric_registry: Arc<::metric::Registry>,
    skip_replay: bool,
}

impl LoaderEmptyInput {
    fn new(metric_registry: Arc<metric::Registry>, skip_replay: bool) -> Self {
        Self {
            metric_registry,
            skip_replay,
        }
    }
}

/// Helper to track data during catalog loading.
#[derive(Debug)]
struct Loader {
    catalog: Catalog,
    planner: Option<ReplayPlanner>,
    metric_registry: Arc<metric::Registry>,
}

impl CatalogState for Loader {
    type EmptyInput = LoaderEmptyInput;

    fn new_empty(db_name: &str, data: Self::EmptyInput) -> Self {
        Self {
            catalog: Catalog::new(Arc::from(db_name), Arc::clone(&data.metric_registry)),
            planner: (!data.skip_replay).then(ReplayPlanner::new),
            metric_registry: Arc::new(Default::default()),
        }
    }

    fn add(
        &mut self,
        iox_object_store: Arc<IoxObjectStore>,
        info: CatalogParquetInfo,
    ) -> Result<(), CatalogStateAddError> {
        use parquet_file::catalog::interface::{
            MetadataExtractFailed, ReplayPlanError, SchemaError,
        };

        // extract relevant bits from parquet file metadata
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

        // remember file for replay
        if let Some(planner) = self.planner.as_mut() {
            planner
                .register_checkpoints(&iox_md.partition_checkpoint, &iox_md.database_checkpoint)
                .map_err(|e| Box::new(e) as _)
                .context(ReplayPlanError {
                    path: info.path.clone(),
                })?;
        }

        // Create a parquet chunk for this chunk
        let metrics = ParquetChunkMetrics::new(self.metric_registry.as_ref());
        let parquet_chunk = ParquetChunk::new(
            &info.path,
            iox_object_store,
            info.file_size_bytes,
            info.metadata,
            Arc::clone(&iox_md.table_name),
            Arc::clone(&iox_md.partition_key),
            metrics,
        )
        .context(ChunkCreationFailed { path: &info.path })?;
        let parquet_chunk = Arc::new(parquet_chunk);

        // Get partition from the catalog
        // Note that the partition might not exist yet if the chunk is loaded from an existing preserved catalog.
        let (partition, table_schema) = self
            .catalog
            .get_or_create_partition(&iox_md.table_name, &iox_md.partition_key);
        let mut partition = partition.write();
        if partition.chunk(iox_md.chunk_id).is_some() {
            return Err(CatalogStateAddError::ParquetFileAlreadyExists { path: info.path });
        }
        let schema_handle = TableSchemaUpsertHandle::new(&table_schema, &parquet_chunk.schema())
            .map_err(|e| Box::new(e) as _)
            .context(SchemaError { path: info.path })?;

        let delete_predicates: Arc<Vec<Predicate>> = Arc::new(vec![]); // NGA todo: After Marco save delete predicate into the catalog, it will need to extract into this variable
        partition.insert_object_store_only_chunk(
            iox_md.chunk_id,
            parquet_chunk,
            iox_md.time_of_first_write,
            iox_md.time_of_last_write,
            delete_predicates,
            iox_md.chunk_order,
        );
        schema_handle.commit();

        Ok(())
    }

    fn remove(&mut self, path: &ParquetFilePath) -> Result<(), CatalogStateRemoveError> {
        let mut removed_any = false;

        for partition in self.catalog.partitions() {
            let mut partition = partition.write();
            let mut to_remove = vec![];

            for chunk in partition.chunks() {
                let chunk = chunk.read();
                if let ChunkStage::Persisted { parquet, .. } = chunk.stage() {
                    if path == parquet.path() {
                        to_remove.push(chunk.id());
                    }
                }
            }

            for chunk_id in to_remove {
                if let Err(e) = partition.drop_chunk(chunk_id) {
                    panic!("Chunk is gone while we've had a partition lock: {}", e);
                }
                removed_any = true;
            }
        }

        if removed_any {
            Ok(())
        } else {
            Err(CatalogStateRemoveError::ParquetFileDoesNotExist { path: path.clone() })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::checkpoint_data_from_catalog;
    use data_types::{server_id::ServerId, DatabaseName};
    use object_store::ObjectStore;
    use parquet_file::catalog::{
        interface::CheckpointData,
        test_helpers::{assert_catalog_state_implementation, TestCatalogState},
    };
    use std::convert::TryFrom;

    #[tokio::test]
    async fn load_or_create_preserved_catalog_recovers_from_error() {
        let object_store = Arc::new(ObjectStore::new_in_memory());
        let server_id = ServerId::try_from(1).unwrap();
        let db_name = DatabaseName::new("preserved_catalog_test").unwrap();
        let iox_object_store = Arc::new(
            IoxObjectStore::new(object_store, server_id, &db_name)
                .await
                .unwrap(),
        );

        let (preserved_catalog, _catalog) =
            PreservedCatalog::new_empty::<TestCatalogState>(Arc::clone(&iox_object_store), ())
                .await
                .unwrap();
        parquet_file::catalog::test_helpers::break_catalog_with_weird_version(&preserved_catalog)
            .await;

        load_or_create_preserved_catalog(
            &db_name,
            iox_object_store,
            Default::default(),
            true,
            false,
        )
        .await
        .unwrap();
    }

    fn checkpoint_data_from_loader(loader: &Loader) -> CheckpointData {
        checkpoint_data_from_catalog(&loader.catalog)
    }

    #[tokio::test]
    async fn test_catalog_state() {
        let empty_input = LoaderEmptyInput {
            metric_registry: Default::default(),
            skip_replay: false,
        };
        assert_catalog_state_implementation::<Loader, _>(empty_input, checkpoint_data_from_loader)
            .await;
    }
}
