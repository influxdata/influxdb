//! Functionality to load a [`Catalog`](crate::db::catalog::Catalog) and other information from a
//! [`PreservedCatalog`](parquet_file::catalog::PreservedCatalog).

use std::sync::Arc;

use data_types::server_id::ServerId;
use metrics::{KeyValue, MetricRegistry};
use object_store::{path::parsed::DirsAndFileName, ObjectStore};
use observability_deps::tracing::{error, info};
use parquet_file::{
    catalog::{CatalogParquetInfo, CatalogState, ChunkCreationFailed, PreservedCatalog},
    chunk::{ChunkMetrics as ParquetChunkMetrics, ParquetChunk},
};
use snafu::ResultExt;

use crate::db::catalog::{chunk::ChunkStage, table::TableSchemaUpsertHandle};

use super::catalog::Catalog;

/// Load preserved catalog state from store.
///
/// If no catalog exists yet, a new one will be created.
///
/// **For now, if the catalog is broken, it will be wiped! (https://github.com/influxdata/influxdb_iox/issues/1522)**
pub async fn load_or_create_preserved_catalog(
    db_name: &str,
    object_store: Arc<ObjectStore>,
    server_id: ServerId,
    metrics_registry: Arc<MetricRegistry>,
    wipe_on_error: bool,
) -> std::result::Result<(PreservedCatalog, Catalog), parquet_file::catalog::Error> {
    // first try to load existing catalogs
    match PreservedCatalog::load(
        Arc::clone(&object_store),
        server_id,
        db_name.to_string(),
        CatalogEmptyInput::new(db_name, server_id, Arc::clone(&metrics_registry)),
    )
    .await
    {
        Ok(Some(catalog)) => {
            // successfull load
            info!("Found existing catalog for DB {}", db_name);
            Ok(catalog)
        }
        Ok(None) => {
            // no catalog yet => create one
            info!(
                "Found NO existing catalog for DB {}, creating new one",
                db_name
            );

            create_preserved_catalog(
                db_name,
                Arc::clone(&object_store),
                server_id,
                Arc::clone(&metrics_registry),
            )
            .await
        }
        Err(e) => {
            if wipe_on_error {
                // https://github.com/influxdata/influxdb_iox/issues/1522)
                // broken => wipe for now (at least during early iterations)
                error!("cannot load catalog, so wipe it: {}", e);

                PreservedCatalog::wipe(&object_store, server_id, db_name).await?;

                create_preserved_catalog(
                    db_name,
                    Arc::clone(&object_store),
                    server_id,
                    Arc::clone(&metrics_registry),
                )
                .await
            } else {
                Err(e)
            }
        }
    }
}

/// Create new empty in-mem and perserved catalog.
///
/// This will fail if a preserved catalog already exists.
pub async fn create_preserved_catalog(
    db_name: &str,
    object_store: Arc<ObjectStore>,
    server_id: ServerId,
    metrics_registry: Arc<MetricRegistry>,
) -> std::result::Result<(PreservedCatalog, Catalog), parquet_file::catalog::Error> {
    PreservedCatalog::new_empty(
        Arc::clone(&object_store),
        server_id,
        db_name.to_string(),
        CatalogEmptyInput::new(db_name, server_id, Arc::clone(&metrics_registry)),
    )
    .await
}

/// All input required to create an empty [`Catalog`](crate::db::catalog::Catalog).
#[derive(Debug)]
pub struct CatalogEmptyInput {
    domain: ::metrics::Domain,
    metrics_registry: Arc<::metrics::MetricRegistry>,
    metric_labels: Vec<KeyValue>,
}

impl CatalogEmptyInput {
    fn new(db_name: &str, server_id: ServerId, metrics_registry: Arc<MetricRegistry>) -> Self {
        let metric_labels = vec![
            KeyValue::new("db_name", db_name.to_string()),
            KeyValue::new("svr_id", format!("{}", server_id)),
        ];
        let domain = metrics_registry.register_domain_with_labels("catalog", metric_labels.clone());

        Self {
            domain,
            metrics_registry,
            metric_labels,
        }
    }
}

impl CatalogState for Catalog {
    type EmptyInput = CatalogEmptyInput;

    fn new_empty(db_name: &str, data: Self::EmptyInput) -> Self {
        Self::new(
            Arc::from(db_name),
            data.domain,
            data.metrics_registry,
            data.metric_labels,
        )
    }

    fn add(
        &mut self,
        object_store: Arc<ObjectStore>,
        info: CatalogParquetInfo,
    ) -> parquet_file::catalog::Result<()> {
        use parquet_file::catalog::{MetadataExtractFailed, SchemaError};

        // extract relevant bits from parquet file metadata
        let iox_md = info
            .metadata
            .read_iox_metadata()
            .context(MetadataExtractFailed {
                path: info.path.clone(),
            })?;

        // Create a parquet chunk for this chunk
        let metrics = self
            .metrics_registry
            .register_domain_with_labels("parquet", self.metric_labels.clone());

        let metrics = ParquetChunkMetrics::new(&metrics, self.metrics().memory().parquet());
        let parquet_chunk = ParquetChunk::new(
            object_store.path_from_dirs_and_filename(info.path.clone()),
            object_store,
            info.metadata,
            metrics,
        )
        .context(ChunkCreationFailed {
            path: info.path.clone(),
        })?;
        let parquet_chunk = Arc::new(parquet_chunk);

        // Get partition from the catalog
        // Note that the partition might not exist yet if the chunk is loaded from an existing preserved catalog.
        let (partition, table_schema) =
            self.get_or_create_partition(&iox_md.table_name, &iox_md.partition_key);
        let mut partition = partition.write();
        if partition.chunk(iox_md.chunk_id).is_some() {
            return Err(parquet_file::catalog::Error::ParquetFileAlreadyExists { path: info.path });
        }
        let schema_handle = TableSchemaUpsertHandle::new(&table_schema, &parquet_chunk.schema())
            .map_err(|e| Box::new(e) as _)
            .context(SchemaError { path: info.path })?;
        partition.insert_object_store_only_chunk(iox_md.chunk_id, parquet_chunk);
        schema_handle.commit();

        Ok(())
    }

    fn remove(&mut self, path: DirsAndFileName) -> parquet_file::catalog::Result<()> {
        let mut removed_any = false;

        for partition in self.partitions() {
            let mut partition = partition.write();
            let mut to_remove = vec![];

            for chunk in partition.chunks() {
                let chunk = chunk.read();
                if let ChunkStage::Persisted { parquet, .. } = chunk.stage() {
                    let chunk_path: DirsAndFileName = parquet.path().into();
                    if path == chunk_path {
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
            Err(parquet_file::catalog::Error::ParquetFileDoesNotExist { path })
        }
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;

    use object_store::memory::InMemory;
    use parquet_file::catalog::test_helpers::{
        assert_catalog_state_implementation, TestCatalogState,
    };

    use crate::db::checkpoint_data_from_catalog;

    use super::*;

    #[tokio::test]
    async fn load_or_create_preserved_catalog_recovers_from_error() {
        let object_store = Arc::new(ObjectStore::new_in_memory(InMemory::new()));
        let server_id = ServerId::try_from(1).unwrap();
        let db_name = "preserved_catalog_test";

        let (preserved_catalog, _catalog) = PreservedCatalog::new_empty::<TestCatalogState>(
            Arc::clone(&object_store),
            server_id,
            db_name.to_string(),
            (),
        )
        .await
        .unwrap();
        parquet_file::catalog::test_helpers::break_catalog_with_weird_version(&preserved_catalog)
            .await;

        let metrics_registry = Arc::new(metrics::MetricRegistry::new());
        load_or_create_preserved_catalog(db_name, object_store, server_id, metrics_registry, true)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_catalog_state() {
        let metrics_registry = Arc::new(::metrics::MetricRegistry::new());
        let empty_input = CatalogEmptyInput {
            domain: metrics_registry.register_domain("catalog"),
            metrics_registry,
            metric_labels: vec![],
        };
        assert_catalog_state_implementation::<Catalog, _>(
            empty_input,
            checkpoint_data_from_catalog,
        )
        .await;
    }
}
