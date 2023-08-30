//! gRPC service for the Catalog.

#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    clippy::explicit_iter_loop,
    // See https://github.com/influxdata/influxdb_iox/pull/1671
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr,
    clippy::todo,
    clippy::dbg_macro,
    unused_crate_dependencies
)]

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

use data_types::{TableId, TransitionPartitionId};
use generated_types::influxdata::iox::catalog::v1::*;
use iox_catalog::interface::{Catalog, SoftDeletedRows};
use observability_deps::tracing::*;
use std::sync::Arc;
use tonic::{Request, Response, Status};

/// Implementation of the Catalog gRPC service
#[derive(Debug)]
pub struct CatalogService {
    /// Catalog.
    catalog: Arc<dyn Catalog>,
}

impl CatalogService {
    /// Create a new catalog service with the given catalog
    pub fn new(catalog: Arc<dyn Catalog>) -> Self {
        Self { catalog }
    }
}

#[tonic::async_trait]
impl catalog_service_server::CatalogService for CatalogService {
    async fn get_parquet_files_by_partition_id(
        &self,
        request: Request<GetParquetFilesByPartitionIdRequest>,
    ) -> Result<Response<GetParquetFilesByPartitionIdResponse>, Status> {
        let mut repos = self.catalog.repositories().await;
        let req = request.into_inner();
        let partition_id = req
            .partition_identifier
            .map(TransitionPartitionId::try_from)
            .ok_or_else(|| Status::invalid_argument("no partition id specified"))?
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        let parquet_files = repos
            .parquet_files()
            .list_by_partition_not_to_delete(&partition_id)
            .await
            .map_err(|e| {
                warn!(error=%e, %partition_id, "failed to get parquet_files for partition");
                Status::not_found(e.to_string())
            })?;

        let parquet_files: Vec<_> = parquet_files.into_iter().map(Into::into).collect();

        let response = GetParquetFilesByPartitionIdResponse { parquet_files };

        Ok(Response::new(response))
    }

    async fn get_partitions_by_table_id(
        &self,
        request: Request<GetPartitionsByTableIdRequest>,
    ) -> Result<Response<GetPartitionsByTableIdResponse>, Status> {
        let mut repos = self.catalog.repositories().await;
        let req = request.into_inner();
        let table_id = TableId::new(req.table_id);

        let partitions = repos
            .partitions()
            .list_by_table_id(table_id)
            .await
            .map_err(|e| Status::unknown(e.to_string()))?;

        let partitions: Vec<_> = partitions.into_iter().map(to_partition).collect();

        let response = GetPartitionsByTableIdResponse { partitions };

        Ok(Response::new(response))
    }

    async fn get_parquet_files_by_namespace_table(
        &self,
        request: Request<GetParquetFilesByNamespaceTableRequest>,
    ) -> Result<Response<GetParquetFilesByNamespaceTableResponse>, Status> {
        let mut repos = self.catalog.repositories().await;
        let req = request.into_inner();

        let namespace = repos
            .namespaces()
            .get_by_name(&req.namespace_name, SoftDeletedRows::ExcludeDeleted)
            .await
            .map_err(|e| Status::unknown(e.to_string()))?
            .ok_or_else(|| {
                Status::not_found(format!("Namespace {} not found", req.namespace_name))
            })?;

        let table = repos
            .tables()
            .get_by_namespace_and_name(namespace.id, &req.table_name)
            .await
            .map_err(|e| Status::unknown(e.to_string()))?
            .ok_or_else(|| Status::not_found(format!("Table {} not found", req.table_name)))?;

        let table_id = table.id;

        let parquet_files = repos
            .parquet_files()
            .list_by_table_not_to_delete(table_id)
            .await
            .map_err(|e| {
                warn!(
                    error=%e,
                    %req.namespace_name,
                    %req.table_name,
                    "failed to get parquet_files for table"
                );
                Status::not_found(e.to_string())
            })?;

        let parquet_files: Vec<_> = parquet_files.into_iter().map(Into::into).collect();

        let response = GetParquetFilesByNamespaceTableResponse { parquet_files };

        Ok(Response::new(response))
    }

    async fn get_parquet_files_by_namespace(
        &self,
        request: Request<GetParquetFilesByNamespaceRequest>,
    ) -> Result<Response<GetParquetFilesByNamespaceResponse>, Status> {
        let mut repos = self.catalog.repositories().await;
        let req = request.into_inner();

        let namespace = repos
            .namespaces()
            .get_by_name(&req.namespace_name, SoftDeletedRows::ExcludeDeleted)
            .await
            .map_err(|e| Status::unknown(e.to_string()))?
            .ok_or_else(|| {
                Status::not_found(format!("Namespace {} not found", req.namespace_name))
            })?;

        let parquet_files = repos
            .parquet_files()
            .list_by_namespace_not_to_delete(namespace.id)
            .await
            .map_err(|e| {
                warn!(
                    error=%e,
                    %req.namespace_name,
                    "failed to get parquet_files for namespace"
                );
                Status::not_found(e.to_string())
            })?;

        let parquet_files: Vec<_> = parquet_files.into_iter().map(Into::into).collect();

        let response = GetParquetFilesByNamespaceResponse { parquet_files };

        Ok(Response::new(response))
    }
}

// converts the catalog Partition to protobuf
fn to_partition(p: data_types::Partition) -> Partition {
    let identifier = PartitionIdentifier::from(p.transition_partition_id());

    let array_sort_key_ids = p
        .sort_key_ids
        .map(|cols| cols.iter().map(|id| id.get()).collect::<Vec<_>>());

    let array_sort_key_ids = match array_sort_key_ids {
        None => vec![],
        Some(array_sort_key_ids) => array_sort_key_ids,
    };

    let proto_sort_key_id = SortKeyIds { array_sort_key_ids };

    Partition {
        identifier: Some(identifier),
        key: p.partition_key.to_string(),
        table_id: p.table_id.get(),
        array_sort_key: p.sort_key,
        sort_key_ids: Some(proto_sort_key_id),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use data_types::{ColumnId, ColumnSet, CompactionLevel, ParquetFileParams, Timestamp};
    use generated_types::influxdata::iox::catalog::v1::catalog_service_server::CatalogService;
    use iox_catalog::{
        mem::MemCatalog,
        test_helpers::{arbitrary_namespace, arbitrary_table},
    };
    use uuid::Uuid;

    #[tokio::test]
    async fn get_parquet_files_by_partition_id() {
        // create a catalog and populate it with some test data, then drop the write lock
        let partition_id;
        let p1;
        let p2;
        let catalog = {
            let metrics = Arc::new(metric::Registry::default());
            let catalog = Arc::new(MemCatalog::new(metrics));
            let mut repos = catalog.repositories().await;
            let namespace = arbitrary_namespace(&mut *repos, "catalog_partition_test").await;
            let table = arbitrary_table(&mut *repos, "schema_test_table", &namespace).await;
            let partition = repos
                .partitions()
                .create_or_get("foo".into(), table.id)
                .await
                .unwrap();
            // Test: sort_key_ids from create_or_get in catalog_service
            assert!(partition.sort_key_ids().unwrap().is_empty());
            let p1params = ParquetFileParams {
                namespace_id: namespace.id,
                table_id: table.id,
                partition_id: partition.transition_partition_id(),
                object_store_id: Uuid::new_v4(),
                min_time: Timestamp::new(1),
                max_time: Timestamp::new(5),
                file_size_bytes: 2343,
                row_count: 29,
                compaction_level: CompactionLevel::Initial,
                created_at: Timestamp::new(2343),
                column_set: ColumnSet::new([ColumnId::new(1), ColumnId::new(2)]),
                max_l0_created_at: Timestamp::new(2343),
            };
            let p2params = ParquetFileParams {
                object_store_id: Uuid::new_v4(),
                ..p1params.clone()
            };
            p1 = repos.parquet_files().create(p1params).await.unwrap();
            p2 = repos.parquet_files().create(p2params).await.unwrap();
            partition_id = partition.transition_partition_id();
            Arc::clone(&catalog)
        };

        let partition_identifier = PartitionIdentifier::from(partition_id);

        let grpc = super::CatalogService::new(catalog);
        let request = GetParquetFilesByPartitionIdRequest {
            partition_identifier: Some(partition_identifier),
        };

        let tonic_response = grpc
            .get_parquet_files_by_partition_id(Request::new(request))
            .await
            .expect("rpc request should succeed");
        let response = tonic_response.into_inner();
        let expect: Vec<ParquetFile> = [p1, p2].into_iter().map(Into::into).collect();
        assert_eq!(expect, response.parquet_files,);
    }

    #[tokio::test]
    async fn get_partitions_by_table_id() {
        // create a catalog and populate it with some test data, then drop the write lock
        let table_id;
        let partition1;
        let partition2;
        let catalog = {
            let metrics = Arc::new(metric::Registry::default());
            let catalog = Arc::new(MemCatalog::new(metrics));
            let mut repos = catalog.repositories().await;
            let namespace = arbitrary_namespace(&mut *repos, "catalog_partition_test").await;
            let table = arbitrary_table(&mut *repos, "schema_test_table", &namespace).await;
            partition1 = repos
                .partitions()
                .create_or_get("foo".into(), table.id)
                .await
                .unwrap();

            partition2 = repos
                .partitions()
                .create_or_get("bar".into(), table.id)
                .await
                .unwrap();

            table_id = table.id;
            Arc::clone(&catalog)
        };

        let grpc = super::CatalogService::new(catalog);
        let request = GetPartitionsByTableIdRequest {
            table_id: table_id.get(),
        };

        let tonic_response = grpc
            .get_partitions_by_table_id(Request::new(request))
            .await
            .expect("rpc request should succeed");
        let response = tonic_response.into_inner();
        let expect: Vec<_> = [partition1, partition2]
            .into_iter()
            .map(to_partition)
            .collect();
        assert_eq!(expect, response.partitions);
    }
}
