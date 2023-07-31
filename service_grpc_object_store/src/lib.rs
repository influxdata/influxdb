//! gRPC service for getting files from the object store a remote IOx service is connected to. Used
//! in router, but can be included in any gRPC server.

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

use futures::{stream::BoxStream, StreamExt};
use generated_types::influxdata::iox::object_store::v1::*;
use iox_catalog::interface::Catalog;
use object_store::DynObjectStore;
use observability_deps::tracing::*;
use parquet_file::ParquetFilePath;
use std::sync::Arc;
use tonic::{Request, Response, Status};
use uuid::Uuid;

/// Implementation of the ObjectStore gRPC service
#[derive(Debug)]
pub struct ObjectStoreService {
    /// Catalog
    catalog: Arc<dyn Catalog>,
    /// The object store
    object_store: Arc<DynObjectStore>,
}

impl ObjectStoreService {
    /// Create a new object store service with the given catalog and object store
    pub fn new(catalog: Arc<dyn Catalog>, object_store: Arc<DynObjectStore>) -> Self {
        Self {
            catalog,
            object_store,
        }
    }
}

#[tonic::async_trait]
impl object_store_service_server::ObjectStoreService for ObjectStoreService {
    type GetParquetFileByObjectStoreIdStream =
        BoxStream<'static, Result<GetParquetFileByObjectStoreIdResponse, Status>>;

    async fn get_parquet_file_by_object_store_id(
        &self,
        request: Request<GetParquetFileByObjectStoreIdRequest>,
    ) -> Result<Response<Self::GetParquetFileByObjectStoreIdStream>, Status> {
        let mut repos = self.catalog.repositories().await;
        let req = request.into_inner();
        let object_store_id =
            Uuid::parse_str(&req.uuid).map_err(|e| Status::invalid_argument(e.to_string()))?;

        let parquet_file = repos
            .parquet_files()
            .get_by_object_store_id(object_store_id)
            .await
            .map_err(|e| {
                warn!(error=%e, %req.uuid, "failed to get parquet_file by object store id");
                Status::unknown(e.to_string())
            })?
            .ok_or_else(|| Status::not_found(req.uuid))?;

        let path = ParquetFilePath::new(
            parquet_file.namespace_id,
            parquet_file.table_id,
            &parquet_file.partition_id.clone(),
            parquet_file.object_store_id,
        );
        let path = path.object_store_path();

        let res = self
            .object_store
            .get(&path)
            .await
            .map_err(|e| Status::unknown(e.to_string()))?;

        let rx = Box::pin(res.into_stream().map(|next| match next {
            Ok(data) => Ok(GetParquetFileByObjectStoreIdResponse {
                data: data.to_vec(),
            }),
            Err(e) => Err(Status::unknown(e.to_string())),
        }));

        Ok(Response::new(rx))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use data_types::{ColumnId, ColumnSet, CompactionLevel, ParquetFileParams, Timestamp};
    use generated_types::influxdata::iox::object_store::v1::object_store_service_server::ObjectStoreService;
    use iox_catalog::{
        mem::MemCatalog,
        test_helpers::{arbitrary_namespace, arbitrary_table},
    };
    use object_store::{memory::InMemory, ObjectStore};
    use uuid::Uuid;

    #[tokio::test]
    async fn test_get_parquet_file_by_object_store_id() {
        // create a catalog and populate it with some test data, then drop the write lock
        let p1;
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

            p1 = repos.parquet_files().create(p1params).await.unwrap();
            Arc::clone(&catalog)
        };

        let object_store = Arc::new(InMemory::new());

        let path = ParquetFilePath::new(
            p1.namespace_id,
            p1.table_id,
            &p1.partition_id.clone(),
            p1.object_store_id,
        );
        let path = path.object_store_path();

        let data = Bytes::from_static(b"some data");

        object_store.put(&path, data.clone()).await.unwrap();

        let grpc = super::ObjectStoreService::new(catalog, object_store);
        let request = GetParquetFileByObjectStoreIdRequest {
            uuid: p1.object_store_id.to_string(),
        };

        let tonic_response = grpc
            .get_parquet_file_by_object_store_id(Request::new(request))
            .await
            .expect("rpc request should succeed");
        let mut response = tonic_response.into_inner();
        let response = response.next().await.unwrap().unwrap();

        assert_eq!(response.data, data);
    }
}
