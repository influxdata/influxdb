//! gRPC service for the Catalog. Used in router, but can be included in any gRPC server.

#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    clippy::explicit_iter_loop,
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]

use data_types::{PartitionId, TableId};
use generated_types::influxdata::iox::catalog::v1::*;
use iox_catalog::interface::Catalog;
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
        let partition_id = PartitionId::new(req.partition_id);

        let parquet_files = repos
            .parquet_files()
            .list_by_partition_not_to_delete(partition_id)
            .await
            .map_err(|e| {
                warn!(error=%e, %req.partition_id, "failed to get parquet_files for partition");
                Status::not_found(e.to_string())
            })?;

        let parquet_files: Vec<_> = parquet_files.into_iter().map(to_parquet_file).collect();

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
}

// converts the catalog ParquetFile to protobuf
fn to_parquet_file(p: data_types::ParquetFile) -> ParquetFile {
    ParquetFile {
        id: p.id.get(),
        sequencer_id: p.sequencer_id.get(),
        namespace_id: p.namespace_id.get(),
        table_id: p.table_id.get(),
        partition_id: p.partition_id.get(),
        object_store_id: p.object_store_id.to_string(),
        min_sequence_number: p.min_sequence_number.get(),
        max_sequence_number: p.max_sequence_number.get(),
        min_time: p.min_time.get(),
        max_time: p.max_time.get(),
        to_delete: p.to_delete.map(|t| t.get()).unwrap_or(0),
        file_size_bytes: p.file_size_bytes,
        row_count: p.row_count,
        compaction_level: p.compaction_level as i32,
        created_at: p.created_at.get(),
    }
}

// converts the catalog Partition to protobuf
fn to_partition(p: data_types::Partition) -> Partition {
    Partition {
        id: p.id.get(),
        sequencer_id: p.sequencer_id.get(),
        key: p.partition_key,
        table_id: p.table_id.get(),
        sort_key: p.sort_key.unwrap_or_else(|| "".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use data_types::{KafkaPartition, ParquetFileParams, SequenceNumber, Timestamp};
    use generated_types::influxdata::iox::catalog::v1::catalog_service_server::CatalogService;
    use iox_catalog::mem::MemCatalog;
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
            let kafka = repos
                .kafka_topics()
                .create_or_get("iox_shared")
                .await
                .unwrap();
            let pool = repos
                .query_pools()
                .create_or_get("iox_shared")
                .await
                .unwrap();
            let sequencer = repos
                .sequencers()
                .create_or_get(&kafka, KafkaPartition::new(1))
                .await
                .unwrap();
            let namespace = repos
                .namespaces()
                .create("catalog_partition_test", "inf", kafka.id, pool.id)
                .await
                .unwrap();
            let table = repos
                .tables()
                .create_or_get("schema_test_table", namespace.id)
                .await
                .unwrap();
            let partition = repos
                .partitions()
                .create_or_get("foo", sequencer.id, table.id)
                .await
                .unwrap();
            let p1params = ParquetFileParams {
                sequencer_id: sequencer.id,
                namespace_id: namespace.id,
                table_id: table.id,
                partition_id: partition.id,
                object_store_id: Uuid::new_v4(),
                min_sequence_number: SequenceNumber::new(1),
                max_sequence_number: SequenceNumber::new(40),
                min_time: Timestamp::new(1),
                max_time: Timestamp::new(5),
                file_size_bytes: 2343,
                parquet_metadata: vec![],
                row_count: 29,
                compaction_level: 0,
                created_at: Timestamp::new(2343),
            };
            let p2params = ParquetFileParams {
                object_store_id: Uuid::new_v4(),
                min_sequence_number: SequenceNumber::new(50),
                max_sequence_number: SequenceNumber::new(70),
                ..p1params.clone()
            };
            p1 = repos.parquet_files().create(p1params).await.unwrap();
            p2 = repos.parquet_files().create(p2params).await.unwrap();
            partition_id = partition.id;
            Arc::clone(&catalog)
        };

        let grpc = super::CatalogService::new(catalog);
        let request = GetParquetFilesByPartitionIdRequest {
            partition_id: partition_id.get(),
        };

        let tonic_response = grpc
            .get_parquet_files_by_partition_id(Request::new(request))
            .await
            .expect("rpc request should succeed");
        let response = tonic_response.into_inner();
        let expect: Vec<_> = [p1, p2].into_iter().map(to_parquet_file).collect();
        assert_eq!(expect, response.parquet_files,);
    }

    #[tokio::test]
    async fn get_partitions_by_table_id() {
        // create a catalog and populate it with some test data, then drop the write lock
        let table_id;
        let partition1;
        let partition2;
        let partition3;
        let catalog = {
            let metrics = Arc::new(metric::Registry::default());
            let catalog = Arc::new(MemCatalog::new(metrics));
            let mut repos = catalog.repositories().await;
            let kafka = repos
                .kafka_topics()
                .create_or_get("iox_shared")
                .await
                .unwrap();
            let pool = repos
                .query_pools()
                .create_or_get("iox_shared")
                .await
                .unwrap();
            let sequencer = repos
                .sequencers()
                .create_or_get(&kafka, KafkaPartition::new(1))
                .await
                .unwrap();
            let namespace = repos
                .namespaces()
                .create("catalog_partition_test", "inf", kafka.id, pool.id)
                .await
                .unwrap();
            let table = repos
                .tables()
                .create_or_get("schema_test_table", namespace.id)
                .await
                .unwrap();
            partition1 = repos
                .partitions()
                .create_or_get("foo", sequencer.id, table.id)
                .await
                .unwrap();
            partition2 = repos
                .partitions()
                .create_or_get("bar", sequencer.id, table.id)
                .await
                .unwrap();
            let sequencer2 = repos
                .sequencers()
                .create_or_get(&kafka, KafkaPartition::new(2))
                .await
                .unwrap();
            partition3 = repos
                .partitions()
                .create_or_get("foo", sequencer2.id, table.id)
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
        let expect: Vec<_> = [partition1, partition2, partition3]
            .into_iter()
            .map(to_partition)
            .collect();
        assert_eq!(expect, response.partitions);
    }
}
