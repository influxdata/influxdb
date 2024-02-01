//! gRPC client implementation.
use std::future::Future;
use std::ops::ControlFlow;
use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use futures::TryStreamExt;
use log::{debug, info, warn};
use tonic::transport::{Channel, Uri};

use crate::{
    interface::{
        CasFailure, Catalog, ColumnRepo, Error, NamespaceRepo, ParquetFileRepo, PartitionRepo,
        RepoCollection, Result, SoftDeletedRows, TableRepo,
    },
    metrics::MetricDecorator,
};
use backoff::{Backoff, BackoffError};
use data_types::snapshot::partition::PartitionSnapshot;
use data_types::{
    partition_template::{NamespacePartitionTemplateOverride, TablePartitionTemplateOverride},
    snapshot::table::TableSnapshot,
    Column, ColumnType, CompactionLevel, MaxColumnsPerTable, MaxTables, Namespace, NamespaceId,
    NamespaceName, NamespaceServiceProtectionLimitsOverride, ObjectStoreId, ParquetFile,
    ParquetFileId, ParquetFileParams, Partition, PartitionId, PartitionKey, SkippedCompaction,
    SortKeyIds, Table, TableId, Timestamp,
};
use generated_types::influxdata::iox::catalog::v2 as proto;
use iox_time::TimeProvider;
use trace_http::metrics::{MetricFamily, RequestMetrics};
use trace_http::tower::TraceService;

use super::serialization::{
    convert_status, deserialize_column, deserialize_namespace, deserialize_object_store_id,
    deserialize_parquet_file, deserialize_partition, deserialize_skipped_compaction,
    deserialize_sort_key_ids, deserialize_table, serialize_column_type, serialize_object_store_id,
    serialize_parquet_file_params, serialize_soft_deleted_rows, serialize_sort_key_ids, ContextExt,
    RequiredExt,
};

type InstrumentedChannel = TraceService<Channel>;

/// Catalog that goes through a gRPC interface.
#[derive(Debug)]
pub struct GrpcCatalogClient {
    channel: InstrumentedChannel,
    metrics: Arc<metric::Registry>,
    time_provider: Arc<dyn TimeProvider>,
}

impl GrpcCatalogClient {
    /// Create new client.
    pub fn new(
        uri: Uri,
        metrics: Arc<metric::Registry>,
        time_provider: Arc<dyn TimeProvider>,
    ) -> Self {
        let channel = TraceService::new_client(
            Channel::builder(uri).connect_lazy(),
            Arc::new(RequestMetrics::new(
                Arc::clone(&metrics),
                MetricFamily::GrpcClient,
            )),
            None,
            "catalog",
        );
        Self {
            channel,
            metrics,
            time_provider,
        }
    }
}

impl std::fmt::Display for GrpcCatalogClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "grpc")
    }
}

#[async_trait]
impl Catalog for GrpcCatalogClient {
    async fn setup(&self) -> Result<(), Error> {
        Ok(())
    }

    fn repositories(&self) -> Box<dyn RepoCollection> {
        Box::new(MetricDecorator::new(
            GrpcCatalogClientRepos {
                channel: self.channel.clone(),
            },
            Arc::clone(&self.metrics),
            Arc::clone(&self.time_provider),
        ))
    }

    #[cfg(test)]
    fn metrics(&self) -> Arc<metric::Registry> {
        Arc::clone(&self.metrics)
    }

    fn time_provider(&self) -> Arc<dyn TimeProvider> {
        Arc::clone(&self.time_provider)
    }
}

#[derive(Debug)]
struct GrpcCatalogClientRepos {
    channel: InstrumentedChannel,
}

type ServiceClient = proto::catalog_service_client::CatalogServiceClient<InstrumentedChannel>;

fn is_upstream_error(e: &tonic::Status) -> bool {
    matches!(
        e.code(),
        tonic::Code::Cancelled
            | tonic::Code::DeadlineExceeded
            | tonic::Code::FailedPrecondition
            | tonic::Code::Aborted
            | tonic::Code::Unavailable
    )
}

impl GrpcCatalogClientRepos {
    fn client(&self) -> ServiceClient {
        proto::catalog_service_client::CatalogServiceClient::new(self.channel.clone())
    }

    async fn retry<U, FunIo, Fut, D>(
        &self,
        operation: &str,
        upload: U,
        fun_io: FunIo,
    ) -> Result<D, Error>
    where
        U: Clone + std::fmt::Debug + Send + Sync,
        FunIo: Fn(U, ServiceClient) -> Fut + Send + Sync,
        Fut: Future<Output = Result<tonic::Response<D>, tonic::Status>> + Send,
        D: std::fmt::Debug,
    {
        Backoff::new(&Default::default())
            .retry_with_backoff(operation, || async {
                let res = fun_io(upload.clone(), self.client()).await;
                match res {
                    Ok(r) => {
                        let r = r.into_inner();
                        debug!("{} successfully received: {:?}", operation, &r);
                        ControlFlow::Break(Ok(r))
                    }
                    Err(e) if is_upstream_error(&e) => {
                        info!("{} retriable error encountered: {:?}", operation, &e);
                        ControlFlow::Continue(e)
                    }
                    Err(e) => {
                        warn!(
                            "{operation} attempted {:?} and received error: {:?}",
                            upload, e
                        );
                        ControlFlow::Break(Err(convert_status(e)))
                    }
                }
            })
            .await
            .map_err(|be| {
                let status = match be {
                    BackoffError::DeadlineExceeded { source, .. } => source,
                };
                convert_status(status)
            })?
    }
}

impl RepoCollection for GrpcCatalogClientRepos {
    fn namespaces(&mut self) -> &mut dyn NamespaceRepo {
        self
    }

    fn tables(&mut self) -> &mut dyn TableRepo {
        self
    }

    fn columns(&mut self) -> &mut dyn ColumnRepo {
        self
    }

    fn partitions(&mut self) -> &mut dyn PartitionRepo {
        self
    }

    fn parquet_files(&mut self) -> &mut dyn ParquetFileRepo {
        self
    }
}

#[async_trait]
impl NamespaceRepo for GrpcCatalogClientRepos {
    async fn create(
        &mut self,
        name: &NamespaceName<'_>,
        partition_template: Option<NamespacePartitionTemplateOverride>,
        retention_period_ns: Option<i64>,
        service_protection_limits: Option<NamespaceServiceProtectionLimitsOverride>,
    ) -> Result<Namespace> {
        let n = proto::NamespaceCreateRequest {
            name: name.to_string(),
            partition_template: partition_template.and_then(|t| t.as_proto().cloned()),
            retention_period_ns,
            service_protection_limits: service_protection_limits.map(|l| {
                proto::ServiceProtectionLimits {
                    max_tables: l.max_tables.map(|x| x.get_i32()),
                    max_columns_per_table: l.max_columns_per_table.map(|x| x.get_i32()),
                }
            }),
        };

        let resp = self
            .retry("namespace_create", n, |data, mut client| async move {
                client.namespace_create(data).await
            })
            .await?;

        Ok(deserialize_namespace(
            resp.namespace.required().ctx("namespace")?,
        )?)
    }

    async fn update_retention_period(
        &mut self,
        name: &str,
        retention_period_ns: Option<i64>,
    ) -> Result<Namespace> {
        let n = proto::NamespaceUpdateRetentionPeriodRequest {
            name: name.to_owned(),
            retention_period_ns,
        };

        let resp = self.retry(
            "namespace_update_retention_period",
            n,
            |data, mut client| async move { client.namespace_update_retention_period(data).await },
        )
        .await?;

        Ok(deserialize_namespace(
            resp.namespace.required().ctx("namespace")?,
        )?)
    }

    async fn list(&mut self, deleted: SoftDeletedRows) -> Result<Vec<Namespace>> {
        let n = proto::NamespaceListRequest {
            deleted: serialize_soft_deleted_rows(deleted),
        };

        self.retry("namespace_list", n, |data, mut client| async move {
            client.namespace_list(data).await
        })
        .await?
        .map_err(convert_status)
        .and_then(|res| async move {
            deserialize_namespace(res.namespace.required().ctx("namespace")?).map_err(Error::from)
        })
        .try_collect()
        .await
    }

    async fn get_by_id(
        &mut self,
        id: NamespaceId,
        deleted: SoftDeletedRows,
    ) -> Result<Option<Namespace>> {
        let n = proto::NamespaceGetByIdRequest {
            id: id.get(),
            deleted: serialize_soft_deleted_rows(deleted),
        };

        let resp = self
            .retry("namespace_get_by_id", n, |data, mut client| async move {
                client.namespace_get_by_id(data).await
            })
            .await?;
        Ok(resp.namespace.map(deserialize_namespace).transpose()?)
    }

    async fn get_by_name(
        &mut self,
        name: &str,
        deleted: SoftDeletedRows,
    ) -> Result<Option<Namespace>> {
        let n = proto::NamespaceGetByNameRequest {
            name: name.to_owned(),
            deleted: serialize_soft_deleted_rows(deleted),
        };

        let resp = self
            .retry("namespace_get_by_name", n, |data, mut client| async move {
                client.namespace_get_by_name(data).await
            })
            .await?;
        Ok(resp.namespace.map(deserialize_namespace).transpose()?)
    }

    async fn soft_delete(&mut self, name: &str) -> Result<()> {
        let n = proto::NamespaceSoftDeleteRequest {
            name: name.to_owned(),
        };

        self.retry("namespace_soft_delete", n, |data, mut client| async move {
            client.namespace_soft_delete(data).await
        })
        .await?;
        Ok(())
    }

    async fn update_table_limit(&mut self, name: &str, new_max: MaxTables) -> Result<Namespace> {
        let n = proto::NamespaceUpdateTableLimitRequest {
            name: name.to_owned(),
            new_max: new_max.get_i32(),
        };

        let resp = self
            .retry("namespace_soft_delete", n, |data, mut client| async move {
                client.namespace_update_table_limit(data).await
            })
            .await?;

        Ok(deserialize_namespace(
            resp.namespace.required().ctx("namespace")?,
        )?)
    }

    async fn update_column_limit(
        &mut self,
        name: &str,
        new_max: MaxColumnsPerTable,
    ) -> Result<Namespace> {
        let n = proto::NamespaceUpdateColumnLimitRequest {
            name: name.to_owned(),
            new_max: new_max.get_i32(),
        };

        let resp = self
            .retry("namespace_soft_delete", n, |data, mut client| async move {
                client.namespace_update_column_limit(data).await
            })
            .await?;

        Ok(deserialize_namespace(
            resp.namespace.required().ctx("namespace")?,
        )?)
    }
}

#[async_trait]
impl TableRepo for GrpcCatalogClientRepos {
    async fn create(
        &mut self,
        name: &str,
        partition_template: TablePartitionTemplateOverride,
        namespace_id: NamespaceId,
    ) -> Result<Table> {
        let t = proto::TableCreateRequest {
            name: name.to_owned(),
            partition_template: partition_template.as_proto().cloned(),
            namespace_id: namespace_id.get(),
        };

        let resp = self
            .retry("table_create", t, |data, mut client| async move {
                client.table_create(data).await
            })
            .await?;
        Ok(deserialize_table(resp.table.required().ctx("table")?)?)
    }

    async fn get_by_id(&mut self, table_id: TableId) -> Result<Option<Table>> {
        let t = proto::TableGetByIdRequest { id: table_id.get() };

        let resp = self
            .retry("table_get_by_id", t, |data, mut client| async move {
                client.table_get_by_id(data).await
            })
            .await?;
        Ok(resp.table.map(deserialize_table).transpose()?)
    }

    async fn get_by_namespace_and_name(
        &mut self,
        namespace_id: NamespaceId,
        name: &str,
    ) -> Result<Option<Table>> {
        let t = proto::TableGetByNamespaceAndNameRequest {
            namespace_id: namespace_id.get(),
            name: name.to_owned(),
        };

        let resp = self.retry(
            "table_get_by_namespace_and_name",
            t,
            |data, mut client| async move { client.table_get_by_namespace_and_name(data).await },
        )
        .await?;
        Ok(resp.table.map(deserialize_table).transpose()?)
    }

    async fn list_by_namespace_id(&mut self, namespace_id: NamespaceId) -> Result<Vec<Table>> {
        let t = proto::TableListByNamespaceIdRequest {
            namespace_id: namespace_id.get(),
        };

        self.retry(
            "table_list_by_namespace_id",
            t,
            |data, mut client| async move { client.table_list_by_namespace_id(data).await },
        )
        .await?
        .map_err(convert_status)
        .and_then(|res| async move { Ok(deserialize_table(res.table.required().ctx("table")?)?) })
        .try_collect()
        .await
    }

    async fn list(&mut self) -> Result<Vec<Table>> {
        let t = proto::TableListRequest {};

        self.retry("table_list", t, |data, mut client| async move {
            client.table_list(data).await
        })
        .await?
        .map_err(convert_status)
        .and_then(|res| async move { Ok(deserialize_table(res.table.required().ctx("table")?)?) })
        .try_collect()
        .await
    }

    async fn snapshot(&mut self, table_id: TableId) -> Result<TableSnapshot> {
        let t = proto::TableSnapshotRequest {
            table_id: table_id.get(),
        };

        let resp = self
            .retry("table_snapshot", t, |data, mut client| async move {
                client.table_snapshot(data).await
            })
            .await?;

        let table = resp.table.required().ctx("table")?;
        Ok(TableSnapshot::decode(table, resp.generation))
    }
}

#[async_trait]
impl ColumnRepo for GrpcCatalogClientRepos {
    async fn create_or_get(
        &mut self,
        name: &str,
        table_id: TableId,
        column_type: ColumnType,
    ) -> Result<Column> {
        let c = proto::ColumnCreateOrGetRequest {
            name: name.to_owned(),
            table_id: table_id.get(),
            column_type: serialize_column_type(column_type),
        };

        let resp = self
            .retry("column_create_or_get", c, |data, mut client| async move {
                client.column_create_or_get(data).await
            })
            .await?;
        Ok(deserialize_column(resp.column.required().ctx("column")?)?)
    }

    async fn create_or_get_many_unchecked(
        &mut self,
        table_id: TableId,
        columns: HashMap<&str, ColumnType>,
    ) -> Result<Vec<Column>> {
        let c = proto::ColumnCreateOrGetManyUncheckedRequest {
            table_id: table_id.get(),
            columns: columns
                .into_iter()
                .map(|(name, t)| (name.to_owned(), serialize_column_type(t)))
                .collect(),
        };

        self.retry(
            "column_create_or_get_many_unchecked",
            c,
            |data, mut client| async move { client.column_create_or_get_many_unchecked(data).await },
        )
        .await?
        .map_err(convert_status)
        .and_then(|res| async move {
            Ok(deserialize_column(res.column.required().ctx("column")?)?)
        })
        .try_collect()
        .await
    }

    async fn list_by_namespace_id(&mut self, namespace_id: NamespaceId) -> Result<Vec<Column>> {
        let c = proto::ColumnListByNamespaceIdRequest {
            namespace_id: namespace_id.get(),
        };

        self.retry(
            "column_list_by_namespace_id",
            c,
            |data, mut client| async move { client.column_list_by_namespace_id(data).await },
        )
        .await?
        .map_err(convert_status)
        .and_then(
            |res| async move { Ok(deserialize_column(res.column.required().ctx("column")?)?) },
        )
        .try_collect()
        .await
    }

    async fn list_by_table_id(&mut self, table_id: TableId) -> Result<Vec<Column>> {
        let c = proto::ColumnListByTableIdRequest {
            table_id: table_id.get(),
        };

        self.retry(
            "column_list_by_table_id",
            c,
            |data, mut client| async move { client.column_list_by_table_id(data).await },
        )
        .await?
        .map_err(convert_status)
        .and_then(
            |res| async move { Ok(deserialize_column(res.column.required().ctx("column")?)?) },
        )
        .try_collect()
        .await
    }

    async fn list(&mut self) -> Result<Vec<Column>> {
        let c = proto::ColumnListRequest {};

        self.retry("column_list", c, |data, mut client| async move {
            client.column_list(data).await
        })
        .await?
        .map_err(convert_status)
        .and_then(
            |res| async move { Ok(deserialize_column(res.column.required().ctx("column")?)?) },
        )
        .try_collect()
        .await
    }
}

#[async_trait]
impl PartitionRepo for GrpcCatalogClientRepos {
    async fn create_or_get(&mut self, key: PartitionKey, table_id: TableId) -> Result<Partition> {
        let p = proto::PartitionCreateOrGetRequest {
            key: key.inner().to_owned(),
            table_id: table_id.get(),
        };

        let resp = self
            .retry(
                "partition_create_or_get",
                p,
                |data, mut client| async move { client.partition_create_or_get(data).await },
            )
            .await?;

        Ok(deserialize_partition(
            resp.partition.required().ctx("partition")?,
        )?)
    }

    async fn get_by_id_batch(&mut self, partition_ids: &[PartitionId]) -> Result<Vec<Partition>> {
        let p = proto::PartitionGetByIdBatchRequest {
            partition_ids: partition_ids.iter().map(|id| id.get()).collect(),
        };

        self.retry(
            "partition_get_by_id_batch",
            p,
            |data, mut client| async move { client.partition_get_by_id_batch(data).await },
        )
        .await?
        .map_err(convert_status)
        .and_then(|res| async move {
            Ok(deserialize_partition(
                res.partition.required().ctx("partition")?,
            )?)
        })
        .try_collect()
        .await
    }

    async fn list_by_table_id(&mut self, table_id: TableId) -> Result<Vec<Partition>> {
        let p = proto::PartitionListByTableIdRequest {
            table_id: table_id.get(),
        };

        self.retry(
            "partition_list_by_table_id",
            p,
            |data, mut client| async move { client.partition_list_by_table_id(data).await },
        )
        .await?
        .map_err(convert_status)
        .and_then(|res| async move {
            Ok(deserialize_partition(
                res.partition.required().ctx("partition")?,
            )?)
        })
        .try_collect()
        .await
    }

    async fn list_ids(&mut self) -> Result<Vec<PartitionId>> {
        let p = proto::PartitionListIdsRequest {};

        self.retry("partition_list_ids", p, |data, mut client| async move {
            client.partition_list_ids(data).await
        })
        .await?
        .map_err(convert_status)
        .map_ok(|res| PartitionId::new(res.partition_id))
        .try_collect()
        .await
    }

    async fn cas_sort_key(
        &mut self,
        partition_id: PartitionId,
        old_sort_key_ids: Option<&SortKeyIds>,
        new_sort_key_ids: &SortKeyIds,
    ) -> Result<Partition, CasFailure<SortKeyIds>> {
        // This method does not use request/request_streaming_response
        // because the error handling (converting to CasFailure) differs
        // from how all the other methods handle errors.

        let p = proto::PartitionCasSortKeyRequest {
            partition_id: partition_id.get(),
            old_sort_key_ids: old_sort_key_ids.map(serialize_sort_key_ids),
            new_sort_key_ids: Some(serialize_sort_key_ids(new_sort_key_ids)),
        };

        let res = self
            .retry("partition_cas_sort_key", p, |data, mut client| async move {
                client.partition_cas_sort_key(data).await
            })
            .await
            .map_err(CasFailure::QueryError)?;

        let res = res
            .res
            .required()
            .ctx("res")
            .map_err(|e| CasFailure::QueryError(e.into()))?;

        match res {
            proto::partition_cas_sort_key_response::Res::Partition(p) => {
                let p = deserialize_partition(p).map_err(|e| CasFailure::QueryError(e.into()))?;
                Ok(p)
            }
            proto::partition_cas_sort_key_response::Res::CurrentSortKey(k) => {
                Err(CasFailure::ValueMismatch(deserialize_sort_key_ids(k)))
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn record_skipped_compaction(
        &mut self,
        partition_id: PartitionId,
        reason: &str,
        num_files: usize,
        limit_num_files: usize,
        limit_num_files_first_in_partition: usize,
        estimated_bytes: u64,
        limit_bytes: u64,
    ) -> Result<()> {
        let p = proto::PartitionRecordSkippedCompactionRequest {
            partition_id: partition_id.get(),
            reason: reason.to_owned(),
            num_files: num_files as u64,
            limit_num_files: limit_num_files as u64,
            limit_num_files_first_in_partition: limit_num_files_first_in_partition as u64,
            estimated_bytes,
            limit_bytes,
        };

        self.retry(
            "partition_record_skipped_compaction",
            p,
            |data, mut client| async move { client.partition_record_skipped_compaction(data).await },
        )
        .await?;
        Ok(())
    }

    async fn get_in_skipped_compactions(
        &mut self,
        partition_id: &[PartitionId],
    ) -> Result<Vec<SkippedCompaction>> {
        let p = proto::PartitionGetInSkippedCompactionsRequest {
            partition_ids: partition_id.iter().map(|id| id.get()).collect(),
        };

        self.retry(
            "partition_get_in_skipped_compactions",
            p,
            |data, mut client| async move { client.partition_get_in_skipped_compactions(data).await },
        )
        .await?
            .map_err(convert_status)
            .and_then(|res| async move {
                Ok(deserialize_skipped_compaction(res.skipped_compaction.required().ctx("skipped_compaction")?))
            })
            .try_collect()
            .await
    }

    async fn list_skipped_compactions(&mut self) -> Result<Vec<SkippedCompaction>> {
        let p = proto::PartitionListSkippedCompactionsRequest {};

        self.retry(
            "partition_list_skipped_compactions",
            p,
            |data, mut client| async move { client.partition_list_skipped_compactions(data).await },
        )
        .await?
        .map_err(convert_status)
        .and_then(|res| async move {
            Ok(deserialize_skipped_compaction(
                res.skipped_compaction
                    .required()
                    .ctx("skipped_compaction")?,
            ))
        })
        .try_collect()
        .await
    }

    async fn delete_skipped_compactions(
        &mut self,
        partition_id: PartitionId,
    ) -> Result<Option<SkippedCompaction>> {
        let p = proto::PartitionDeleteSkippedCompactionsRequest {
            partition_id: partition_id.get(),
        };

        let resp = self
            .retry(
                "partition_delete_skipped_compactions",
                p,
                |data, mut client| async move {
                    client.partition_delete_skipped_compactions(data).await
                },
            )
            .await?;

        Ok(resp.skipped_compaction.map(deserialize_skipped_compaction))
    }

    async fn most_recent_n(&mut self, n: usize) -> Result<Vec<Partition>> {
        let p = proto::PartitionMostRecentNRequest { n: n as u64 };

        self.retry(
            "partition_most_recent_n",
            p,
            |data, mut client| async move { client.partition_most_recent_n(data).await },
        )
        .await?
        .map_err(convert_status)
        .and_then(|res| async move {
            Ok(deserialize_partition(
                res.partition.required().ctx("partition")?,
            )?)
        })
        .try_collect()
        .await
    }

    async fn partitions_new_file_between(
        &mut self,
        minimum_time: Timestamp,
        maximum_time: Option<Timestamp>,
    ) -> Result<Vec<PartitionId>> {
        let p = proto::PartitionNewFileBetweenRequest {
            minimum_time: minimum_time.get(),
            maximum_time: maximum_time.map(|ts| ts.get()),
        };

        self.retry(
            "partition_new_file_between",
            p,
            |data, mut client| async move { client.partition_new_file_between(data).await },
        )
        .await?
        .map_err(convert_status)
        .map_ok(|res| PartitionId::new(res.partition_id))
        .try_collect()
        .await
    }

    async fn list_old_style(&mut self) -> Result<Vec<Partition>> {
        let p = proto::PartitionListOldStyleRequest {};

        self.retry(
            "partition_list_old_style",
            p,
            |data, mut client| async move { client.partition_list_old_style(data).await },
        )
        .await?
        .map_err(convert_status)
        .and_then(|res| async move {
            Ok(deserialize_partition(
                res.partition.required().ctx("partition")?,
            )?)
        })
        .try_collect()
        .await
    }

    async fn snapshot(&mut self, partition_id: PartitionId) -> Result<PartitionSnapshot> {
        let p = proto::PartitionSnapshotRequest {
            partition_id: partition_id.get(),
        };

        let resp = self
            .retry("partition_snapshot", p, |data, mut client| async move {
                client.partition_snapshot(data).await
            })
            .await?;
        let partition = resp.partition.required().ctx("partition")?;
        Ok(PartitionSnapshot::decode(partition, resp.generation))
    }
}

#[async_trait]
impl ParquetFileRepo for GrpcCatalogClientRepos {
    async fn flag_for_delete_by_retention(&mut self) -> Result<Vec<(PartitionId, ObjectStoreId)>> {
        let p = proto::ParquetFileFlagForDeleteByRetentionRequest {};

        self.retry(
            "parquet_file_flag_for_delete_by_retention",
            p,
            |data, mut client| async move {
                client.parquet_file_flag_for_delete_by_retention(data).await
            },
        )
        .await?
        .map_err(convert_status)
        .and_then(|res| async move {
            Ok((
                PartitionId::new(res.partition_id),
                deserialize_object_store_id(res.object_store_id.required().ctx("object_store_id")?),
            ))
        })
        .try_collect()
        .await
    }

    async fn delete_old_ids_only(&mut self, older_than: Timestamp) -> Result<Vec<ObjectStoreId>> {
        let p = proto::ParquetFileDeleteOldIdsOnlyRequest {
            older_than: older_than.get(),
        };

        self.retry(
            "parquet_file_delete_old_ids_only",
            p,
            |data, mut client| async move { client.parquet_file_delete_old_ids_only(data).await },
        )
        .await?
        .map_err(convert_status)
        .and_then(|res| async move {
            Ok(deserialize_object_store_id(
                res.object_store_id.required().ctx("object_store_id")?,
            ))
        })
        .try_collect()
        .await
    }

    async fn list_by_partition_not_to_delete_batch(
        &mut self,
        partition_ids: Vec<PartitionId>,
    ) -> Result<Vec<ParquetFile>> {
        let p = proto::ParquetFileListByPartitionNotToDeleteBatchRequest {
            partition_ids: partition_ids.into_iter().map(|p| p.get()).collect(),
        };

        self.retry(
            "parquet_file_list_by_partition_not_to_delete_batch",
            p,
            |data, mut client| async move {
                client
                    .parquet_file_list_by_partition_not_to_delete_batch(data)
                    .await
            },
        )
        .await?
        .map_err(convert_status)
        .and_then(|res| async move {
            Ok(deserialize_parquet_file(
                res.parquet_file.required().ctx("parquet_file")?,
            )?)
        })
        .try_collect()
        .await
    }

    async fn get_by_object_store_id(
        &mut self,
        object_store_id: ObjectStoreId,
    ) -> Result<Option<ParquetFile>> {
        let p = proto::ParquetFileGetByObjectStoreIdRequest {
            object_store_id: Some(serialize_object_store_id(object_store_id)),
        };

        let maybe_file = self.retry(
            "parquet_file_get_by_object_store_id",
            p,
            |data, mut client| async move { client.parquet_file_get_by_object_store_id(data).await })
            .await?
            .parquet_file.map(deserialize_parquet_file).transpose()?;
        Ok(maybe_file)
    }

    async fn exists_by_object_store_id_batch(
        &mut self,
        object_store_ids: Vec<ObjectStoreId>,
    ) -> Result<Vec<ObjectStoreId>> {
        let p = futures::stream::iter(object_store_ids.into_iter().map(|id| {
            proto::ParquetFileExistsByObjectStoreIdBatchRequest {
                object_store_id: Some(serialize_object_store_id(id)),
            }
        }));

        self.retry(
            "parquet_file_exists_by_object_store_id_batch",
            p,
            |data, mut client: ServiceClient| async move {
                client
                    .parquet_file_exists_by_object_store_id_batch(data)
                    .await
            },
        )
        .await?
        .map_err(convert_status)
        .and_then(|res| async move {
            Ok(deserialize_object_store_id(
                res.object_store_id.required().ctx("object_store_id")?,
            ))
        })
        .try_collect()
        .await
    }

    async fn create_upgrade_delete(
        &mut self,
        partition_id: PartitionId,
        delete: &[ObjectStoreId],
        upgrade: &[ObjectStoreId],
        create: &[ParquetFileParams],
        target_level: CompactionLevel,
    ) -> Result<Vec<ParquetFileId>> {
        let p = proto::ParquetFileCreateUpgradeDeleteRequest {
            partition_id: partition_id.get(),
            delete: delete
                .iter()
                .copied()
                .map(serialize_object_store_id)
                .collect(),
            upgrade: upgrade
                .iter()
                .copied()
                .map(serialize_object_store_id)
                .collect(),
            create: create.iter().map(serialize_parquet_file_params).collect(),
            target_level: target_level as i32,
        };

        let resp = self.retry(
            "parquet_file_create_upgrade_delete",
            p,
            |data, mut client| async move { client.parquet_file_create_upgrade_delete(data).await },
        )
        .await?;

        Ok(resp
            .created_parquet_file_ids
            .into_iter()
            .map(ParquetFileId::new)
            .collect())
    }
}
