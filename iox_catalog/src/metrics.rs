//! Metric instrumentation for catalog implementations.

use crate::interface::{
    CasFailure, ColumnRepo, NamespaceRepo, ParquetFileRepo, PartitionRepo, RepoCollection, Result,
    SoftDeletedRows, TableRepo,
};
use async_trait::async_trait;
use data_types::{
    partition_template::{NamespacePartitionTemplateOverride, TablePartitionTemplateOverride},
    Column, ColumnType, CompactionLevel, Namespace, NamespaceId, NamespaceName,
    NamespaceServiceProtectionLimitsOverride, ParquetFile, ParquetFileId, ParquetFileParams,
    Partition, PartitionHashId, PartitionId, PartitionKey, SkippedCompaction, SortedColumnSet,
    Table, TableId, Timestamp, TransitionPartitionId,
};
use iox_time::{SystemProvider, TimeProvider};
use metric::{DurationHistogram, Metric};
use std::{collections::HashMap, fmt::Debug, sync::Arc};
use uuid::Uuid;

/// Decorates a implementation of the catalog's [`RepoCollection`] (and the
/// transactional variant) with instrumentation that emits latency histograms
/// for each method.
///
/// Values are recorded under the `catalog_op_duration` metric, labelled by
/// operation name and result (success/error).
#[derive(Debug)]
pub struct MetricDecorator<T, P = SystemProvider> {
    inner: T,
    time_provider: P,
    metrics: Arc<metric::Registry>,
}

impl<T> MetricDecorator<T> {
    /// Wrap `T` with instrumentation recording operation latency in `metrics`.
    pub fn new(inner: T, metrics: Arc<metric::Registry>) -> Self {
        Self {
            inner,
            time_provider: Default::default(),
            metrics,
        }
    }
}

impl<T, P> RepoCollection for MetricDecorator<T, P>
where
    T: NamespaceRepo + TableRepo + ColumnRepo + PartitionRepo + ParquetFileRepo + Debug,
    P: TimeProvider,
{
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

/// Emit a trait impl for `impl_trait` that delegates calls to the inner
/// implementation, recording the duration and result to the metrics registry.
///
/// Format:
///
/// ```ignore
///     decorate!(
///         impl_trait = <trait name>,
///         methods = [
///             "<metric name>" = <method signature>;
///             "<metric name>" = <method signature>;
///             // ... and so on
///         ]
///     );
/// ```
///
/// All methods of a given trait MUST be defined in the `decorate!()` call so
/// they are all instrumented or the decorator will not compile as it won't
/// fully implement the trait.
macro_rules! decorate {
    (
        impl_trait = $trait:ident,
        methods = [$(
            $metric:literal = $method:ident(
                &mut self $(,)?
                $($arg:ident : $t:ty),*
            ) -> Result<$out:ty$(, $err:ty)?>;
        )+]
    ) => {
        #[async_trait]
        impl<P: TimeProvider, T:$trait> $trait for MetricDecorator<T, P> {
            /// NOTE: if you're seeing an error here about "not all trait items
            /// implemented" or something similar, one or more methods are
            /// missing from / incorrectly defined in the decorate!() blocks
            /// below.

            $(
                async fn $method(&mut self, $($arg : $t),*) -> Result<$out$(, $err)?> {
                    let observer: Metric<DurationHistogram> = self.metrics.register_metric(
                        "catalog_op_duration",
                        "catalog call duration",
                    );

                    let t = self.time_provider.now();
                    let res = self.inner.$method($($arg),*).await;

                    // Avoid exploding if time goes backwards - simply drop the
                    // measurement if it happens.
                    if let Some(delta) = self.time_provider.now().checked_duration_since(t) {
                        let tag = match &res {
                            Ok(_) => "success",
                            Err(_) => "error",
                        };
                        observer.recorder(&[("op", $metric), ("result", tag)]).record(delta);
                    }

                    res
                }
            )+
        }
    };
}

decorate!(
    impl_trait = NamespaceRepo,
    methods = [
        "namespace_create" = create(&mut self, name: &NamespaceName<'_>, partition_template: Option<NamespacePartitionTemplateOverride>, retention_period_ns: Option<i64>, service_protection_limits: Option<NamespaceServiceProtectionLimitsOverride>) -> Result<Namespace>;
        "namespace_update_retention_period" = update_retention_period(&mut self, name: &str, retention_period_ns: Option<i64>) -> Result<Namespace>;
        "namespace_list" = list(&mut self, deleted: SoftDeletedRows) -> Result<Vec<Namespace>>;
        "namespace_get_by_id" = get_by_id(&mut self, id: NamespaceId, deleted: SoftDeletedRows) -> Result<Option<Namespace>>;
        "namespace_get_by_name" = get_by_name(&mut self, name: &str, deleted: SoftDeletedRows) -> Result<Option<Namespace>>;
        "namespace_soft_delete" = soft_delete(&mut self, name: &str) -> Result<()>;
        "namespace_update_table_limit" = update_table_limit(&mut self, name: &str, new_max: i32) -> Result<Namespace>;
        "namespace_update_column_limit" = update_column_limit(&mut self, name: &str, new_max: i32) -> Result<Namespace>;
    ]
);

decorate!(
    impl_trait = TableRepo,
    methods = [
        "table_create" = create(&mut self, name: &str, partition_template: TablePartitionTemplateOverride, namespace_id: NamespaceId) -> Result<Table>;
        "table_get_by_id" = get_by_id(&mut self, table_id: TableId) -> Result<Option<Table>>;
        "table_get_by_namespace_and_name" = get_by_namespace_and_name(&mut self, namespace_id: NamespaceId, name: &str) -> Result<Option<Table>>;
        "table_list_by_namespace_id" = list_by_namespace_id(&mut self, namespace_id: NamespaceId) -> Result<Vec<Table>>;
        "table_list" = list(&mut self) -> Result<Vec<Table>>;
    ]
);

decorate!(
    impl_trait = ColumnRepo,
    methods = [
        "column_create_or_get" = create_or_get(&mut self, name: &str, table_id: TableId, column_type: ColumnType) -> Result<Column>;
        "column_list_by_namespace_id" = list_by_namespace_id(&mut self, namespace_id: NamespaceId) -> Result<Vec<Column>>;
        "column_list_by_table_id" = list_by_table_id(&mut self, table_id: TableId) -> Result<Vec<Column>>;
        "column_create_or_get_many_unchecked" = create_or_get_many_unchecked(&mut self, table_id: TableId, columns: HashMap<&str, ColumnType>) -> Result<Vec<Column>>;
        "column_list" = list(&mut self) -> Result<Vec<Column>>;
    ]
);

decorate!(
    impl_trait = PartitionRepo,
    methods = [
        "partition_create_or_get" = create_or_get(&mut self, key: PartitionKey, table_id: TableId) -> Result<Partition>;
        "partition_get_by_id" = get_by_id(&mut self, partition_id: PartitionId) -> Result<Option<Partition>>;
        "partition_get_by_id_batch" = get_by_id_batch(&mut self, partition_ids: Vec<PartitionId>) -> Result<Vec<Partition>>;
        "partition_get_by_hash_id" = get_by_hash_id(&mut self, partition_hash_id: &PartitionHashId) -> Result<Option<Partition>>;
        "partition_get_by_hash_id_batch" = get_by_hash_id_batch(&mut self, partition_hash_ids: &[&PartitionHashId]) -> Result<Vec<Partition>>;
        "partition_list_by_table_id" = list_by_table_id(&mut self, table_id: TableId) -> Result<Vec<Partition>>;
        "partition_list_ids" = list_ids(&mut self) -> Result<Vec<PartitionId>>;
        "partition_update_sort_key" = cas_sort_key(&mut self, partition_id: &TransitionPartitionId, old_sort_key: Option<Vec<String>>, new_sort_key: &[&str], new_sort_key_ids: &SortedColumnSet) -> Result<Partition, CasFailure<(Vec<String>, Option<SortedColumnSet>)>>;
        "partition_record_skipped_compaction" = record_skipped_compaction(&mut self, partition_id: PartitionId, reason: &str, num_files: usize, limit_num_files: usize, limit_num_files_first_in_partition: usize, estimated_bytes: u64, limit_bytes: u64) -> Result<()>;
        "partition_list_skipped_compactions" = list_skipped_compactions(&mut self) -> Result<Vec<SkippedCompaction>>;
        "partition_delete_skipped_compactions" = delete_skipped_compactions(&mut self, partition_id: PartitionId) -> Result<Option<SkippedCompaction>>;
        "partition_most_recent_n" = most_recent_n(&mut self, n: usize) -> Result<Vec<Partition>>;
        "partition_partitions_new_file_between" = partitions_new_file_between(&mut self, minimum_time: Timestamp, maximum_time: Option<Timestamp>) -> Result<Vec<PartitionId>>;
        "partition_get_in_skipped_compactions" = get_in_skipped_compactions(&mut self, partition_ids: &[PartitionId]) -> Result<Vec<SkippedCompaction>>;
        "partition_list_old_style" = list_old_style(&mut self) -> Result<Vec<Partition>>;
    ]
);

decorate!(
    impl_trait = ParquetFileRepo,
    methods = [
        "parquet_create" = create(&mut self, parquet_file_params: ParquetFileParams) -> Result<ParquetFile>;
        "parquet_list_all" = list_all(&mut self) -> Result<Vec<ParquetFile>>;
        "parquet_flag_for_delete_by_retention" = flag_for_delete_by_retention(&mut self) -> Result<Vec<ParquetFileId>>;
        "parquet_list_by_namespace_not_to_delete" = list_by_namespace_not_to_delete(&mut self, namespace_id: NamespaceId) -> Result<Vec<ParquetFile>>;
        "parquet_list_by_table_not_to_delete" = list_by_table_not_to_delete(&mut self, table_id: TableId) -> Result<Vec<ParquetFile>>;
        "parquet_delete_old_ids_only" = delete_old_ids_only(&mut self, older_than: Timestamp) -> Result<Vec<ParquetFileId>>;
        "parquet_list_by_partition_not_to_delete" = list_by_partition_not_to_delete(&mut self, partition_id: &TransitionPartitionId) -> Result<Vec<ParquetFile>>;
        "parquet_get_by_object_store_id" = get_by_object_store_id(&mut self, object_store_id: Uuid) -> Result<Option<ParquetFile>>;
        "parquet_exists_by_object_store_id_batch" = exists_by_object_store_id_batch(&mut self, object_store_ids: Vec<Uuid>) -> Result<Vec<Uuid>>;
        "parquet_create_upgrade_delete" = create_upgrade_delete(&mut self, delete: &[ParquetFileId], upgrade: &[ParquetFileId], create: &[ParquetFileParams], target_level: CompactionLevel) -> Result<Vec<ParquetFileId>>;
    ]
);
