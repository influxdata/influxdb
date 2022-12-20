//! Metric instrumentation for catalog implementations.

use crate::interface::{
    sealed::TransactionFinalize, CasFailure, ColumnRepo, NamespaceRepo, ParquetFileRepo,
    PartitionRepo, ProcessedTombstoneRepo, QueryPoolRepo, RepoCollection, Result, ShardRepo,
    TableRepo, TombstoneRepo, TopicMetadataRepo,
};
use async_trait::async_trait;
use data_types::{
    Column, ColumnType, ColumnTypeCount, CompactionLevel, Namespace, NamespaceId, ParquetFile,
    ParquetFileId, ParquetFileParams, Partition, PartitionId, PartitionKey, PartitionParam,
    ProcessedTombstone, QueryPool, QueryPoolId, SequenceNumber, Shard, ShardId, ShardIndex,
    SkippedCompaction, Table, TableId, TablePartition, Timestamp, Tombstone, TombstoneId, TopicId,
    TopicMetadata,
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
    T: TopicMetadataRepo
        + QueryPoolRepo
        + NamespaceRepo
        + TableRepo
        + ColumnRepo
        + ShardRepo
        + PartitionRepo
        + TombstoneRepo
        + ProcessedTombstoneRepo
        + ParquetFileRepo
        + Debug,
    P: TimeProvider,
{
    fn topics(&mut self) -> &mut dyn TopicMetadataRepo {
        self
    }

    fn query_pools(&mut self) -> &mut dyn QueryPoolRepo {
        self
    }

    fn namespaces(&mut self) -> &mut dyn NamespaceRepo {
        self
    }

    fn tables(&mut self) -> &mut dyn TableRepo {
        self
    }

    fn columns(&mut self) -> &mut dyn ColumnRepo {
        self
    }

    fn shards(&mut self) -> &mut dyn ShardRepo {
        self
    }

    fn partitions(&mut self) -> &mut dyn PartitionRepo {
        self
    }

    fn tombstones(&mut self) -> &mut dyn TombstoneRepo {
        self
    }

    fn parquet_files(&mut self) -> &mut dyn ParquetFileRepo {
        self
    }

    fn processed_tombstones(&mut self) -> &mut dyn ProcessedTombstoneRepo {
        self
    }
}

#[async_trait]
impl<T, P> TransactionFinalize for MetricDecorator<T, P>
where
    T: TransactionFinalize,
    P: TimeProvider,
{
    async fn commit_inplace(&mut self) -> Result<(), super::interface::Error> {
        self.inner.commit_inplace().await
    }
    async fn abort_inplace(&mut self) -> Result<(), super::interface::Error> {
        self.inner.abort_inplace().await
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
    impl_trait = TopicMetadataRepo,
    methods = [
        "topic_create_or_get" = create_or_get(&mut self, name: &str) -> Result<TopicMetadata>;
        "topic_get_by_name" = get_by_name(&mut self, name: &str) -> Result<Option<TopicMetadata>>;
    ]
);

decorate!(
    impl_trait = QueryPoolRepo,
    methods = [
        "query_create_or_get" = create_or_get(&mut self, name: &str) -> Result<QueryPool>;
    ]
);

decorate!(
    impl_trait = NamespaceRepo,
    methods = [
        "namespace_create" = create(&mut self, name: &str, retention_period_ns: Option<i64>, topic_id: TopicId, query_pool_id: QueryPoolId) -> Result<Namespace>;
        "namespace_update_retention_period" = update_retention_period(&mut self, name: &str, retention_period_ns: Option<i64>) -> Result<Namespace>;
        "namespace_list" = list(&mut self) -> Result<Vec<Namespace>>;
        "namespace_get_by_id" = get_by_id(&mut self, id: NamespaceId) -> Result<Option<Namespace>>;
        "namespace_get_by_name" = get_by_name(&mut self, name: &str) -> Result<Option<Namespace>>;
        "namespace_update_table_limit" = update_table_limit(&mut self, name: &str, new_max: i32) -> Result<Namespace>;
        "namespace_update_column_limit" = update_column_limit(&mut self, name: &str, new_max: i32) -> Result<Namespace>;
    ]
);

decorate!(
    impl_trait = TableRepo,
    methods = [
        "table_create_or_get" = create_or_get(&mut self, name: &str, namespace_id: NamespaceId) -> Result<Table>;
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
        "column_list_type_count_by_table_id" = list_type_count_by_table_id(&mut self, table_id: TableId) -> Result<Vec<ColumnTypeCount>>;
    ]
);

decorate!(
    impl_trait = ShardRepo,
    methods = [
        "shard_create_or_get" = create_or_get(&mut self, topic: &TopicMetadata, shard_index: ShardIndex) -> Result<Shard>;
        "shard_get_by_topic_id_and_shard_index" = get_by_topic_id_and_shard_index(&mut self, topic_id: TopicId, shard_index: ShardIndex) -> Result<Option<Shard>>;
        "shard_list" = list(&mut self) -> Result<Vec<Shard>>;
        "shard_list_by_topic" = list_by_topic(&mut self, topic: &TopicMetadata) -> Result<Vec<Shard>>;
        "shard_update_min_unpersisted_sequence_number" = update_min_unpersisted_sequence_number(&mut self, shard_id: ShardId, sequence_number: SequenceNumber) -> Result<()>;
    ]
);

decorate!(
    impl_trait = PartitionRepo,
    methods = [
        "partition_create_or_get" = create_or_get(&mut self, key: PartitionKey, shard_id: ShardId, table_id: TableId) -> Result<Partition>;
        "partition_get_by_id" = get_by_id(&mut self, partition_id: PartitionId) -> Result<Option<Partition>>;
        "partition_list_by_shard" = list_by_shard(&mut self, shard_id: ShardId) -> Result<Vec<Partition>>;
        "partition_list_by_namespace" = list_by_namespace(&mut self, namespace_id: NamespaceId) -> Result<Vec<Partition>>;
        "partition_list_by_table_id" = list_by_table_id(&mut self, table_id: TableId) -> Result<Vec<Partition>>;
        "partition_update_sort_key" = cas_sort_key(&mut self, partition_id: PartitionId, old_sort_key: Option<Vec<String>>, new_sort_key: &[&str]) -> Result<Partition, CasFailure<Vec<String>>>;
        "partition_record_skipped_compaction" = record_skipped_compaction(&mut self, partition_id: PartitionId, reason: &str, num_files: usize, limit_num_files: usize, limit_num_files_first_in_partition: usize, estimated_bytes: u64, limit_bytes: u64) -> Result<()>;
        "partition_list_skipped_compactions" = list_skipped_compactions(&mut self) -> Result<Vec<SkippedCompaction>>;
        "partition_delete_skipped_compactions" = delete_skipped_compactions(&mut self, partition_id: PartitionId) -> Result<Option<SkippedCompaction>>;
        "partition_update_persisted_sequence_number" = update_persisted_sequence_number(&mut self, partition_id: PartitionId, sequence_number: SequenceNumber) -> Result<()>;
        "partition_most_recent_n" = most_recent_n(&mut self, n: usize, shards: &[ShardId]) -> Result<Vec<Partition>>;
    ]
);

decorate!(
    impl_trait = TombstoneRepo,
    methods = [
        "tombstone_create_or_get" = create_or_get( &mut self, table_id: TableId, shard_id: ShardId, sequence_number: SequenceNumber, min_time: Timestamp, max_time: Timestamp, predicate: &str) -> Result<Tombstone>;
        "tombstone_list_by_namespace" = list_by_namespace(&mut self, namespace_id: NamespaceId) -> Result<Vec<Tombstone>>;
        "tombstone_list_by_table" = list_by_table(&mut self, table_id: TableId) -> Result<Vec<Tombstone>>;
        "tombstone_get_by_id" = get_by_id(&mut self, id: TombstoneId) -> Result<Option<Tombstone>>;
        "tombstone_list_tombstones_by_shard_greater_than" = list_tombstones_by_shard_greater_than(&mut self, shard_id: ShardId, sequence_number: SequenceNumber) -> Result<Vec<Tombstone>>;
        "tombstone_remove" =  remove(&mut self, tombstone_ids: &[TombstoneId]) -> Result<()>;
        "tombstone_list_tombstones_for_time_range" = list_tombstones_for_time_range(&mut self, shard_id: ShardId, table_id: TableId, sequence_number: SequenceNumber, min_time: Timestamp, max_time: Timestamp) -> Result<Vec<Tombstone>>;
    ]
);

decorate!(
    impl_trait = ParquetFileRepo,
    methods = [
        "parquet_create" = create( &mut self, parquet_file_params: ParquetFileParams) -> Result<ParquetFile>;
        "parquet_flag_for_delete" = flag_for_delete(&mut self, id: ParquetFileId) -> Result<()>;
        "parquet_flag_for_delete_by_retention" = flag_for_delete_by_retention(&mut self) -> Result<Vec<ParquetFileId>>;
        "parquet_list_by_shard_greater_than" = list_by_shard_greater_than(&mut self, shard_id: ShardId, sequence_number: SequenceNumber) -> Result<Vec<ParquetFile>>;
        "parquet_list_by_namespace_not_to_delete" = list_by_namespace_not_to_delete(&mut self, namespace_id: NamespaceId) -> Result<Vec<ParquetFile>>;
        "parquet_list_by_table_not_to_delete" = list_by_table_not_to_delete(&mut self, table_id: TableId) -> Result<Vec<ParquetFile>>;
        "parquet_delete_old" = delete_old(&mut self, older_than: Timestamp) -> Result<Vec<ParquetFile>>;
        "parquet_delete_old_ids_only" = delete_old_ids_only(&mut self, older_than: Timestamp) -> Result<Vec<ParquetFileId>>;
        "parquet_list_by_partition_not_to_delete" = list_by_partition_not_to_delete(&mut self, partition_id: PartitionId) -> Result<Vec<ParquetFile>>;
        "parquet_level_0" = level_0(&mut self, shard_id: ShardId) -> Result<Vec<ParquetFile>>;
        "parquet_level_1" = level_1(&mut self, table_partition: TablePartition, min_time: Timestamp, max_time: Timestamp) -> Result<Vec<ParquetFile>>;
        "parquet_update_compaction_level" = update_compaction_level(&mut self, parquet_file_ids: &[ParquetFileId], compaction_level: CompactionLevel) -> Result<Vec<ParquetFileId>>;
        "parquet_exist" = exist(&mut self, id: ParquetFileId) -> Result<bool>;
        "parquet_count" = count(&mut self) -> Result<i64>;
        "parquet_count_by_overlaps_with_level_0" = count_by_overlaps_with_level_0(&mut self, table_id: TableId, shard_id: ShardId, min_time: Timestamp, max_time: Timestamp, sequence_number: SequenceNumber) -> Result<i64>;
        "parquet_count_by_overlaps_with_level_1" = count_by_overlaps_with_level_1(&mut self, table_id: TableId, shard_id: ShardId, min_time: Timestamp, max_time: Timestamp) -> Result<i64>;
        "parquet_get_by_object_store_id" = get_by_object_store_id(&mut self, object_store_id: Uuid) -> Result<Option<ParquetFile>>;
        "recent_highest_throughput_partitions" = recent_highest_throughput_partitions(&mut self, shard_id: Option<ShardId>, time_in_the_past: Timestamp, min_num_files: usize, num_partitions: usize) -> Result<Vec<PartitionParam>>;
        "parquet_partitions_with_small_l1_file_count" = partitions_with_small_l1_file_count(&mut self, shard_id: Option<ShardId>, small_size_threshold_bytes: i64, min_small_file_count: usize, num_partitions: usize) -> Result<Vec<PartitionParam>>;
        "most_cold_files_partitions" =  most_cold_files_partitions(&mut self, shard_id: Option<ShardId>, time_in_the_past: Timestamp, num_partitions: usize) -> Result<Vec<PartitionParam>>;
    ]
);

decorate!(
    impl_trait = ProcessedTombstoneRepo,
    methods = [
        "processed_tombstone_create" = create(&mut self, parquet_file_id: ParquetFileId, tombstone_id: TombstoneId) -> Result<ProcessedTombstone>;
        "processed_tombstone_exist" = exist(&mut self, parquet_file_id: ParquetFileId, tombstone_id: TombstoneId) -> Result<bool>;
        "processed_tombstone_count" = count(&mut self) -> Result<i64>;
        "processed_tombstone_count_by_tombstone_id" = count_by_tombstone_id(&mut self, tombstone_id: TombstoneId) -> Result<i64>;
    ]
);
