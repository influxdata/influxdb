//! Metric instrumentation for catalog implementations.

use crate::interface::{
    sealed::TransactionFinalize, ColumnRepo, ColumnUpsertRequest, KafkaTopicRepo, NamespaceRepo,
    ParquetFileRepo, PartitionRepo, ProcessedTombstoneRepo, QueryPoolRepo, RepoCollection, Result,
    SequencerRepo, TablePersistInfo, TableRepo, TombstoneRepo,
};
use async_trait::async_trait;
use data_types2::{
    Column, ColumnType, KafkaPartition, KafkaTopic, KafkaTopicId, Namespace, NamespaceId,
    ParquetFile, ParquetFileId, ParquetFileParams, Partition, PartitionId, PartitionInfo,
    ProcessedTombstone, QueryPool, QueryPoolId, SequenceNumber, Sequencer, SequencerId, Table,
    TableId, TablePartition, Timestamp, Tombstone, TombstoneId,
};
use metric::{Metric, U64Histogram, U64HistogramOptions};
use std::{fmt::Debug, sync::Arc};
use time::{SystemProvider, TimeProvider};

/// Decorates a implementation of the catalog's [`RepoCollection`] (and the
/// transactional variant) with instrumentation that emits latency histograms
/// for each method.
///
/// Values are recorded under the `catalog_op_duration_ms` metric, labelled by
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
    T: KafkaTopicRepo
        + QueryPoolRepo
        + NamespaceRepo
        + TableRepo
        + ColumnRepo
        + SequencerRepo
        + PartitionRepo
        + TombstoneRepo
        + ProcessedTombstoneRepo
        + ParquetFileRepo
        + Debug,
    P: TimeProvider,
{
    fn kafka_topics(&mut self) -> &mut dyn KafkaTopicRepo {
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

    fn sequencers(&mut self) -> &mut dyn SequencerRepo {
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
            ) -> Result<$out:ty>;
        )+]
    ) => {
        #[async_trait]
        impl<P: TimeProvider, T:$trait> $trait for MetricDecorator<T, P> {
            /// NOTE: if you're seeing an error here about "not all trait items
            /// implemented" or something similar, one or more methods are
            /// missing from / incorrectly defined in the decorate!() blocks
            /// below.

            $(
                async fn $method(&mut self, $($arg : $t),*) -> Result<$out> {
                    let buckets = || {
                        U64HistogramOptions::new([5, 10, 20, 40, 80, 160, 320, 640, 1280, 2560, 5120, u64::MAX])
                    };

                    let observer: Metric<U64Histogram> = self.metrics.register_metric_with_options(
                        "catalog_op_duration_ms",
                        "catalog call duration in milliseconds",
                        buckets,
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
                        observer.recorder(&[("op", $metric), ("result", tag)]).record(delta.as_millis() as _);
                    }

                    res
                }
            )+
        }
    };
}

decorate!(
    impl_trait = KafkaTopicRepo,
    methods = [
        "kafka_create_or_get" = create_or_get(&mut self, name: &str) -> Result<KafkaTopic>;
        "kafka_get_by_name" = get_by_name(&mut self, name: &str) -> Result<Option<KafkaTopic>>;
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
        "namespace_create" = create(&mut self, name: &str, retention_duration: &str, kafka_topic_id: KafkaTopicId, query_pool_id: QueryPoolId) -> Result<Namespace>;
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
        "get_table_persist_info" = get_table_persist_info(&mut self, sequencer_id: SequencerId, namespace_id: NamespaceId, table_name: &str) -> Result<Option<TablePersistInfo>>;
    ]
);

decorate!(
    impl_trait = ColumnRepo,
    methods = [
        "column_create_or_get" = create_or_get(&mut self, name: &str, table_id: TableId, column_type: ColumnType) -> Result<Column>;
        "column_list_by_namespace_id" = list_by_namespace_id(&mut self, namespace_id: NamespaceId) -> Result<Vec<Column>>;
        "column_create_or_get_many" = create_or_get_many(&mut self, columns: &[ColumnUpsertRequest<'_>]) -> Result<Vec<Column>>;
    ]
);

decorate!(
    impl_trait = SequencerRepo,
    methods = [
        "sequencer_create_or_get" = create_or_get(&mut self, topic: &KafkaTopic, partition: KafkaPartition) -> Result<Sequencer>;
        "sequencer_get_by_topic_id_and_partition" = get_by_topic_id_and_partition(&mut self, topic_id: KafkaTopicId, partition: KafkaPartition) -> Result<Option<Sequencer>>;
        "sequencer_list" = list(&mut self) -> Result<Vec<Sequencer>>;
        "sequencer_list_by_kafka_topic" = list_by_kafka_topic(&mut self, topic: &KafkaTopic) -> Result<Vec<Sequencer>>;
        "sequencer_update_min_unpersisted_sequence_number" = update_min_unpersisted_sequence_number(&mut self, sequencer_id: SequencerId, sequence_number: SequenceNumber) -> Result<()>;
    ]
);

decorate!(
    impl_trait = PartitionRepo,
    methods = [
        "partition_create_or_get" = create_or_get(&mut self, key: &str, sequencer_id: SequencerId, table_id: TableId) -> Result<Partition>;
        "partition_get_by_id" = get_by_id(&mut self, partition_id: PartitionId) -> Result<Option<Partition>>;
        "partition_list_by_sequencer" = list_by_sequencer(&mut self, sequencer_id: SequencerId) -> Result<Vec<Partition>>;
        "partition_list_by_namespace" = list_by_namespace(&mut self, namespace_id: NamespaceId) -> Result<Vec<Partition>>;
        "partition_partition_info_by_id" = partition_info_by_id(&mut self, partition_id: PartitionId) -> Result<Option<PartitionInfo>>;
        "partition_update_sort_key" = update_sort_key(&mut self, partition_id: PartitionId, sort_key: &str) -> Result<Partition>;
    ]
);

decorate!(
    impl_trait = TombstoneRepo,
    methods = [
        "tombstone_create_or_get" = create_or_get( &mut self, table_id: TableId, sequencer_id: SequencerId, sequence_number: SequenceNumber, min_time: Timestamp, max_time: Timestamp, predicate: &str) -> Result<Tombstone>;
        "tombstone_list_by_namespace" = list_by_namespace(&mut self, namespace_id: NamespaceId) -> Result<Vec<Tombstone>>;
        "tombstone_list_by_table" = list_by_table(&mut self, table_id: TableId) -> Result<Vec<Tombstone>>;
        "tombstone_get_by_id" = get_by_id(&mut self, id: TombstoneId) -> Result<Option<Tombstone>>;
        "tombstone_list_tombstones_by_sequencer_greater_than" = list_tombstones_by_sequencer_greater_than(&mut self, sequencer_id: SequencerId, sequence_number: SequenceNumber) -> Result<Vec<Tombstone>>;
        "tombstone_remove" =  remove(&mut self, tombstone_ids: &[TombstoneId]) -> Result<()>;
        "tombstone_list_tombstones_for_time_range" = list_tombstones_for_time_range(&mut self, sequencer_id: SequencerId, table_id: TableId, sequence_number: SequenceNumber, min_time: Timestamp, max_time: Timestamp) -> Result<Vec<Tombstone>>;
    ]
);

decorate!(
    impl_trait = ParquetFileRepo,
    methods = [
        "parquet_create" = create( &mut self, parquet_file_params: ParquetFileParams) -> Result<ParquetFile>;
        "parquet_flag_for_delete" = flag_for_delete(&mut self, id: ParquetFileId) -> Result<()>;
        "parquet_list_by_sequencer_greater_than" = list_by_sequencer_greater_than(&mut self, sequencer_id: SequencerId, sequence_number: SequenceNumber) -> Result<Vec<ParquetFile>>;
        "parquet_list_by_namespace_not_to_delete" = list_by_namespace_not_to_delete(&mut self, namespace_id: NamespaceId) -> Result<Vec<ParquetFile>>;
        "parquet_list_by_table_not_to_delete" = list_by_table_not_to_delete(&mut self, table_id: TableId) -> Result<Vec<ParquetFile>>;
        "parquet_delete_old" = delete_old(&mut self, older_than: Timestamp) -> Result<Vec<ParquetFile>>;
        "parquet_list_by_partition_not_to_delete" = list_by_partition_not_to_delete(&mut self, partition_id: PartitionId) -> Result<Vec<ParquetFile>>;
        "parquet_level_0" = level_0(&mut self, sequencer_id: SequencerId) -> Result<Vec<ParquetFile>>;
        "parquet_level_1" = level_1(&mut self, table_partition: TablePartition, min_time: Timestamp, max_time: Timestamp) -> Result<Vec<ParquetFile>>;
        "parquet_update_to_level_1" = update_to_level_1(&mut self, parquet_file_ids: &[ParquetFileId]) -> Result<Vec<ParquetFileId>>;
        "parquet_exist" = exist(&mut self, id: ParquetFileId) -> Result<bool>;
        "parquet_parquet_metadata" = parquet_metadata(&mut self, id: ParquetFileId) -> Result<Vec<u8>>;
        "parquet_count" = count(&mut self) -> Result<i64>;
        "parquet_count_by_overlaps" = count_by_overlaps(&mut self, table_id: TableId, sequencer_id: SequencerId, min_time: Timestamp, max_time: Timestamp, sequence_number: SequenceNumber) -> Result<i64>;
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
