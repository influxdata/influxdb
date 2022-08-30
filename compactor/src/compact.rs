//! Data Points for the lifecycle of the Compactor

use crate::handler::CompactorConfig;
use backoff::BackoffConfig;
use data_types::{
    ColumnTypeCount, Namespace, NamespaceId, PartitionId, PartitionKey, PartitionParam, ShardId,
    Table, TableId, TableSchema,
};
use iox_catalog::interface::{get_schema_by_id, Catalog};
use iox_query::exec::Executor;
use iox_time::TimeProvider;
use metric::{
    Attributes, DurationHistogram, DurationHistogramOptions, Metric, U64Gauge, U64Histogram,
    U64HistogramOptions, DURATION_MAX,
};
use observability_deps::tracing::debug;
use parquet_file::storage::ParquetStorage;
use schema::sort::SortKey;
use snafu::{OptionExt, ResultExt, Snafu};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
    time::Duration,
};

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub enum Error {
    #[snafu(display("Error querying partition {}", source))]
    QueryingPartition {
        source: iox_catalog::interface::Error,
    },

    #[snafu(display("Error querying table {}", source))]
    QueryingTable {
        source: iox_catalog::interface::Error,
    },

    #[snafu(display("Error querying column {}", source))]
    QueryingColumn {
        source: iox_catalog::interface::Error,
    },

    #[snafu(display("Error querying namespace {}", source))]
    QueryingNamespace {
        source: iox_catalog::interface::Error,
    },

    #[snafu(display("Could not find partition {:?}", partition_id))]
    PartitionNotFound { partition_id: PartitionId },

    #[snafu(display("Could not find table {:?}", table_id))]
    TableNotFound { table_id: TableId },

    #[snafu(display("Could not find namespace {:?}", namespace_id))]
    NamespaceNotFound { namespace_id: NamespaceId },

    #[snafu(display(
        "Error getting the most recent highest ingested throughput partitions for shard {}. {}",
        shard_id,
        source
    ))]
    HighestThroughputPartitions {
        source: iox_catalog::interface::Error,
        shard_id: ShardId,
    },

    #[snafu(display(
        "Error getting the most level 0 file partitions for shard {}. {}",
        shard_id,
        source
    ))]
    MostL0Partitions {
        source: iox_catalog::interface::Error,
        shard_id: ShardId,
    },
}

/// A specialized `Error` for Compactor Data errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Data points needed to run a compactor
#[derive(Debug)]
pub struct Compactor {
    /// Shards assigned to this compactor
    shards: Vec<ShardId>,

    /// Object store for reading and persistence of parquet files
    pub(crate) store: ParquetStorage,

    /// The global catalog for schema, parquet files and tombstones
    pub(crate) catalog: Arc<dyn Catalog>,

    /// Executor for running queries, compacting, and persisting
    pub(crate) exec: Arc<Executor>,

    /// Time provider for all activities in this compactor
    pub time_provider: Arc<dyn TimeProvider>,

    /// Backoff config
    pub(crate) backoff_config: BackoffConfig,

    /// Configuration options for the compactor
    pub(crate) config: CompactorConfig,

    /// Gauge for the number of compaction partition candidates before filtering
    compaction_candidate_gauge: Metric<U64Gauge>,

    /// Gauge for the number of Parquet file candidates after filtering. The recorded values have
    /// attributes for the compaction level of the file and whether the file was selected for
    /// compaction or not.
    pub(crate) parquet_file_candidate_gauge: Metric<U64Gauge>,

    /// Histogram for the number of bytes of Parquet files selected for compaction. The recorded
    /// values have attributes for the compaction level of the file.
    pub(crate) parquet_file_candidate_bytes: Metric<U64Histogram>,

    /// After a successful compaction operation, track the sizes of the files that were used as the
    /// inputs of the compaction operation by compaction level.
    pub(crate) compaction_input_file_bytes: Metric<U64Histogram>,

    /// Histogram for tracking the time to compact a partition
    pub(crate) compaction_duration: Metric<DurationHistogram>,

    /// Histogram for tracking time to select partition candidates to compact.
    /// Even though we choose partitions to compact, we have to read parquet_file catalog
    /// table to see which partitions have the most recent L0 files. This time is for tracking
    /// reading that. This includes time to get candidates for all shard
    /// this compactor manages and for each shard the process invokes
    /// at most 3 three different SQLs and at least one.
    /// The expectation is small (a second or less) otherwise we have to improve it
    pub(crate) candidate_selection_duration: Metric<DurationHistogram>,

    /// Histogram for tracking time to add more information to selected partitions.
    /// After we get partitions to compact from reading parquet files, we need more
    /// information such schema and sort key of the partitions to proceed with compaction.
    /// This reading extra information turns out to run a lot of catalog queries.
    /// The expectation is small (a second or less) otherwise we have to improve it
    pub(crate) partitions_extra_info_reading_duration: Metric<DurationHistogram>,

    /// Histogram for tracking time to compact all selected partitions in a cycle
    /// This is used to observe:
    ///  . Whether there is a big difference between each cycle or not
    ///  . How well this process  is parallelized
    pub(crate) compaction_cycle_duration: Metric<DurationHistogram>,
}

impl Compactor {
    /// Initialize the Compactor Data
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        shards: Vec<ShardId>,
        catalog: Arc<dyn Catalog>,
        store: ParquetStorage,
        exec: Arc<Executor>,
        time_provider: Arc<dyn TimeProvider>,
        backoff_config: BackoffConfig,
        config: CompactorConfig,
        registry: Arc<metric::Registry>,
    ) -> Self {
        let compaction_candidate_gauge = registry.register_metric(
            "compactor_candidates",
            "gauge for the number of compaction candidates that are found when checked",
        );

        let parquet_file_candidate_gauge = registry.register_metric(
            "parquet_file_candidates",
            "Number of Parquet file candidates",
        );

        let file_size_buckets = U64HistogramOptions::new([
            500 * 1024,       // 500 KB
            1024 * 1024,      // 1 MB
            3 * 1024 * 1024,  // 3 MB
            10 * 1024 * 1024, // 10 MB
            30 * 1024 * 1024, // 30 MB
            u64::MAX,         // Inf
        ]);

        let parquet_file_candidate_bytes = registry.register_metric_with_options(
            "parquet_file_candidate_bytes",
            "Number of bytes of Parquet file compaction candidates",
            || file_size_buckets.clone(),
        );

        let compaction_input_file_bytes = registry.register_metric_with_options(
            "compaction_input_file_bytes",
            "Number of bytes of Parquet files used as inputs to a successful compaction operation",
            || file_size_buckets.clone(),
        );

        let duration_histogram_options = DurationHistogramOptions::new([
            Duration::from_millis(100),
            Duration::from_millis(500),
            Duration::from_micros(2_000),
            Duration::from_millis(5_000),
            Duration::from_millis(15_000),
            Duration::from_millis(30_000),
            Duration::from_millis(60_000), // 1 minute
            Duration::from_millis(5 * 60_000),
            DURATION_MAX,
        ]);
        let compaction_duration: Metric<DurationHistogram> = registry.register_metric_with_options(
            "compactor_compact_partition_duration",
            "Compact partition duration",
            || duration_histogram_options.clone(),
        );

        let candidate_selection_duration: Metric<DurationHistogram> = registry.register_metric(
            "compactor_candidate_selection_duration",
            "Duration to select compaction partition candidates",
        );

        let partitions_extra_info_reading_duration: Metric<DurationHistogram> = registry
            .register_metric(
                "compactor_partitions_extra_info_reading_duration",
                "Duration to read and add extra information into selected partition candidates",
            );

        let compaction_cycle_duration: Metric<DurationHistogram> = registry
            .register_metric_with_options(
                "compactor_compaction_cycle_duration",
                "Duration to compact all selected candidates for each cycle",
                || duration_histogram_options,
            );

        Self {
            shards,
            catalog,
            store,
            exec,
            time_provider,
            backoff_config,
            config,
            compaction_candidate_gauge,
            parquet_file_candidate_gauge,
            parquet_file_candidate_bytes,
            compaction_input_file_bytes,
            compaction_duration,
            candidate_selection_duration,
            partitions_extra_info_reading_duration,
            compaction_cycle_duration,
        }
    }

    /// Return a list of the most recent highest ingested throughput partitions.
    /// The highest throughput partitions are prioritized as follows:
    ///  1. If there are partitions with new ingested files within the last 4 hours, pick them.
    ///  2. If no new ingested files in the last 4 hours, will look for partitions with new writes
    ///     within the last 24 hours.
    ///  3. If there are no ingested files within the last 24 hours, will look for partitions
    ///     with any new ingested files in the past.
    ///
    /// * New ingested files means non-deleted L0 files
    /// * In all cases above, for each shard, N partitions with the most new ingested files
    ///   will be selected and the return list will include at most, P = N * S, partitions where S
    ///   is the number of shards this compactor handles.
    pub async fn hot_partitions_to_compact(
        &self,
        // Max number of the most recent highest ingested throughput partitions
        // per shard we want to read
        max_num_partitions_per_shard: usize,
        // Minimum number of the most recent writes per partition we want to count
        // to prioritize partitions
        min_recent_ingested_files: usize,
    ) -> Result<Vec<PartitionParam>> {
        let mut candidates = Vec::with_capacity(self.shards.len() * max_num_partitions_per_shard);
        let mut repos = self.catalog.repositories().await;

        for shard_id in &self.shards {
            let attributes = Attributes::from([
                ("shard_id", format!("{}", *shard_id).into()),
                ("partition_type", "hot".into()),
            ]);

            // Get the most recent highest ingested throughput partitions within
            // the last 4 hours. If nothing, increase to 24 hours
            let mut num_partitions = 0;
            for num_hours in [4, 24] {
                let mut partitions = repos
                    .parquet_files()
                    .recent_highest_throughput_partitions(
                        *shard_id,
                        num_hours,
                        min_recent_ingested_files,
                        max_num_partitions_per_shard,
                    )
                    .await
                    .context(HighestThroughputPartitionsSnafu {
                        shard_id: *shard_id,
                    })?;

                if !partitions.is_empty() {
                    debug!(
                        shard_id = shard_id.get(),
                        num_hours,
                        n = partitions.len(),
                        "found high-throughput partitions"
                    );
                    num_partitions = partitions.len();
                    candidates.append(&mut partitions);
                    break;
                }
            }

            // Record metric for candidates per shard
            debug!(
                shard_id = shard_id.get(),
                n = num_partitions,
                "hot compaction candidates",
            );
            let number_gauge = self.compaction_candidate_gauge.recorder(attributes);
            number_gauge.set(num_partitions as u64);
        }

        Ok(candidates)
    }

    /// Return a list of partitions that:
    ///
    /// - Have not received any writes in 24 hours (determined by all parquet files having a
    ///   created_at time older than 24 hours ago)
    /// - Have some level 0 parquet files that need to be upgraded or compacted
    pub async fn cold_partitions_to_compact(
        &self,
        // Max number of cold partitions per shard we want to compact
        max_num_partitions_per_shard: usize,
    ) -> Result<Vec<PartitionParam>> {
        let mut candidates = Vec::with_capacity(self.shards.len() * max_num_partitions_per_shard);
        let mut repos = self.catalog.repositories().await;

        for shard_id in &self.shards {
            let attributes = Attributes::from([
                ("shard_id", format!("{}", *shard_id).into()),
                ("partition_type", "cold".into()),
            ]);

            let mut partitions = repos
                .parquet_files()
                .most_level_0_files_partitions(*shard_id, 24, max_num_partitions_per_shard)
                .await
                .context(MostL0PartitionsSnafu {
                    shard_id: *shard_id,
                })?;

            let num_partitions = partitions.len();
            candidates.append(&mut partitions);

            // Record metric for candidates per shard
            debug!(
                shard_id = shard_id.get(),
                n = num_partitions,
                "cold compaction candidates",
            );
            let number_gauge = self.compaction_candidate_gauge.recorder(attributes);
            number_gauge.set(num_partitions as u64);
        }

        Ok(candidates)
    }

    /// Get column types for tables of given partitions
    pub async fn table_columns(
        &self,
        partitions: &[PartitionParam],
    ) -> Result<HashMap<TableId, Vec<ColumnTypeCount>>> {
        use std::collections::hash_map::Entry::Vacant; // this can be moved to the imports at the top if you want
        let mut repos = self.catalog.repositories().await;

        let mut result = HashMap::with_capacity(partitions.len());

        for table_id in partitions.iter().map(|p| p.table_id) {
            let entry = result.entry(table_id);
            if let Vacant(entry) = entry {
                let cols = repos
                    .columns()
                    .list_type_count_by_table_id(table_id)
                    .await
                    .context(QueryingColumnSnafu)?;
                entry.insert(cols);
            }
        }

        Ok(result)
    }

    /// Add namespace and table information to partition candidates.
    pub async fn add_info_to_partitions(
        &self,
        partitions: &[PartitionParam],
    ) -> Result<VecDeque<PartitionCompactionCandidateWithInfo>> {
        let mut repos = self.catalog.repositories().await;

        let table_ids: HashSet<_> = partitions.iter().map(|p| p.table_id).collect();
        let namespace_ids: HashSet<_> = partitions.iter().map(|p| p.namespace_id).collect();

        let mut namespaces = HashMap::with_capacity(namespace_ids.len());
        for id in namespace_ids {
            let namespace = repos
                .namespaces()
                .get_by_id(id)
                .await
                .context(QueryingNamespaceSnafu)?
                .context(NamespaceNotFoundSnafu { namespace_id: id })?;
            let schema = get_schema_by_id(namespace.id, repos.as_mut())
                .await
                .context(QueryingNamespaceSnafu)?;
            namespaces.insert(id, (Arc::new(namespace), schema));
        }

        let mut tables = HashMap::with_capacity(table_ids.len());
        for id in table_ids {
            let table = repos
                .tables()
                .get_by_id(id)
                .await
                .context(QueryingTableSnafu)?
                .context(TableNotFoundSnafu { table_id: id })?;
            let schema = namespaces
                .get(&table.namespace_id)
                .expect("just queried")
                .1
                .tables
                .get(&table.name)
                .context(TableNotFoundSnafu { table_id: id })?
                .clone();
            tables.insert(id, (Arc::new(table), Arc::new(schema)));
        }

        let mut parts = HashMap::with_capacity(partitions.len());
        for p in partitions {
            let partition = repos
                .partitions()
                .get_by_id(p.partition_id)
                .await
                .context(QueryingPartitionSnafu)?
                .context(PartitionNotFoundSnafu {
                    partition_id: p.partition_id,
                })?;
            parts.insert(p.partition_id, partition);
        }

        Ok(partitions
            .iter()
            .map(|p| {
                let (table, table_schema) = tables.get(&p.table_id).expect("just queried");
                let part = parts.get(&p.partition_id).expect("just queried");

                PartitionCompactionCandidateWithInfo {
                    table: Arc::clone(table),
                    table_schema: Arc::clone(table_schema),
                    namespace: Arc::clone(
                        &namespaces.get(&p.namespace_id).expect("just queried").0,
                    ),
                    candidate: *p,
                    sort_key: part.sort_key(),
                    partition_key: part.partition_key.clone(),
                }
            })
            .collect::<VecDeque<_>>())
    }
}

/// [`PartitionParam`] with some information about its table and namespace.
#[derive(Debug, Clone)]
pub struct PartitionCompactionCandidateWithInfo {
    /// Partition compaction candidate.
    pub candidate: PartitionParam,

    /// Namespace.
    pub namespace: Arc<Namespace>,

    /// Table.
    pub table: Arc<Table>,

    /// Table schema
    pub table_schema: Arc<TableSchema>,

    /// Sort key of the partition
    pub sort_key: Option<SortKey>,

    /// partition_key
    pub partition_key: PartitionKey,
}

impl PartitionCompactionCandidateWithInfo {
    /// Partition ID
    pub fn id(&self) -> PartitionId {
        self.candidate.partition_id
    }

    /// Partition shard ID
    pub fn shard_id(&self) -> ShardId {
        self.candidate.shard_id
    }

    /// Partition namespace ID
    pub fn namespace_id(&self) -> NamespaceId {
        self.candidate.namespace_id
    }

    /// Partition table ID
    pub fn table_id(&self) -> TableId {
        self.candidate.table_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use data_types::{
        ColumnId, ColumnSet, CompactionLevel, ParquetFileParams, SequenceNumber, ShardIndex,
        Timestamp,
    };
    use iox_tests::util::TestCatalog;
    use iox_time::SystemProvider;
    use std::time::Duration;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_hot_partitions_to_compact() {
        let catalog = TestCatalog::new();

        // Create a db with 2 shards, one with 4 empty partitions and the other one with one
        // empty partition
        let mut txn = catalog.catalog.start_transaction().await.unwrap();

        let topic = txn.topics().create_or_get("foo").await.unwrap();
        let pool = txn.query_pools().create_or_get("foo").await.unwrap();
        let namespace = txn
            .namespaces()
            .create(
                "namespace_hot_partitions_to_compact",
                "inf",
                topic.id,
                pool.id,
            )
            .await
            .unwrap();
        let table = txn
            .tables()
            .create_or_get("test_table", namespace.id)
            .await
            .unwrap();
        let shard = txn
            .shards()
            .create_or_get(&topic, ShardIndex::new(1))
            .await
            .unwrap();
        let partition1 = txn
            .partitions()
            .create_or_get("one".into(), shard.id, table.id)
            .await
            .unwrap();
        let partition2 = txn
            .partitions()
            .create_or_get("two".into(), shard.id, table.id)
            .await
            .unwrap();
        let partition3 = txn
            .partitions()
            .create_or_get("three".into(), shard.id, table.id)
            .await
            .unwrap();
        let partition4 = txn
            .partitions()
            .create_or_get("four".into(), shard.id, table.id)
            .await
            .unwrap();
        // other shard
        let another_table = txn
            .tables()
            .create_or_get("another_test_table", namespace.id)
            .await
            .unwrap();
        let another_shard = txn
            .shards()
            .create_or_get(&topic, ShardIndex::new(2))
            .await
            .unwrap();
        let another_partition = txn
            .partitions()
            .create_or_get(
                "another_partition".into(),
                another_shard.id,
                another_table.id,
            )
            .await
            .unwrap();
        // update sort key for this another_partition
        let another_partition = txn
            .partitions()
            .update_sort_key(another_partition.id, &["tag1", "time"])
            .await
            .unwrap();
        txn.commit().await.unwrap();

        // Create a compactor
        let time_provider = Arc::new(SystemProvider::new());
        let config = make_compactor_config();
        let compactor = Compactor::new(
            vec![shard.id, another_shard.id],
            Arc::clone(&catalog.catalog),
            ParquetStorage::new(Arc::clone(&catalog.object_store)),
            Arc::new(Executor::new(1)),
            time_provider,
            BackoffConfig::default(),
            config,
            Arc::new(metric::Registry::new()),
        );

        // Some times in the past to set to created_at of the files
        let time_now = Timestamp::new(compactor.time_provider.now().timestamp_nanos());
        let time_three_hour_ago = Timestamp::new(
            (compactor.time_provider.now() - Duration::from_secs(60 * 60 * 3)).timestamp_nanos(),
        );
        let time_five_hour_ago = Timestamp::new(
            (compactor.time_provider.now() - Duration::from_secs(60 * 60 * 5)).timestamp_nanos(),
        );
        let time_38_hour_ago = Timestamp::new(
            (compactor.time_provider.now() - Duration::from_secs(60 * 60 * 38)).timestamp_nanos(),
        );

        // Basic parquet info
        let p1 = ParquetFileParams {
            shard_id: shard.id,
            namespace_id: namespace.id,
            table_id: table.id,
            partition_id: partition1.id,
            object_store_id: Uuid::new_v4(),
            max_sequence_number: SequenceNumber::new(100),
            min_time: Timestamp::new(1),
            max_time: Timestamp::new(5),
            file_size_bytes: 1337,
            row_count: 0,
            compaction_level: CompactionLevel::Initial, // level of file of new writes
            created_at: time_now,
            column_set: ColumnSet::new([ColumnId::new(1), ColumnId::new(2)]),
        };

        // Note: The order of the test cases below is important and should not be changed
        // because they depend on the order of the writes and their content. For example,
        // in order to test `Case 3`, we do not need to add asserts for `Case 1` and `Case 2`,
        // but all the writes, deletes and updates in Cases 1 and 2 are a must for testing Case 3.
        // In order words, the last Case needs all content of previous tests.
        // This shows the priority of selecting compaction candidates

        // --------------------------------------
        // Case 1: no files yet --> no partition candidates
        //
        let candidates = compactor.hot_partitions_to_compact(1, 1).await.unwrap();
        assert!(candidates.is_empty());

        // --------------------------------------
        // Case 2: no non-deleleted L0 files -->  no partition candidates
        //
        // partition1 has a deleted L0
        let mut txn = catalog.catalog.start_transaction().await.unwrap();
        let pf1 = txn.parquet_files().create(p1.clone()).await.unwrap();
        txn.parquet_files().flag_for_delete(pf1.id).await.unwrap();
        //
        // partition2 has a non-L0 file
        let p2 = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            partition_id: partition2.id,
            ..p1.clone()
        };
        let pf2 = txn.parquet_files().create(p2).await.unwrap();
        txn.parquet_files()
            .update_to_level_1(&[pf2.id])
            .await
            .unwrap();
        txn.commit().await.unwrap();
        // No non-deleted level 0 files yet --> no candidates
        let candidates = compactor.hot_partitions_to_compact(1, 1).await.unwrap();
        assert!(candidates.is_empty());

        // --------------------------------------
        // Case 3: no new recent writes (within the last 24 hours) --> no partition candidates
        // (the cold case will pick them up)
        //
        // partition2 has an old (more than 24 hours ago) non-deleted level 0 file
        let mut txn = catalog.catalog.start_transaction().await.unwrap();
        let p3 = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            partition_id: partition2.id,
            created_at: time_38_hour_ago,
            ..p1.clone()
        };
        let _pf3 = txn.parquet_files().create(p3).await.unwrap();
        txn.commit().await.unwrap();

        // No hot candidates
        let candidates = compactor.hot_partitions_to_compact(1, 1).await.unwrap();
        assert!(candidates.is_empty());

        // --------------------------------------
        // Case 4: has one partition with recent writes (5 hours ago) --> return that partition
        //
        // partition4 has a new write 5 hours ago
        let mut txn = catalog.catalog.start_transaction().await.unwrap();
        let p4 = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            partition_id: partition4.id,
            created_at: time_five_hour_ago,
            ..p1.clone()
        };
        let _pf4 = txn.parquet_files().create(p4).await.unwrap();
        txn.commit().await.unwrap();
        //
        // Has at least one partition with a recent write --> make it a candidate
        let candidates = compactor.hot_partitions_to_compact(1, 1).await.unwrap();
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].partition_id, partition4.id);

        // --------------------------------------
        // Case 5: has 2 partitions with 2 different groups of recent writes:
        //  1. Within the last 4 hours
        //  2. Within the last 24 hours but older than 4 hours ago
        // When we have group 1, we will ignore partitions in group 2
        //
        // partition3 has a new write 3 hours ago
        let mut txn = catalog.catalog.start_transaction().await.unwrap();
        let p5 = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            partition_id: partition3.id,
            created_at: time_three_hour_ago,
            ..p1.clone()
        };
        let _pf5 = txn.parquet_files().create(p5).await.unwrap();
        txn.commit().await.unwrap();
        //
        // make partitions in the most recent group candidates
        let candidates = compactor.hot_partitions_to_compact(1, 1).await.unwrap();
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].partition_id, partition3.id);

        // --------------------------------------
        // Case 6: has partition candidates for 2 shards
        //
        // The another_shard now has non-deleted level-0 file ingested 5 hours ago
        let mut txn = catalog.catalog.start_transaction().await.unwrap();
        let p6 = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            shard_id: another_shard.id,
            table_id: another_table.id,
            partition_id: another_partition.id,
            created_at: time_five_hour_ago,
            ..p1.clone()
        };
        let _pf6 = txn.parquet_files().create(p6).await.unwrap();
        txn.commit().await.unwrap();
        //
        // Will have 2 candidates, one for each shard
        let mut candidates = compactor.hot_partitions_to_compact(1, 1).await.unwrap();
        candidates.sort();
        assert_eq!(candidates.len(), 2);
        assert_eq!(candidates[0].partition_id, partition3.id);
        assert_eq!(candidates[0].shard_id, shard.id);
        assert_eq!(candidates[1].partition_id, another_partition.id);
        assert_eq!(candidates[1].shard_id, another_shard.id);

        // Add info to partition
        let partitions_with_info = compactor.add_info_to_partitions(&candidates).await.unwrap();
        assert_eq!(partitions_with_info.len(), 2);
        //
        assert_eq!(*partitions_with_info[0].namespace, namespace);
        assert_eq!(*partitions_with_info[0].table, table);
        assert_eq!(
            partitions_with_info[0].partition_key,
            partition3.partition_key
        );
        assert_eq!(partitions_with_info[0].sort_key, partition3.sort_key()); // this sort key is None
                                                                             //
        assert_eq!(*partitions_with_info[1].namespace, namespace);
        assert_eq!(*partitions_with_info[1].table, another_table);
        assert_eq!(
            partitions_with_info[1].partition_key,
            another_partition.partition_key
        );
        assert_eq!(
            partitions_with_info[1].sort_key,
            another_partition.sort_key()
        ); // this sort key is Some(tag1, time)
    }

    fn make_compactor_config() -> CompactorConfig {
        let max_desired_file_size_bytes = 10_000;
        let percentage_max_file_size = 30;
        let split_percentage = 80;
        let max_cold_concurrent_size_bytes = 90_000;
        let max_number_partitions_per_shard = 1;
        let min_number_recent_ingested_per_partition = 1;
        let cold_input_size_threshold_bytes = 600 * 1024 * 1024;
        let cold_input_file_count_threshold = 100;
        let hot_multiple = 4;
        let memory_budget_bytes = 10 * 1024 * 1024;
        CompactorConfig::new(
            max_desired_file_size_bytes,
            percentage_max_file_size,
            split_percentage,
            max_cold_concurrent_size_bytes,
            max_number_partitions_per_shard,
            min_number_recent_ingested_per_partition,
            cold_input_size_threshold_bytes,
            cold_input_file_count_threshold,
            hot_multiple,
            memory_budget_bytes,
        )
    }

    #[tokio::test]
    async fn test_cold_partitions_to_compact() {
        let catalog = TestCatalog::new();

        // Create a db with 2 shards, one with 4 empty partitions and the other one with one
        // empty partition
        let mut txn = catalog.catalog.start_transaction().await.unwrap();

        let topic = txn.topics().create_or_get("foo").await.unwrap();
        let pool = txn.query_pools().create_or_get("foo").await.unwrap();
        let namespace = txn
            .namespaces()
            .create(
                "namespace_hot_partitions_to_compact",
                "inf",
                topic.id,
                pool.id,
            )
            .await
            .unwrap();
        let table = txn
            .tables()
            .create_or_get("test_table", namespace.id)
            .await
            .unwrap();
        let shard = txn
            .shards()
            .create_or_get(&topic, ShardIndex::new(1))
            .await
            .unwrap();
        let partition1 = txn
            .partitions()
            .create_or_get("one".into(), shard.id, table.id)
            .await
            .unwrap();
        let partition2 = txn
            .partitions()
            .create_or_get("two".into(), shard.id, table.id)
            .await
            .unwrap();
        let partition3 = txn
            .partitions()
            .create_or_get("three".into(), shard.id, table.id)
            .await
            .unwrap();
        let partition4 = txn
            .partitions()
            .create_or_get("four".into(), shard.id, table.id)
            .await
            .unwrap();
        let partition5 = txn
            .partitions()
            .create_or_get("five".into(), shard.id, table.id)
            .await
            .unwrap();
        // other shard
        let another_table = txn
            .tables()
            .create_or_get("another_test_table", namespace.id)
            .await
            .unwrap();
        let another_shard = txn
            .shards()
            .create_or_get(&topic, ShardIndex::new(2))
            .await
            .unwrap();
        let another_partition = txn
            .partitions()
            .create_or_get(
                "another_partition".into(),
                another_shard.id,
                another_table.id,
            )
            .await
            .unwrap();
        // update sort key for this another_partition
        let another_partition = txn
            .partitions()
            .update_sort_key(another_partition.id, &["tag1", "time"])
            .await
            .unwrap();
        txn.commit().await.unwrap();

        // Create a compactor
        let time_provider = Arc::new(SystemProvider::new());
        let config = make_compactor_config();
        let compactor = Compactor::new(
            vec![shard.id, another_shard.id],
            Arc::clone(&catalog.catalog),
            ParquetStorage::new(Arc::clone(&catalog.object_store)),
            Arc::new(Executor::new(1)),
            time_provider,
            BackoffConfig::default(),
            config,
            Arc::new(metric::Registry::new()),
        );

        // Some times in the past to set to created_at of the files
        let time_five_hour_ago = Timestamp::new(
            (compactor.time_provider.now() - Duration::from_secs(60 * 60 * 5)).timestamp_nanos(),
        );
        let time_38_hour_ago = Timestamp::new(
            (compactor.time_provider.now() - Duration::from_secs(60 * 60 * 38)).timestamp_nanos(),
        );

        // Basic parquet info
        let p1 = ParquetFileParams {
            shard_id: shard.id,
            namespace_id: namespace.id,
            table_id: table.id,
            partition_id: partition1.id,
            object_store_id: Uuid::new_v4(),
            max_sequence_number: SequenceNumber::new(100),
            min_time: Timestamp::new(1),
            max_time: Timestamp::new(5),
            file_size_bytes: 1337,
            row_count: 0,
            compaction_level: CompactionLevel::Initial, // level of file of new writes
            created_at: time_38_hour_ago,               // create cold files by default
            column_set: ColumnSet::new([ColumnId::new(1), ColumnId::new(2)]),
        };

        // Note: The order of the test cases below is important and should not be changed
        // because they depend on the order of the writes and their content. For example,
        // in order to test `Case 3`, we do not need to add asserts for `Case 1` and `Case 2`,
        // but all the writes, deletes and updates in Cases 1 and 2 are a must for testing Case 3.
        // In order words, the last Case needs all content of previous tests.
        // This shows the priority of selecting compaction candidates

        // --------------------------------------
        // Case 1: no files yet --> no partition candidates
        //
        let candidates = compactor.cold_partitions_to_compact(1).await.unwrap();
        assert!(candidates.is_empty());

        // --------------------------------------
        // Case 2: no non-deleleted cold L0 files -->  no partition candidates
        //
        // partition1 has a cold deleted L0
        let mut txn = catalog.catalog.start_transaction().await.unwrap();
        let pf1 = txn.parquet_files().create(p1.clone()).await.unwrap();
        txn.parquet_files().flag_for_delete(pf1.id).await.unwrap();
        //
        // partition2 has a cold non-L0 file
        let p2 = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            partition_id: partition2.id,
            ..p1.clone()
        };
        let pf2 = txn.parquet_files().create(p2).await.unwrap();
        txn.parquet_files()
            .update_to_level_1(&[pf2.id])
            .await
            .unwrap();
        txn.commit().await.unwrap();
        // No non-deleted level 0 files yet --> no candidates
        let candidates = compactor.cold_partitions_to_compact(1).await.unwrap();
        assert!(candidates.is_empty());

        // --------------------------------------
        // Case 3: no new recent writes (within the last 24 hours) --> return that partition
        //
        // partition2 has a cold (more than 24 hours ago) non-deleted level 0 file
        let mut txn = catalog.catalog.start_transaction().await.unwrap();
        let p3 = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            partition_id: partition2.id,
            ..p1.clone()
        };
        let _pf3 = txn.parquet_files().create(p3).await.unwrap();
        txn.commit().await.unwrap();
        //
        // Has at least one partition with a L0 file --> make it a candidate
        let candidates = compactor.cold_partitions_to_compact(1).await.unwrap();
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].partition_id, partition2.id);

        // --------------------------------------
        // Case 4: has two cold partitions --> return the candidate with the most L0
        //
        // partition4 has two cold non-deleted level 0 files
        let mut txn = catalog.catalog.start_transaction().await.unwrap();
        let p4 = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            partition_id: partition4.id,
            ..p1.clone()
        };
        let _pf4 = txn.parquet_files().create(p4).await.unwrap();
        let p5 = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            partition_id: partition4.id,
            ..p1.clone()
        };
        let _pf5 = txn.parquet_files().create(p5).await.unwrap();
        txn.commit().await.unwrap();
        // Partition with the most l0 files is the candidate
        let candidates = compactor.cold_partitions_to_compact(1).await.unwrap();
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].partition_id, partition4.id);

        // --------------------------------------
        // Case 5: "warm" and "hot" partitions aren't returned
        //
        // partition3 has one cold level 0 file and one hot level 0 file
        // partition5 has one hot level 0 file
        let mut txn = catalog.catalog.start_transaction().await.unwrap();
        let p3_cold = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            partition_id: partition3.id,
            ..p1.clone()
        };
        let _pf3_cold = txn.parquet_files().create(p3_cold).await.unwrap();
        let p3_hot = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            partition_id: partition3.id,
            created_at: time_five_hour_ago,
            ..p1.clone()
        };
        let _pf3_hot = txn.parquet_files().create(p3_hot).await.unwrap();
        let p5_hot = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            partition_id: partition5.id,
            created_at: time_five_hour_ago,
            ..p1.clone()
        };
        let _pf5_hot = txn.parquet_files().create(p5_hot).await.unwrap();
        txn.commit().await.unwrap();
        // Partition4 is still the only candidate
        let candidates = compactor.cold_partitions_to_compact(1).await.unwrap();
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].partition_id, partition4.id);

        // Ask for 2 partitions per shard; get partition4 and partition2
        let candidates = compactor.cold_partitions_to_compact(2).await.unwrap();
        assert_eq!(candidates.len(), 2);
        assert_eq!(candidates[0].partition_id, partition4.id);
        assert_eq!(candidates[1].partition_id, partition2.id);

        // Ask for 3 partitions per shard; still get only partition4 and partition2
        let candidates = compactor.cold_partitions_to_compact(3).await.unwrap();
        assert_eq!(candidates.len(), 2);
        assert_eq!(candidates[0].partition_id, partition4.id);
        assert_eq!(candidates[1].partition_id, partition2.id);

        // --------------------------------------
        // Case 6: has partition candidates for 2 shards
        //
        // The another_shard now has non-deleted level-0 file ingested 38 hours ago
        let mut txn = catalog.catalog.start_transaction().await.unwrap();
        let p6 = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            shard_id: another_shard.id,
            table_id: another_table.id,
            partition_id: another_partition.id,
            created_at: time_38_hour_ago,
            ..p1.clone()
        };
        let _pf6 = txn.parquet_files().create(p6).await.unwrap();
        txn.commit().await.unwrap();

        // Will have 2 candidates, one for each shard
        let mut candidates = compactor.cold_partitions_to_compact(1).await.unwrap();
        candidates.sort();
        assert_eq!(candidates.len(), 2);
        assert_eq!(candidates[0].partition_id, partition4.id);
        assert_eq!(candidates[0].shard_id, shard.id);
        assert_eq!(candidates[1].partition_id, another_partition.id);
        assert_eq!(candidates[1].shard_id, another_shard.id);

        // Ask for 2 candidates per shard; get back 3: 2 from shard and 1 from
        // another_shard
        let mut candidates = compactor.cold_partitions_to_compact(2).await.unwrap();
        candidates.sort();
        assert_eq!(candidates.len(), 3);
        assert_eq!(candidates[0].partition_id, partition2.id);
        assert_eq!(candidates[0].shard_id, shard.id);
        assert_eq!(candidates[1].partition_id, partition4.id);
        assert_eq!(candidates[1].shard_id, shard.id);
        assert_eq!(candidates[2].partition_id, another_partition.id);
        assert_eq!(candidates[2].shard_id, another_shard.id);
    }
}
