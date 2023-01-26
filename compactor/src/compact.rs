//! Data Points for the lifecycle of the Compactor

use crate::{handler::CompactorConfig, parquet_file_lookup::CompactionType};
use backoff::BackoffConfig;
use data_types::{
    ColumnType, ColumnTypeCount, Namespace, NamespaceId, PartitionId, PartitionKey, PartitionParam,
    ShardId, Table, TableId, TableSchema, Timestamp,
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
    collections::{HashMap, HashSet},
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
        "Error getting partitions with recently created files for {} compaction. {:?}",
        compaction_type,
        source,
    ))]
    RecentIngestedPartitions {
        source: iox_catalog::interface::Error,
        compaction_type: CompactionType,
    },

    #[snafu(display(
        "Error getting partitions with small L1 files for warm compaction for shard {:?}. {}",
        shard_id,
        source
    ))]
    PartitionsWithSmallL1Files {
        source: iox_catalog::interface::Error,
        shard_id: Option<ShardId>,
    },
}

/// A specialized `Error` for Compactor Data errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A temporary type for the transition between the write buffer architecture and the RPC write
/// path. In the RPC write path, shards are irrelevant, so the compactor should look for data to
/// compact across all shards.
#[derive(Debug)]
pub enum ShardAssignment {
    /// Compact everything regardless of shard (for the RPC path that doesn't use shards)
    All,
    /// Compact only partitions on these specified shards
    Only(Vec<ShardId>),
}

impl ShardAssignment {
    /// Useful for multiplying to get a total capacity needed for, say, number of partitions per
    /// shard needed.
    pub(crate) fn len(&self) -> usize {
        match self {
            // When querying all shards, there is no "per shard" amount, only the total.
            Self::All => 1,
            Self::Only(v) => v.len(),
        }
    }
}

/// Data points needed to run a compactor
#[derive(Debug)]
pub struct Compactor {
    /// Shards assigned to this compactor
    pub(crate) shards: ShardAssignment,

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
    pub(crate) compaction_candidate_gauge: Metric<U64Gauge>,

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
        shards: ShardAssignment,
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
            50 * 1024,         // 50KB
            500 * 1024,        // 500 KB
            1024 * 1024,       // 1 MB
            3 * 1024 * 1024,   // 3 MB
            10 * 1024 * 1024,  // 10 MB
            30 * 1024 * 1024,  // 30 MB
            100 * 1024 * 1024, // 100 MB
            500 * 1024 * 1024, // 500 MB
            u64::MAX,          // Inf
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
            Duration::from_millis(500),
            Duration::from_millis(1_000), // 1 second
            Duration::from_millis(5_000),
            Duration::from_millis(15_000),
            Duration::from_millis(30_000),
            Duration::from_millis(60_000), // 1 minute
            Duration::from_millis(5 * 60_000),
            Duration::from_millis(15 * 60_000),
            Duration::from_millis(60 * 60_000),
            DURATION_MAX,
        ]);
        let compaction_duration: Metric<DurationHistogram> = registry.register_metric_with_options(
            "compactor_compact_partition_duration",
            "Compact partition duration",
            || duration_histogram_options.clone(),
        );

        let candidate_selection_duration: Metric<DurationHistogram> = registry
            .register_metric_with_options(
                "compactor_candidate_selection_duration",
                "Duration to select compaction partition candidates",
                || duration_histogram_options.clone(),
            );

        let partitions_extra_info_reading_duration: Metric<DurationHistogram> = registry
            .register_metric_with_options(
                "compactor_partitions_extra_info_reading_duration",
                "Duration to read and add extra information into selected partition candidates",
                || duration_histogram_options.clone(),
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

    /// Access to the TimeProvider
    pub fn time_provider(&self) -> Arc<dyn TimeProvider> {
        Arc::clone(&self.time_provider) as _
    }

    /// hours to nanos for all thresholds
    pub fn threshold_times(
        time_provider: Arc<dyn TimeProvider>,
        hours_thresholds: Vec<u64>,
    ) -> Vec<(u64, Timestamp)> {
        hours_thresholds
            .iter()
            .map(|&num_hours| {
                (
                    num_hours,
                    Timestamp::from(time_provider.hours_ago(num_hours)),
                )
            })
            .collect()
    }

    /// Partition candidate to compact
    /// This is a list of partitions that have new created files (any level) after
    /// the given partition_candidate_hours_threshold
    pub async fn partitions_to_compact(
        &self,
        compaction_type: CompactionType,
        partition_candidate_hours_thresholds: Vec<u64>,
        max_num_partitions: usize,
    ) -> Result<Vec<Arc<PartitionCompactionCandidateWithInfo>>> {
        let threshold_times = Self::threshold_times(
            Arc::clone(&self.time_provider),
            partition_candidate_hours_thresholds,
        );

        let candidates = Self::partition_candidates(
            Arc::clone(&self.catalog),
            &threshold_times,
            compaction_type,
            max_num_partitions,
        )
        .await?;
        let num_partitions = candidates.len();

        debug!(n = num_partitions, %compaction_type, "compaction candidates",);

        // Get extra needed information for selected partitions
        let start_time = self.time_provider.now();

        // Column types and their counts of the tables of the partition candidates
        debug!(
            num_candidates=?candidates.len(),
            %compaction_type,
            "start getting column types for the partition candidates"
        );

        let table_columns = self.table_columns(&candidates).await?;

        // Add other compaction-needed info into selected partitions
        debug!(
            num_candidates=?candidates.len(),
            %compaction_type,
            "start getting additional info for the partition candidates"
        );
        let candidates = self
            .add_info_to_partitions(&candidates, &table_columns)
            .await?;

        if let Some(delta) = self.time_provider.now().checked_duration_since(start_time) {
            let attributes =
                Attributes::from([("partition_type", compaction_type.to_string().into())]);
            let duration = self
                .partitions_extra_info_reading_duration
                .recorder(attributes);
            duration.record(delta);
        }

        Ok(candidates)
    }

    /// Get partition candidates with new created files (any level) after the thresholds
    /// The threshold is ordered. The function returns right after there are candidates for a threshold
    pub async fn partition_candidates(
        catalog: Arc<dyn Catalog>,
        thresholds: &[(u64, Timestamp)],
        compaction_type: CompactionType,
        max_num_partitions: usize,
    ) -> Result<Vec<PartitionParam>> {
        let mut repos = catalog.repositories().await;

        for &(hours_ago, hours_ago_in_ns) in thresholds {
            let partitions = repos
                .partitions()
                .partitions_with_recent_created_files(hours_ago_in_ns, max_num_partitions)
                .await
                .context(RecentIngestedPartitionsSnafu { compaction_type })?;
            if !partitions.is_empty() {
                debug!(
                    %compaction_type,
                    hours_ago,
                    n = partitions.len(),
                    "found partition candidates"
                );
                return Ok(partitions);
            }
        }

        Ok(Vec::new())
    }

    /// Get column types for tables of given partitions
    pub(crate) async fn table_columns(
        &self,
        partitions: &[PartitionParam],
    ) -> Result<HashMap<TableId, Vec<ColumnTypeCount>>> {
        let mut repos = self.catalog.repositories().await;

        let table_ids: HashSet<_> = partitions.iter().map(|p| p.table_id).collect();
        let mut result = HashMap::with_capacity(table_ids.len());
        for table_id in table_ids {
            let cols = repos
                .columns()
                .list_type_count_by_table_id(table_id)
                .await
                .context(QueryingColumnSnafu)?;
            result.insert(table_id, cols);
        }

        Ok(result)
    }

    /// Add namespace and table information to partition candidates.
    pub(crate) async fn add_info_to_partitions(
        &self,
        partitions: &[PartitionParam],
        table_columns: &HashMap<TableId, Vec<ColumnTypeCount>>,
    ) -> Result<Vec<Arc<PartitionCompactionCandidateWithInfo>>> {
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
                let column_type_counts = table_columns
                    .get(&p.table_id)
                    .expect("just queried")
                    .clone();

                Arc::new(PartitionCompactionCandidateWithInfo {
                    table: Arc::clone(table),
                    table_schema: Arc::clone(table_schema),
                    column_type_counts,
                    namespace: Arc::clone(
                        &namespaces.get(&p.namespace_id).expect("just queried").0,
                    ),
                    candidate: *p,
                    sort_key: part.sort_key(),
                    partition_key: part.partition_key.clone(),
                })
            })
            .collect())
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

    /// Counts of the number of columns of each type, used for estimating arrow size
    pub column_type_counts: Vec<ColumnTypeCount>,

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

    /// Estimate the amount of memory needed to work with this parquet file based on the count of
    /// columns of different types in this partition and the number of rows in the parquet file.
    pub fn estimated_arrow_bytes(
        &self,
        min_num_rows_allocated_per_record_batch_to_datafusion_plan: u64,
    ) -> u64 {
        estimate_arrow_bytes_for_file(
            &self.column_type_counts,
            min_num_rows_allocated_per_record_batch_to_datafusion_plan,
        )
    }
}

fn estimate_arrow_bytes_for_file(
    columns: &[ColumnTypeCount],
    min_num_rows_allocated_per_record_batch_to_datafusion_plan: u64,
) -> u64 {
    const AVERAGE_TAG_VALUE_LENGTH: i64 = 200;
    const STRING_LENGTH: i64 = 1000;
    const DICTIONARY_BYTE: i64 = 8;
    const VALUE_BYTE: i64 = 8;
    const BOOL_BYTE: i64 = 1;
    const AVERAGE_ROW_COUNT_CARDINALITY_RATIO: i64 = 2;

    // Since DataFusion streams files and allocates a fixed (configurable) number of rows per batch,
    // we always use that number to estimate the memory usage per batch.
    let row_count_per_batch = min_num_rows_allocated_per_record_batch_to_datafusion_plan as i64;

    let average_cardinality = row_count_per_batch / AVERAGE_ROW_COUNT_CARDINALITY_RATIO;

    // Bytes needed for number columns
    let mut value_bytes = 0;
    let mut string_bytes = 0;
    let mut bool_bytes = 0;
    let mut dictionary_key_bytes = 0;
    let mut dictionary_value_bytes = 0;
    for c in columns {
        match c.col_type {
            ColumnType::I64 | ColumnType::U64 | ColumnType::F64 | ColumnType::Time => {
                value_bytes += c.count * row_count_per_batch * VALUE_BYTE;
            }
            ColumnType::String => {
                string_bytes += row_count_per_batch * STRING_LENGTH;
            }
            ColumnType::Bool => {
                bool_bytes += row_count_per_batch * BOOL_BYTE;
            }
            ColumnType::Tag => {
                dictionary_key_bytes += average_cardinality * AVERAGE_TAG_VALUE_LENGTH;
                dictionary_value_bytes = row_count_per_batch * DICTIONARY_BYTE;
            }
        }
    }

    let estimated_arrow_bytes_for_file =
        value_bytes + string_bytes + bool_bytes + dictionary_key_bytes + dictionary_value_bytes;

    estimated_arrow_bytes_for_file as u64
}

#[cfg(test)]
pub mod tests {
    use crate::parquet_file_lookup::{self, CompactionType};

    use super::*;
    use data_types::{
        ColumnId, ColumnSet, CompactionLevel, ParquetFileParams, SequenceNumber, ShardIndex,
        Timestamp,
    };
    use iox_tests::util::{TestCatalog, TestPartition};
    use iox_time::SystemProvider;
    use parquet_file::storage::StorageId;
    use uuid::Uuid;

    const DEFAULT_HOT_COMPACTION_HOURS_THRESHOLD_1: u64 = 4;
    const DEFAULT_HOT_COMPACTION_HOURS_THRESHOLD_2: u64 = 24;
    const DEFAULT_COLD_PARTITION_CANDIDATES_HOURS_THRESHOLD: u64 = 24;
    const DEFAULT_MAX_PARALLEL_PARTITIONS: u64 = 20;
    const DEFAULT_MAX_NUM_PARTITION_CANDIDATES: usize = 10;

    impl PartitionCompactionCandidateWithInfo {
        pub(crate) async fn from_test_partition(test_partition: &TestPartition) -> Self {
            Self {
                table: Arc::new(test_partition.table.table.clone()),
                table_schema: Arc::new(test_partition.table.catalog_schema().await),
                column_type_counts: Vec::new(), // not relevant
                namespace: Arc::new(test_partition.namespace.namespace.clone()),
                candidate: PartitionParam {
                    partition_id: test_partition.partition.id,
                    shard_id: test_partition.partition.shard_id,
                    namespace_id: test_partition.namespace.namespace.id,
                    table_id: test_partition.partition.table_id,
                },
                sort_key: test_partition.partition.sort_key(),
                partition_key: test_partition.partition.partition_key.clone(),
            }
        }
    }

    #[test]
    fn test_estimate_arrow_bytes_for_file_small_row_count() {
        // Always use this config param to estimate memory usage for each batch
        // no matter it is larger or smaller than row_count
        let min_num_rows_allocated_per_record_batch = 20;

        // Time, U64, I64, F64
        let columns = vec![
            ColumnTypeCount::new(ColumnType::Time, 1),
            ColumnTypeCount::new(ColumnType::U64, 2),
            ColumnTypeCount::new(ColumnType::F64, 3),
            ColumnTypeCount::new(ColumnType::I64, 4),
        ];
        let bytes =
            estimate_arrow_bytes_for_file(&columns, min_num_rows_allocated_per_record_batch);
        assert_eq!(bytes, 1600); // 20 * (1+2+3+4) * 8

        // Tag
        let columns = vec![ColumnTypeCount::new(ColumnType::Tag, 1)];
        let bytes =
            estimate_arrow_bytes_for_file(&columns, min_num_rows_allocated_per_record_batch);
        assert_eq!(bytes, 2160); // 10 * 200 + 20 * 8

        // String
        let columns = vec![ColumnTypeCount::new(ColumnType::String, 1)];
        let bytes =
            estimate_arrow_bytes_for_file(&columns, min_num_rows_allocated_per_record_batch);
        assert_eq!(bytes, 20000); // 20 * 1000

        // Bool
        let columns = vec![ColumnTypeCount::new(ColumnType::Bool, 1)];
        let bytes =
            estimate_arrow_bytes_for_file(&columns, min_num_rows_allocated_per_record_batch);
        assert_eq!(bytes, 20); // 20 * 1

        // all types
        let columns = vec![
            ColumnTypeCount::new(ColumnType::Time, 1),
            ColumnTypeCount::new(ColumnType::U64, 2),
            ColumnTypeCount::new(ColumnType::F64, 3),
            ColumnTypeCount::new(ColumnType::I64, 4),
            ColumnTypeCount::new(ColumnType::Tag, 1),
            ColumnTypeCount::new(ColumnType::String, 1),
            ColumnTypeCount::new(ColumnType::Bool, 1),
        ];
        let bytes =
            estimate_arrow_bytes_for_file(&columns, min_num_rows_allocated_per_record_batch);
        assert_eq!(bytes, 23780); // 1600 + 2160 + 20000 + 20
    }

    #[test]
    fn test_estimate_arrow_bytes_for_file_large_row_count() {
        // Always use this config param to estimate memory usage for each batch
        // even if it is smaller than row_count
        let min_num_rows_allocated_per_record_batch = 10;

        // Time, U64, I64, F64
        let columns = vec![
            ColumnTypeCount::new(ColumnType::Time, 1),
            ColumnTypeCount::new(ColumnType::U64, 2),
            ColumnTypeCount::new(ColumnType::F64, 3),
            ColumnTypeCount::new(ColumnType::I64, 4),
        ];
        let bytes =
            estimate_arrow_bytes_for_file(&columns, min_num_rows_allocated_per_record_batch);
        assert_eq!(bytes, 800); // 10 * (1+2+3+4) * 8

        // Tag
        let columns = vec![ColumnTypeCount::new(ColumnType::Tag, 1)];
        let bytes =
            estimate_arrow_bytes_for_file(&columns, min_num_rows_allocated_per_record_batch);
        assert_eq!(bytes, 1080); // 5 * 200 + 10 * 8

        // String
        let columns = vec![ColumnTypeCount::new(ColumnType::String, 1)];
        let bytes =
            estimate_arrow_bytes_for_file(&columns, min_num_rows_allocated_per_record_batch);
        assert_eq!(bytes, 10000); // 10 * 1000

        // Bool
        let columns = vec![ColumnTypeCount::new(ColumnType::Bool, 1)];
        let bytes =
            estimate_arrow_bytes_for_file(&columns, min_num_rows_allocated_per_record_batch);
        assert_eq!(bytes, 10); // 10 * 1

        // all types
        let columns = vec![
            ColumnTypeCount::new(ColumnType::Time, 1),
            ColumnTypeCount::new(ColumnType::U64, 2),
            ColumnTypeCount::new(ColumnType::F64, 3),
            ColumnTypeCount::new(ColumnType::I64, 4),
            ColumnTypeCount::new(ColumnType::Tag, 1),
            ColumnTypeCount::new(ColumnType::String, 1),
            ColumnTypeCount::new(ColumnType::Bool, 1),
        ];
        let bytes =
            estimate_arrow_bytes_for_file(&columns, min_num_rows_allocated_per_record_batch);
        assert_eq!(bytes, 11890); // 800 + 1080 + 10000 + 10
    }

    fn make_compactor_config() -> CompactorConfig {
        CompactorConfig {
            max_desired_file_size_bytes: 10_000,
            percentage_max_file_size: 30,
            split_percentage: 80,
            max_number_partitions_per_shard: 1,
            min_number_recent_ingested_files_per_partition: 1,
            hot_multiple: 4,
            warm_multiple: 1,
            memory_budget_bytes: 10 * 1024 * 1024,
            min_num_rows_allocated_per_record_batch_to_datafusion_plan: 100,
            max_num_compacting_files: 20,
            max_num_compacting_files_first_in_partition: 40,
            minutes_without_new_writes_to_be_cold: 10,
            cold_partition_candidates_hours_threshold:
                DEFAULT_COLD_PARTITION_CANDIDATES_HOURS_THRESHOLD,
            hot_compaction_hours_threshold_1: DEFAULT_HOT_COMPACTION_HOURS_THRESHOLD_1,
            hot_compaction_hours_threshold_2: DEFAULT_HOT_COMPACTION_HOURS_THRESHOLD_2,
            max_parallel_partitions: DEFAULT_MAX_PARALLEL_PARTITIONS,
            warm_partition_candidates_hours_threshold:
                DEFAULT_COLD_PARTITION_CANDIDATES_HOURS_THRESHOLD,
            warm_compaction_small_size_threshold_bytes: 5_000,
            warm_compaction_min_small_file_count: 10,
        }
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
                None,
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
            .cas_sort_key(
                another_partition.id,
                Some(another_partition.sort_key),
                &["tag1", "time"],
            )
            .await
            .unwrap();
        txn.commit().await.unwrap();

        // Create a compactor
        let time_provider = Arc::new(SystemProvider::new());
        let mut config = make_compactor_config();
        // 24-hour threshold: partitions with any files created after this threshold will be selected as
        // cold partition candidates. Only candidates that meet another condition below will actually get cold compacted
        config.hot_compaction_hours_threshold_2 = DEFAULT_HOT_COMPACTION_HOURS_THRESHOLD_2;
        config.max_number_partitions_per_shard = DEFAULT_MAX_NUM_PARTITION_CANDIDATES;
        // 8 hours without new writes for the candidates to actually get cold compaction
        config.minutes_without_new_writes_to_be_cold = 8 * 60;
        let compactor = Arc::new(Compactor::new(
            ShardAssignment::Only(vec![shard.id, another_shard.id]),
            Arc::clone(&catalog.catalog),
            ParquetStorage::new(Arc::clone(&catalog.object_store), StorageId::from("iox")),
            catalog.exec(),
            time_provider,
            BackoffConfig::default(),
            config,
            Arc::new(metric::Registry::new()),
        ));

        // Some times in the past to set to created_at of the files
        let time_5_hour_ago = Timestamp::from(compactor.time_provider.hours_ago(5));
        let time_9_hour_ago = Timestamp::from(compactor.time_provider.hours_ago(9));

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
            created_at: time_9_hour_ago,                // create cold files by default
            column_set: ColumnSet::new([ColumnId::new(1), ColumnId::new(2)]),
            max_l0_created_at: time_9_hour_ago,
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
        let compaction_type = CompactionType::Cold;
        let hour_threshold = compactor.config.cold_partition_candidates_hours_threshold;
        let max_num_partitions = compactor.config.max_number_partitions_per_shard;
        let candidates = compactor
            .partitions_to_compact(compaction_type, vec![hour_threshold], max_num_partitions)
            .await
            .unwrap();
        assert!(candidates.is_empty());

        // --------------------------------------
        // Case 2: 2 partitions with file created recently (newer than the threshold to selct for cold candidates) -> 2 partition candidates
        //
        // partition1 has a cold deleted L0 created recently (default 9 hours ago) --> a cold candidate
        let mut txn = catalog.catalog.start_transaction().await.unwrap();
        let pf1 = txn.parquet_files().create(p1.clone()).await.unwrap();
        txn.parquet_files().flag_for_delete(pf1.id).await.unwrap();
        //
        // partition2 has only a L2 file created recently (default 9 hours ago) --> not a candidate
        let p2 = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            partition_id: partition2.id,
            compaction_level: CompactionLevel::Final,
            ..p1.clone()
        };
        let _pf2 = txn.parquet_files().create(p2).await.unwrap();
        txn.commit().await.unwrap();
        // only partition 1 is returned as a candidate
        let candidates = compactor
            .partitions_to_compact(compaction_type, vec![hour_threshold], max_num_partitions)
            .await
            .unwrap();
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].candidate.partition_id, partition1.id);
        //
        // verify no candidates will actualy get cold compaction because they are not old enough
        let files = parquet_file_lookup::ParquetFilesForCompaction::for_partition(
            Arc::clone(&compactor),
            Arc::clone(&candidates[0]),
            CompactionType::Cold,
        )
        .await
        .unwrap();
        assert!(files.is_none());

        // --------------------------------------
        // Case 3: no new recent writes (within the last 8 hours) --> return that partition
        //
        // partition2 has a cold (more than 8 hours ago) non-deleted level 0 file
        let mut txn = catalog.catalog.start_transaction().await.unwrap();
        let p3 = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            partition_id: partition2.id,
            ..p1.clone()
        };
        let _pf3 = txn.parquet_files().create(p3).await.unwrap();
        txn.commit().await.unwrap();
        //
        // Return 2 candidates
        let candidates = compactor
            .partitions_to_compact(compaction_type, vec![hour_threshold], max_num_partitions)
            .await
            .unwrap();
        assert_eq!(candidates.len(), 2);
        // sort candidates on partition_id
        let mut candidates = candidates.into_iter().collect::<Vec<_>>();
        candidates.sort_by_key(|c| c.candidate.partition_id);
        assert_eq!(candidates[0].candidate.partition_id, partition1.id);
        assert_eq!(candidates[1].candidate.partition_id, partition2.id);
        //
        // verify only partition2 is qualified for cold compaction
        // partition1
        let files = parquet_file_lookup::ParquetFilesForCompaction::for_partition(
            Arc::clone(&compactor),
            Arc::clone(&candidates[0]),
            CompactionType::Cold,
        )
        .await
        .unwrap();
        assert!(files.is_none());
        // partition2
        let files = parquet_file_lookup::ParquetFilesForCompaction::for_partition(
            Arc::clone(&compactor),
            Arc::clone(&candidates[1]),
            CompactionType::Cold,
        )
        .await
        .unwrap();
        assert!(files.is_some());

        // --------------------------------------
        // Case 4: has two actual cold partitions but three cold candidates --> return all 3 candidates
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
        // Three candidates
        let candidates = compactor
            .partitions_to_compact(compaction_type, vec![hour_threshold], max_num_partitions)
            .await
            .unwrap();
        assert_eq!(candidates.len(), 3);
        // sort candidates on partition_id
        let mut candidates = candidates.into_iter().collect::<Vec<_>>();
        candidates.sort_by_key(|c| c.candidate.partition_id);
        assert_eq!(candidates[0].candidate.partition_id, partition1.id);
        assert_eq!(candidates[1].candidate.partition_id, partition2.id);
        assert_eq!(candidates[2].candidate.partition_id, partition4.id);
        //
        //  verify two candidates, partition2 and partition4, are qualified for cold compaction
        // partition1
        let files = parquet_file_lookup::ParquetFilesForCompaction::for_partition(
            Arc::clone(&compactor),
            Arc::clone(&candidates[0]),
            CompactionType::Cold,
        )
        .await
        .unwrap();
        assert!(files.is_none());
        // partition2
        let files = parquet_file_lookup::ParquetFilesForCompaction::for_partition(
            Arc::clone(&compactor),
            Arc::clone(&candidates[1]),
            CompactionType::Cold,
        )
        .await
        .unwrap();
        assert!(files.is_some());
        // partition4
        let files = parquet_file_lookup::ParquetFilesForCompaction::for_partition(
            Arc::clone(&compactor),
            Arc::clone(&candidates[2]),
            CompactionType::Cold,
        )
        .await
        .unwrap();
        assert!(files.is_some());

        // --------------------------------------
        // Case 5: Add 2 more "warm"/"hot" partitions --> 5 candidates will be returned
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
            created_at: time_5_hour_ago,
            ..p1.clone()
        };
        let _pf3_hot = txn.parquet_files().create(p3_hot).await.unwrap();
        let p5_hot = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            partition_id: partition5.id,
            created_at: time_5_hour_ago,
            ..p1.clone()
        };
        let _pf5_hot = txn.parquet_files().create(p5_hot).await.unwrap();
        txn.commit().await.unwrap();
        // Ask for only 1 first-read candidate
        let candidates = compactor
            .partitions_to_compact(compaction_type, vec![hour_threshold], 1)
            .await
            .unwrap();
        assert_eq!(candidates.len(), 1);

        // Ask for 2 first-read partitions
        let candidates = compactor
            .partitions_to_compact(compaction_type, vec![hour_threshold], 2)
            .await
            .unwrap();
        assert_eq!(candidates.len(), 2);

        // Ask for a lot of partitions -> return all 5 candidates
        let candidates = compactor
            .partitions_to_compact(compaction_type, vec![hour_threshold], max_num_partitions)
            .await
            .unwrap();
        assert_eq!(candidates.len(), 5);
        // sort candidates on partition_id
        let mut candidates = candidates.into_iter().collect::<Vec<_>>();
        candidates.sort_by_key(|c| c.candidate.partition_id);
        assert_eq!(candidates[0].candidate.partition_id, partition1.id);
        assert_eq!(candidates[1].candidate.partition_id, partition2.id);
        assert_eq!(candidates[2].candidate.partition_id, partition3.id);
        assert_eq!(candidates[3].candidate.partition_id, partition4.id);
        assert_eq!(candidates[4].candidate.partition_id, partition5.id);
        //
        // verify still two candidates partition2 and partition4, are qualified for cold compaction
        // partition1
        let files = parquet_file_lookup::ParquetFilesForCompaction::for_partition(
            Arc::clone(&compactor),
            Arc::clone(&candidates[0]),
            CompactionType::Cold,
        )
        .await
        .unwrap();
        assert!(files.is_none());
        // partition2
        let files = parquet_file_lookup::ParquetFilesForCompaction::for_partition(
            Arc::clone(&compactor),
            Arc::clone(&candidates[1]),
            CompactionType::Cold,
        )
        .await
        .unwrap();
        assert!(files.is_some());
        // partition3
        let files = parquet_file_lookup::ParquetFilesForCompaction::for_partition(
            Arc::clone(&compactor),
            Arc::clone(&candidates[2]),
            CompactionType::Cold,
        )
        .await
        .unwrap();
        assert!(files.is_none());
        // partition4
        let files = parquet_file_lookup::ParquetFilesForCompaction::for_partition(
            Arc::clone(&compactor),
            Arc::clone(&candidates[3]),
            CompactionType::Cold,
        )
        .await
        .unwrap();
        assert!(files.is_some());
        // partition5
        let files = parquet_file_lookup::ParquetFilesForCompaction::for_partition(
            Arc::clone(&compactor),
            Arc::clone(&candidates[4]),
            CompactionType::Cold,
        )
        .await
        .unwrap();
        assert!(files.is_none());

        // --------------------------------------
        // Case 6: has partition candidates for 2 shards
        //
        // The another_shard now has non-deleted level-0 file ingested 1 hour after config.hot_compaction_hours_threshold_2
        let file_created_time = Timestamp::from(
            compactor
                .time_provider
                .hours_ago(config.hot_compaction_hours_threshold_2 - 1),
        );
        let mut txn = catalog.catalog.start_transaction().await.unwrap();
        let p6 = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            shard_id: another_shard.id,
            table_id: another_table.id,
            partition_id: another_partition.id,
            created_at: file_created_time,
            ..p1.clone()
        };
        let _pf6 = txn.parquet_files().create(p6).await.unwrap();
        txn.commit().await.unwrap();

        // Will have 6 candidates
        let candidates = compactor
            .partitions_to_compact(compaction_type, vec![hour_threshold], max_num_partitions)
            .await
            .unwrap();
        assert_eq!(candidates.len(), 6);
        // sort candidates on partition_id
        let mut candidates = candidates.into_iter().collect::<Vec<_>>();
        candidates.sort_by_key(|c| c.candidate.partition_id);
        assert_eq!(candidates[0].candidate.partition_id, partition1.id);
        assert_eq!(candidates[1].candidate.partition_id, partition2.id);
        assert_eq!(candidates[2].candidate.partition_id, partition3.id);
        assert_eq!(candidates[3].candidate.partition_id, partition4.id);
        assert_eq!(candidates[4].candidate.partition_id, partition5.id);
        assert_eq!(candidates[5].candidate.partition_id, another_partition.id);

        // --------------------------------------
        // Test limit of number of partitions to compact
        let candidates = compactor
            .partitions_to_compact(compaction_type, vec![hour_threshold], 3)
            .await
            .unwrap();
        // Since we do not prioritize partitions (for now), only need to check the number of candidates
        // We are testing and the goal is to keep this limit high and do not have to think about prioritization
        assert_eq!(candidates.len(), 3);
    }
}
