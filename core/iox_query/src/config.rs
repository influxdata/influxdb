use std::{str::FromStr, time::Duration};

use datafusion::{
    common::{Result, extensions_options},
    config::{ConfigExtension, ConfigField, Visit},
    error::DataFusionError,
};
use datafusion_udf_wasm_host::AllowCertainHttpRequests;

/// IOx-specific config extension prefix.
pub const IOX_CONFIG_PREFIX: &str = "iox";

extensions_options! {
    /// Config options for IOx.
    pub struct IoxConfigExt {
        /// When splitting de-duplicate operations based on IOx partitions[^iox_part] and time, this is the maximum number of groups
        /// that should be considered. If there are more groups, the split will NOT be performed.
        ///
        /// This protects against certain highly degenerative plans.
        ///
        ///
        /// [^iox_part]: "IOx partition" refers to a partition within the IOx catalog, i.e. a partition within the
        ///              primary key space. This is NOT the same as a DataFusion partition which refers to a stream
        ///              within the physical plan data flow.
        pub max_dedup_split: usize, default = 100

        /// When multiple parquet files are required in a sorted way (e.g. for de-duplication), we have two options:
        ///
        /// 1. **In-mem sorting:** Put them into [`target_partitions`] DataFusion partitions. This limits the fan-out,
        ///    but requires that we potentially chain multiple parquet files into a single DataFusion partition. Since
        ///    chaining sorted data does NOT automatically result in sorted data (e.g. AB-AB is not sorted), we need to
        ///    preform an in-memory sort using [`SortExec`] afterwards. This is expensive.
        /// 2. **Fan-out:** Instead of chaining files within DataFusion partitions, we can accept a fan-out beyond
        ///    [`target_partitions`]. This prevents in-memory sorting but may result in OOMs (out-of-memory).
        ///
        /// We try to pick option 2 up to a certain number of files, which is configured by this setting.
        ///
        ///
        /// [`SortExec`]: datafusion::physical_plan::sorts::sort::SortExec
        /// [`target_partitions`]: datafusion::common::config::ExecutionOptions::target_partitions
        pub max_parquet_fanout: usize, default = 40

        /// Number of input streams to prefect for ProgressiveEvalExec
        /// Since ProgressiveEvalExec only polls one stream at a time in their stream order,
        /// we do not need to prefetch all streams at once to save resources. However, if the
        /// streams' IO time is way more than their CPU/procesing time, prefetching them will help
        /// improve the performance.
        /// Default is 2 which means we will prefetch one extra stream before polling the current one.
        /// Increasing this value if IO time to read a stream is often much more than CPU time to process its previous one.
        pub progressive_eval_num_prefetch_input_streams: usize, default = 2

        /// Cuttoff date for InfluxQL metadata queries.
        pub influxql_metadata_cutoff: MetadataCutoff, default = MetadataCutoff::Relative(Duration::from_secs(3600 * 24))

        /// Limit for the number of partitions to scan in a single query. Zero means no limit.
        pub partition_limit: usize, default = 0

        /// Limit for the number of parquet files to scan in a single query. Zero means no limit.
        pub parquet_file_limit: usize, default = 0

        /// Rewrite a regular expression predicate into an `IN` list where it matches a finite set
        /// of values.
        ///
        /// A regular expression that is anchored at both ends and describes a finite set of strings can
        /// be rewritten: `tag ~ '^val(1|2|3)$'` becomes `tag IN ('val1', 'val2', 'val3')`, which
        /// is cheaper per row.
        pub use_regex_to_in_list: bool, default = true

        /// Limit for the number of values a regular expression predicate may be rewritten into an
        /// `IN` list of.
        ///
        /// A pattern matching more values than this is left as a regular expression.
        pub regex_to_in_list_max_entries: usize, default = 1000

        /// Limit for the total size, in bytes, of the values a regular expression predicate may be
        /// rewritten into an `IN` list of.
        ///
        /// Bounds the memory a rewrite may take even where
        /// [`regex_to_in_list_max_entries`](Self::regex_to_in_list_max_entries) permits it, as a
        /// pattern such as `^[0-9][0-9](…a long literal…)$` reaches few values that are each long.
        pub regex_to_in_list_max_bytes: usize, default = 1024 * 1024

        /// Limit for the number of times a repetition in a regular expression is repeated when
        /// rewriting it into an `IN` list.
        ///
        /// Bounds the work a rewrite may take even where the other two limits permit it: the
        /// values of `^a{0,999}$` fit within both, but cost far longer to enumerate than planning
        /// a query should. A pattern repeating anything more times than this is left as a regular
        /// expression.
        pub regex_to_in_list_max_repetition: usize, default = 50

        /// Use an InfluxDB-specific parquet loader.
        ///
        /// Our custom loader currently has the following features:
        /// - avoids excessive read request for parquet files
        /// - decouples tokio IO/main-runtime from CPU-bound DataFusion runtime
        pub use_cached_parquet_loader: bool, default = true

        /// Hint known object store size from catalog to object store subsystem.
        pub hint_known_object_size_to_object_store: bool, default = true

        /// Allow User Defined Functions.
        ///
        /// These functions mean actual user-defined ones like Python-based UDFs, not what DataFusion calls "UDF"
        /// (which is basically all kinds of functions).
        pub udfs_enabled: bool, default = false

        /// HTTP/S requests permitted from UDFs.
        ///
        /// Each request is controlled by its host, port, connection mode, HTTP methods, and optional
        /// IP allow/deny subnets. Configure nested settings through the `iox.udfs_http_permissions`
        /// DataFusion namespace. For `--datafusion-config`, for example, use:
        /// `iox.udfs_http_permissions.host.[api.example.com].port.443.methods:GET|POST`.
        pub udfs_http_permissions: AllowCertainHttpRequests, default = AllowCertainHttpRequests::default()
    }
}

impl IoxConfigExt {
    /// Get the partition limit as an Option.
    pub fn partition_limit_opt(&self) -> Option<usize> {
        match self.partition_limit {
            0 => None,
            n => Some(n),
        }
    }

    /// Set the partition limit. If the limit is already set it will not be increased.
    pub fn set_partition_limit(&mut self, limit: usize) {
        match (self.partition_limit, limit) {
            (_, 0) => {}
            (0, n) => self.partition_limit = n,
            (a, b) => self.partition_limit = a.min(b),
        }
    }

    /// Get the parquet file limit as an Option.
    pub fn parquet_file_limit_opt(&self) -> Option<usize> {
        match self.parquet_file_limit {
            0 => None,
            n => Some(n),
        }
    }

    /// Set the parquet file limit. If the limit is already set it will not be increased.
    pub fn set_parquet_file_limit(&mut self, limit: usize) {
        match (self.parquet_file_limit, limit) {
            (_, 0) => {}
            (0, n) => self.parquet_file_limit = n,
            (a, b) => self.parquet_file_limit = a.min(b),
        }
    }
}

impl ConfigExtension for IoxConfigExt {
    const PREFIX: &'static str = IOX_CONFIG_PREFIX;
}

/// Optional datetime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataCutoff {
    Absolute(chrono::DateTime<chrono::Utc>),
    Relative(Duration),
}

#[derive(Debug)]
pub struct ParseError(String);

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for ParseError {}

impl FromStr for MetadataCutoff {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(s) = s.strip_prefix('-') {
            let delta = u64::from_str(s).map_err(|e| ParseError(e.to_string()))?;
            let delta = Duration::from_nanos(delta);
            Ok(Self::Relative(delta))
        } else {
            let dt = chrono::DateTime::<chrono::Utc>::from_str(s)
                .map_err(|e| ParseError(e.to_string()))?;
            Ok(Self::Absolute(dt))
        }
    }
}

impl std::fmt::Display for MetadataCutoff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Relative(delta) => write!(f, "-{}", delta.as_nanos()),
            Self::Absolute(dt) => write!(f, "{dt}"),
        }
    }
}

impl ConfigField for MetadataCutoff {
    fn visit<V: Visit>(&self, v: &mut V, key: &str, description: &'static str) {
        v.some(key, self, description)
    }

    fn set(&mut self, _key: &str, value: &str) -> Result<()> {
        *self = value.parse().map_err(|e| {
            DataFusionError::Context(
                format!("Error parsing '{value}' as ConfigField"),
                Box::new(DataFusionError::External(Box::new(e))),
            )
        })?;
        Ok(())
    }
}
