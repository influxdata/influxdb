use std::{borrow::Cow, str::FromStr, time::Duration};

use datafusion::{
    common::{Result, extensions_options},
    config::{ConfigExtension, ConfigField, Visit},
    error::DataFusionError,
};
use datafusion_udf_wasm_host::HttpRequestMatcher;

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

  /// Set of HTTP/S request allowed in UDFs (pipe-delimited list of `METHOD:HOST:PORT`)
  ///
  /// Example: Allow HTTP GET requests to influxdata.com and POST requests to api.github.com
  /// GET:influxdata.com:80|POST:api.github.com:443
        pub udfs_http_allow_list: UDFHttpAllowList, default = UDFHttpAllowList::default()
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

/// List of of allowed HTTP targets for the UDF.
///
/// Items have the format `<METHOD>:<HOST>:<PORT>` and are separated by a pipe `|`.
///
/// # Example Format
/// ```
/// # use std::{fmt::{Debug, Display}, str::FromStr};
/// # use iox_query::config::UDFHttpAllowList;
/// # use datafusion_udf_wasm_host::{HttpMethod, HttpRequestMatcher};
/// #
/// # #[track_caller]
/// # fn assert_roundtrip<T>(s: &'static str, t: T)
/// # where
/// #     T: Debug + Display + FromStr + PartialEq,
/// #     <T as FromStr>::Err: Debug,
/// # {
/// #     let t2 = T::from_str(s).unwrap();
/// #     assert_eq!(t, t2);
/// #
/// #     let s2 = t.to_string();
/// #     assert_eq!(s, s2);
/// # }
///
/// // An empty string means "no items".
/// assert_roundtrip(
///     "",
///     UDFHttpAllowList(vec![]),
/// );
///
/// assert_roundtrip(
///     "GET:influxdata.com:80",
///     UDFHttpAllowList(vec![
///         HttpRequestMatcher {
///             method: HttpMethod::GET,
///             host: "influxdata.com".into(),
///             port: 80,
///         },
///     ]),
/// );
///
/// // Multiple items are split by `|`.
/// // This is done because we pass the different DataFusion config
/// // options as one environment variable as `k1:v2,k2:v2,...` and
/// // hence `,` is already taken (`:` can be used though since only
/// // the first occurance in a key-value pair is used for splitting).
/// assert_roundtrip(
///     "GET:influxdata.com:80|POST:foo.com:443",
///     UDFHttpAllowList(vec![
///         HttpRequestMatcher {
///             method: HttpMethod::GET,
///             host: "influxdata.com".into(),
///             port: 80,
///         },
///         HttpRequestMatcher {
///             method: HttpMethod::POST,
///             host: "foo.com".into(),
///             port: 443,
///         },
///     ]),
/// );
/// ```
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct UDFHttpAllowList(pub Vec<HttpRequestMatcher>);

impl UDFHttpAllowList {
    const ITEM_SEP: &str = "|";
    const PART_SEP: &str = ":";
}

impl FromStr for UDFHttpAllowList {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let l = s
            .split(Self::ITEM_SEP)
            .filter(|s| !s.is_empty())
            .map(|s| {
                let parts = s.split(Self::PART_SEP).collect::<Vec<_>>();
                let [method, host, port] = parts.as_slice() else {
                    return Err(ParseError(format!(
                        "HTTP allow-list items must have 3 parts, separated by `{}`, but got `{s}`",
                        Self::PART_SEP,
                    )));
                };
                Ok(HttpRequestMatcher {
                    method: method
                        .parse()
                        .map_err(|e| ParseError(format!("cannot parse HTTP method: {e}")))?,
                    host: Cow::Owned((*host).to_owned()),
                    port: port
                        .parse()
                        .map_err(|e| ParseError(format!("cannot parse HTTP port: {e}")))?,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self(l))
    }
}

impl std::fmt::Display for UDFHttpAllowList {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (i, item) in self.0.iter().enumerate() {
            if i > 0 {
                write!(f, "{}", Self::ITEM_SEP)?;
            }
            let HttpRequestMatcher { method, host, port } = item;
            write!(
                f,
                "{method}{}{host}{}{port}",
                Self::PART_SEP,
                Self::PART_SEP,
            )?;
        }
        Ok(())
    }
}

impl ConfigField for UDFHttpAllowList {
    fn visit<V: Visit>(&self, v: &mut V, key: &str, description: &'static str) {
        v.some(key, self, description)
    }

    fn set(&mut self, _key: &str, value: &str) -> Result<()> {
        *self = value.parse().map_err(|e| {
            DataFusionError::Context(
                format!("Error parsing '{value}' as UDFHttpAllowList",),
                Box::new(DataFusionError::External(Box::new(e))),
            )
        })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use test_helpers::assert_contains;

    #[test]
    fn udf_http_allow_list_requires_three_parts() {
        let err = UDFHttpAllowList::from_str("GET:example.com").unwrap_err();
        assert_contains!(err.to_string(), "HTTP allow-list items must have 3 parts");
    }

    #[test]
    fn udf_http_allow_list_rejects_invalid_port() {
        let err = UDFHttpAllowList::from_str("GET:example.com:not_a_port").unwrap_err();
        assert_contains!(err.to_string(), "cannot parse HTTP port");
    }

    #[test]
    fn udf_http_allow_list_rejects_invalid_method() {
        let err = UDFHttpAllowList::from_str(":example.com:1234").unwrap_err();
        assert_contains!(err.to_string(), "cannot parse HTTP method");
    }
}
