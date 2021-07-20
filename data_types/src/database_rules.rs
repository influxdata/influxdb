use std::num::NonZeroU64;
use std::time::Duration;
use std::{
    collections::HashMap,
    hash::{Hash, Hasher},
    num::{NonZeroU32, NonZeroUsize},
    sync::Arc,
};

use chrono::{TimeZone, Utc};
use regex::Regex;
use snafu::{OptionExt, Snafu};

use influxdb_line_protocol::ParsedLine;

use crate::{consistent_hasher::ConsistentHasher, server_id::ServerId, DatabaseName};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error in {}: {}", source_module, source))]
    PassThrough {
        source_module: &'static str,
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    #[snafu(display("No sharding rule matches line: {}", line))]
    NoShardingRuleMatches { line: String },

    #[snafu(display("No shards defined"))]
    NoShardsDefined,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// DatabaseRules contains the rules for replicating data, sending data to
/// subscribers, and querying data for a single database.
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct DatabaseRules {
    /// The name of the database
    pub name: DatabaseName<'static>,

    /// Template that generates a partition key for each row inserted into the
    /// db
    pub partition_template: PartitionTemplate,

    /// Configure how data flows through the system
    pub lifecycle_rules: LifecycleRules,

    /// An optional config to delegate data plane operations to one or more
    /// remote servers.
    pub routing_rules: Option<RoutingRules>,

    /// Duration for which the cleanup loop should sleep on average.
    /// Defaults to 500 seconds.
    pub worker_cleanup_avg_sleep: Duration,

    /// An optional connection string to a write buffer for either writing or reading.
    pub write_buffer_connection: Option<WriteBufferConnection>,
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum WriteBufferConnection {
    Writing(String),
    Reading(String),
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum RoutingRules {
    // A routing config defines the target where all data plane operations for
    // a given database are delegated to.
    RoutingConfig(RoutingConfig),

    /// A sharding config split writes into different "shards". A shard
    /// is a logical concept, but the usage is meant to split data into
    /// mutually exclusive areas. The rough order of organization is:
    /// database -> shard -> partition -> chunk. For example, you could shard
    /// based on table name and assign to 1 of 10 shards. Within each
    /// shard you would have partitions, which would likely be based off time.
    /// This makes it possible to horizontally scale out writes.
    ShardConfig(ShardConfig),
}

impl DatabaseRules {
    pub fn partition_key(&self, line: &ParsedLine<'_>, default_time: i64) -> Result<String> {
        self.partition_template.partition_key(line, default_time)
    }

    pub fn new(name: DatabaseName<'static>) -> Self {
        Self {
            name,
            partition_template: Default::default(),
            lifecycle_rules: Default::default(),
            routing_rules: None,
            worker_cleanup_avg_sleep: Duration::from_secs(500),
            write_buffer_connection: None,
        }
    }

    pub fn db_name(&self) -> &str {
        &self.name.as_str()
    }
}

/// Generates a partition key based on the line and the default time.
pub trait Partitioner {
    fn partition_key(&self, _line: &ParsedLine<'_>, _default_time: i64) -> Result<String>;
}

impl Partitioner for DatabaseRules {
    fn partition_key(&self, line: &ParsedLine<'_>, default_time: i64) -> Result<String> {
        self.partition_key(&line, default_time)
    }
}

pub const DEFAULT_WORKER_BACKOFF_MILLIS: u64 = 1_000;
pub const DEFAULT_CATALOG_TRANSACTIONS_UNTIL_CHECKPOINT: u64 = 100;
pub const DEFAULT_MUB_ROW_THRESHOLD: usize = 100_000;
pub const DEFAULT_PERSIST_ROW_THRESHOLD: usize = 1_000_000;
pub const DEFAULT_PERSIST_AGE_THRESHOLD_SECONDS: u32 = 30 * 60;
pub const DEFAULT_LATE_ARRIVE_WINDOW_SECONDS: u32 = 5 * 60;

/// Configures how data automatically flows through the system
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct LifecycleRules {
    /// Once the total amount of buffered data in memory reaches this size start
    /// dropping data from memory
    pub buffer_size_soft: Option<NonZeroUsize>,

    /// Once the amount of data in memory reaches this size start
    /// rejecting writes
    pub buffer_size_hard: Option<NonZeroUsize>,

    /// Allow dropping data that has not been persisted to object storage
    /// once the database size has exceeded the configured limits
    pub drop_non_persisted: bool,

    /// Persists chunks to object storage.
    pub persist: bool,

    /// Do not allow writing new data to this database
    pub immutable: bool,

    /// If the background worker doesn't find anything to do it
    /// will sleep for this many milliseconds before looking again
    pub worker_backoff_millis: NonZeroU64,

    /// The maximum number of permitted concurrently executing compactions.
    /// It is not currently possible to set a limit that disables compactions
    /// entirely, nor is it possible to set an "unlimited" value.
    pub max_active_compactions: NonZeroU32,

    /// After how many transactions should IOx write a new checkpoint?
    pub catalog_transactions_until_checkpoint: NonZeroU64,

    /// Once a partition hasn't received a write for this period of time,
    /// it will be compacted and, if set, persisted. Writers will generally
    /// have this amount of time to send late arriving writes or this could
    /// be their clock skew.
    pub late_arrive_window_seconds: NonZeroU32,

    /// Maximum number of rows before triggering persistence
    pub persist_row_threshold: NonZeroUsize,

    /// Maximum age of a write before triggering persistence
    pub persist_age_threshold_seconds: NonZeroU32,

    /// Maximum number of rows to buffer in a MUB chunk before compacting it
    pub mub_row_threshold: NonZeroUsize,

    /// Use up to this amount of space in bytes for caching Parquet files. None
    /// will disable Parquet file caching.
    pub parquet_cache_limit: Option<NonZeroU64>,
}

impl LifecycleRules {
    /// The max timestamp skew across concurrent writers before persisted chunks might overlap
    pub fn late_arrive_window(&self) -> Duration {
        Duration::from_secs(self.late_arrive_window_seconds.get() as u64)
    }
}

impl Default for LifecycleRules {
    fn default() -> Self {
        Self {
            buffer_size_soft: None,
            buffer_size_hard: None,
            drop_non_persisted: false,
            persist: false,
            immutable: false,
            worker_backoff_millis: NonZeroU64::new(DEFAULT_WORKER_BACKOFF_MILLIS).unwrap(),
            max_active_compactions: NonZeroU32::new(num_cpus::get() as u32).unwrap(), // defaults to number of CPU threads
            catalog_transactions_until_checkpoint: NonZeroU64::new(
                DEFAULT_CATALOG_TRANSACTIONS_UNTIL_CHECKPOINT,
            )
            .unwrap(),
            late_arrive_window_seconds: NonZeroU32::new(DEFAULT_LATE_ARRIVE_WINDOW_SECONDS)
                .unwrap(),
            persist_row_threshold: NonZeroUsize::new(DEFAULT_PERSIST_ROW_THRESHOLD).unwrap(),
            persist_age_threshold_seconds: NonZeroU32::new(DEFAULT_PERSIST_AGE_THRESHOLD_SECONDS)
                .unwrap(),
            mub_row_threshold: NonZeroUsize::new(DEFAULT_MUB_ROW_THRESHOLD).unwrap(),
            parquet_cache_limit: None,
        }
    }
}

/// `PartitionTemplate` is used to compute the partition key of each row that
/// gets written. It can consist of the table name, a column name and its value,
/// a formatted time, or a string column and regex captures of its value. For
/// columns that do not appear in the input row, a blank value is output.
///
/// The key is constructed in order of the template parts; thus ordering changes
/// what partition key is generated.
#[derive(Debug, Default, Eq, PartialEq, Clone)]
pub struct PartitionTemplate {
    pub parts: Vec<TemplatePart>,
}

impl Partitioner for PartitionTemplate {
    fn partition_key(&self, line: &ParsedLine<'_>, default_time: i64) -> Result<String> {
        let parts: Vec<_> = self
            .parts
            .iter()
            .map(|p| match p {
                TemplatePart::Table => line.series.measurement.to_string(),
                TemplatePart::Column(column) => match line.tag_value(&column) {
                    Some(v) => format!("{}_{}", column, v),
                    None => match line.field_value(&column) {
                        Some(v) => format!("{}_{}", column, v),
                        None => "".to_string(),
                    },
                },
                TemplatePart::TimeFormat(format) => {
                    let nanos = line.timestamp.unwrap_or(default_time);
                    Utc.timestamp_nanos(nanos).format(&format).to_string()
                }
                _ => unimplemented!(),
            })
            .collect();

        Ok(parts.join("-"))
    }
}

/// `TemplatePart` specifies what part of a row should be used to compute this
/// part of a partition key.
#[derive(Debug, Eq, PartialEq, Clone)]
pub enum TemplatePart {
    /// The name of a table
    Table,
    /// The value in a named column
    Column(String),
    /// Applies a  `strftime` format to the "time" column.
    ///
    /// For example, a time format of "%Y-%m-%d %H:%M:%S" will produce
    /// partition key parts such as "2021-03-14 12:25:21" and
    /// "2021-04-14 12:24:21"
    TimeFormat(String),
    /// Applies a regex to the value in a string column
    RegexCapture(RegexCapture),
    /// Applies a `strftime` pattern to some column other than "time"
    StrftimeColumn(StrftimeColumn),
}

/// `RegexCapture` is for pulling parts of a string column into the partition
/// key.
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct RegexCapture {
    pub column: String,
    pub regex: String,
}

/// [`StrftimeColumn`] is used to create a time based partition key off some
/// column other than the builtin `time` column.
///
/// The value of the named column is formatted using a `strftime`
/// style string.
///
/// For example, a time format of "%Y-%m-%d %H:%M:%S" will produce
/// partition key parts such as "2021-03-14 12:25:21" and
/// "2021-04-14 12:24:21"
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct StrftimeColumn {
    pub column: String,
    pub format: String,
}

/// A routing config defines the destination where to route all data plane operations
/// for a given database.
#[derive(Debug, Eq, PartialEq, Clone, Default)]
pub struct RoutingConfig {
    pub target: NodeGroup,
}

/// ShardId maps to a nodegroup that holds the the shard.
pub type ShardId = u32;
pub const NO_SHARD_CONFIG: Option<&ShardConfig> = None;

/// Assigns a given line to a specific shard id.
pub trait Sharder {
    fn shard(&self, line: &ParsedLine<'_>) -> Result<ShardId>;
}

/// ShardConfig defines rules for assigning a line/row to an individual
/// host or a group of hosts. A shard
/// is a logical concept, but the usage is meant to split data into
/// mutually exclusive areas. The rough order of organization is:
/// database -> shard -> partition -> chunk. For example, you could shard
/// based on table name and assign to 1 of 10 shards. Within each
/// shard you would have partitions, which would likely be based off time.
/// This makes it possible to horizontally scale out writes.
#[derive(Debug, Eq, PartialEq, Clone, Default)]
pub struct ShardConfig {
    /// Each matcher, if any, is evaluated in order.
    /// If there is a match, the route will be evaluated to
    /// the given targets, otherwise the hash ring will be evaluated.
    /// This is useful for overriding the hashring function on some hot spot. For
    /// example, if you use the table name as the input to the hash function
    /// and your ring has 4 slots. If two tables that are very hot get
    /// assigned to the same slot you can override that by putting in a
    /// specific matcher to pull that table over to a different node.
    pub specific_targets: Vec<MatcherToShard>,
    /// An optional default hasher which will route to one in a collection of
    /// nodes.
    pub hash_ring: Option<HashRing>,
    /// If set to true the router will ignore any errors sent by the remote
    /// targets in this route. That is, the write request will succeed
    /// regardless of this route's success.
    pub ignore_errors: bool,
    /// Mapping between shard IDs and node groups. Other sharding rules use
    /// ShardId as targets.
    pub shards: Arc<HashMap<ShardId, Shard>>,
}

/// Configuration for a specific IOx shard
#[derive(Debug, Eq, PartialEq, Clone)]
pub enum Shard {
    Iox(NodeGroup),
}

struct LineHasher<'a, 'b, 'c> {
    line: &'a ParsedLine<'c>,
    hash_ring: &'b HashRing,
}

impl<'a, 'b, 'c> Hash for LineHasher<'a, 'b, 'c> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        if self.hash_ring.table_name {
            self.line.series.measurement.hash(state);
        }
        for column in &self.hash_ring.columns {
            if let Some(tag_value) = self.line.tag_value(column) {
                tag_value.hash(state);
            } else if let Some(field_value) = self.line.field_value(column) {
                field_value.to_string().hash(state);
            }
            state.write_u8(0); // column separator
        }
    }
}

impl Sharder for ShardConfig {
    fn shard(&self, line: &ParsedLine<'_>) -> Result<ShardId, Error> {
        for i in &self.specific_targets {
            if i.matcher.match_line(line) {
                return Ok(i.shard);
            }
        }
        if let Some(hash_ring) = &self.hash_ring {
            return hash_ring
                .shards
                .find(LineHasher { line, hash_ring })
                .context(NoShardsDefined);
        }

        NoShardingRuleMatches {
            line: line.to_string(),
        }
        .fail()
    }
}

/// Maps a matcher with specific shard. If the line/row matches
/// it should be sent to the group.
#[derive(Debug, Eq, PartialEq, Clone, Default)]
pub struct MatcherToShard {
    pub matcher: Matcher,
    pub shard: ShardId,
}

/// A collection of IOx nodes
pub type NodeGroup = Vec<ServerId>;

/// HashRing is a rule for creating a hash key for a row and mapping that to
/// an individual node on a ring.
#[derive(Debug, Eq, PartialEq, Clone, Default)]
pub struct HashRing {
    /// If true the table name will be included in the hash key
    pub table_name: bool,
    /// include the values of these columns in the hash key
    pub columns: Vec<String>,
    /// ring of shard ids
    pub shards: ConsistentHasher<ShardId>,
}

/// A matcher is used to match routing rules or subscriptions on a row-by-row
/// (or line) basis.
#[derive(Debug, Clone, Default)]
pub struct Matcher {
    /// if provided, match if the table name matches against the regex
    pub table_name_regex: Option<Regex>,
    // paul: what should we use for predicate matching here against a single row/line?
    pub predicate: Option<String>,
}

impl PartialEq for Matcher {
    fn eq(&self, other: &Self) -> bool {
        // this is kind of janky, but it's only used during tests and should get the job
        // done
        format!("{:?}{:?}", self.table_name_regex, self.predicate)
            == format!("{:?}{:?}", other.table_name_regex, other.predicate)
    }
}
impl Eq for Matcher {}

impl Matcher {
    fn match_line(&self, line: &ParsedLine<'_>) -> bool {
        let table_name_matches = if let Some(table_name_regex) = &self.table_name_regex {
            table_name_regex.is_match(line.series.measurement.as_str())
        } else {
            false
        };

        let predicate_matches = if self.predicate.is_some() {
            unimplemented!("predicates not implemented yet")
        } else {
            false
        };

        table_name_matches || predicate_matches
    }
}

/// `PartitionId` is the object storage identifier for a specific partition. It
/// should be a path that can be used against an object store to locate all the
/// files and subdirectories for a partition. It takes the form of `/<writer
/// ID>/<database>/<partition key>/`.
pub type PartitionId = String;

#[cfg(test)]
mod tests {
    use influxdb_line_protocol::parse_lines;

    use super::*;

    const ARBITRARY_DEFAULT_TIME: i64 = 456;

    #[test]
    fn partition_key_with_table() {
        let template = PartitionTemplate {
            parts: vec![TemplatePart::Table],
        };

        let line = parse_line("cpu foo=1 10");
        assert_eq!(
            "cpu",
            template
                .partition_key(&line, ARBITRARY_DEFAULT_TIME)
                .unwrap()
        );
    }

    #[test]
    fn partition_key_with_int_field() {
        let template = PartitionTemplate {
            parts: vec![TemplatePart::Column("foo".to_string())],
        };

        let line = parse_line("cpu foo=1 10");
        assert_eq!(
            "foo_1",
            template
                .partition_key(&line, ARBITRARY_DEFAULT_TIME)
                .unwrap()
        );
    }

    #[test]
    fn partition_key_with_float_field() {
        let template = PartitionTemplate {
            parts: vec![TemplatePart::Column("foo".to_string())],
        };

        let line = parse_line("cpu foo=1.1 10");
        assert_eq!(
            "foo_1.1",
            template
                .partition_key(&line, ARBITRARY_DEFAULT_TIME)
                .unwrap()
        );
    }

    #[test]
    fn partition_key_with_string_field() {
        let template = PartitionTemplate {
            parts: vec![TemplatePart::Column("foo".to_string())],
        };

        let line = parse_line("cpu foo=\"asdf\" 10");
        assert_eq!(
            "foo_asdf",
            template
                .partition_key(&line, ARBITRARY_DEFAULT_TIME)
                .unwrap()
        );
    }

    #[test]
    fn partition_key_with_bool_field() {
        let template = PartitionTemplate {
            parts: vec![TemplatePart::Column("bar".to_string())],
        };

        let line = parse_line("cpu bar=true 10");
        assert_eq!(
            "bar_true",
            template
                .partition_key(&line, ARBITRARY_DEFAULT_TIME)
                .unwrap()
        );
    }

    #[test]
    fn partition_key_with_tag_column() {
        let template = PartitionTemplate {
            parts: vec![TemplatePart::Column("region".to_string())],
        };

        let line = parse_line("cpu,region=west usage_user=23.2 10");
        assert_eq!(
            "region_west",
            template
                .partition_key(&line, ARBITRARY_DEFAULT_TIME)
                .unwrap()
        );
    }

    #[test]
    fn partition_key_with_missing_column() {
        let template = PartitionTemplate {
            parts: vec![TemplatePart::Column("not_here".to_string())],
        };

        let line = parse_line("cpu,foo=asdf bar=true 10");
        assert_eq!(
            "",
            template
                .partition_key(&line, ARBITRARY_DEFAULT_TIME)
                .unwrap()
        );
    }

    #[test]
    fn partition_key_with_time() {
        let template = PartitionTemplate {
            parts: vec![TemplatePart::TimeFormat("%Y-%m-%d %H:%M:%S".to_string())],
        };

        let line = parse_line("cpu,foo=asdf bar=true 1602338097000000000");
        assert_eq!(
            "2020-10-10 13:54:57",
            template
                .partition_key(&line, ARBITRARY_DEFAULT_TIME)
                .unwrap()
        );
    }

    #[test]
    fn partition_key_with_default_time() {
        let format_string = "%Y-%m-%d %H:%M:%S";
        let template = PartitionTemplate {
            parts: vec![TemplatePart::TimeFormat(format_string.to_string())],
        };

        let default_time = Utc::now();
        let line = parse_line("cpu,foo=asdf bar=true");
        assert_eq!(
            default_time.format(format_string).to_string(),
            template
                .partition_key(&line, default_time.timestamp_nanos())
                .unwrap()
        );
    }

    #[test]
    fn partition_key_with_many_parts() {
        let template = PartitionTemplate {
            parts: vec![
                TemplatePart::Table,
                TemplatePart::Column("region".to_string()),
                TemplatePart::Column("usage_system".to_string()),
                TemplatePart::TimeFormat("%Y-%m-%d %H:%M:%S".to_string()),
            ],
        };

        let line = parse_line(
            "cpu,host=a,region=west usage_user=22.1,usage_system=53.1 1602338097000000000",
        );
        assert_eq!(
            "cpu-region_west-usage_system_53.1-2020-10-10 13:54:57",
            template
                .partition_key(&line, ARBITRARY_DEFAULT_TIME)
                .unwrap()
        );
    }

    #[test]
    #[allow(clippy::trivial_regex)]
    fn test_sharder() {
        let shards: Vec<_> = (1000..1000000).collect();
        let shard_config = ShardConfig {
            specific_targets: vec![MatcherToShard {
                matcher: Matcher {
                    table_name_regex: Some(Regex::new("pu$").unwrap()),
                    predicate: None,
                },
                shard: 1,
            }],
            hash_ring: Some(HashRing {
                table_name: true,
                columns: vec!["t1", "t2", "f1", "f2"]
                    .into_iter()
                    .map(|i| i.to_string())
                    .collect(),
                shards: ConsistentHasher::new(&shards),
            }),
            ..Default::default()
        };

        // hit the specific targets

        let line = parse_line("cpu,t1=1,t2=2,t3=3 f1=1,f2=2,f3=3 10");
        let sharded_line = shard_config.shard(&line).expect("cannot shard a line");
        assert_eq!(sharded_line, 1);

        let line = parse_line("cpu,t1=10,t2=20,t3=30 f1=10,f2=20,f3=30 10");
        let sharded_line = shard_config.shard(&line).expect("cannot shard a line");
        assert_eq!(sharded_line, 1);

        // hit the hash ring

        let line = parse_line("mem,t1=1,t2=2,t3=3 f1=1,f2=2,f3=3 10");
        let sharded_line = shard_config.shard(&line).expect("cannot shard a line");
        assert_eq!(sharded_line, 710570);

        // change a column that is not part of the hashring columns
        let line = parse_line("mem,t1=1,t2=2,t3=30 f1=1,f2=2,f3=3 10");
        let sharded_line = shard_config.shard(&line).expect("cannot shard a line");
        assert_eq!(sharded_line, 710570);

        // change a column that is part of the hashring
        let line = parse_line("mem,t1=10,t2=2,t3=3 f1=1,f2=2,f3=3 10");
        let sharded_line = shard_config.shard(&line).expect("cannot shard a line");
        assert_eq!(sharded_line, 342220);

        // ensure columns can be optional and yet cannot be mixed up
        let line = parse_line("mem,t1=10,t3=3 f1=1,f2=2,f3=3 10");
        let sharded_line = shard_config.shard(&line).expect("cannot shard a line");
        assert_eq!(sharded_line, 494892);
        let line = parse_line("mem,t2=10,t3=3 f1=1,f2=2,f3=3 10");
        let sharded_line = shard_config.shard(&line).expect("cannot shard a line");
        assert_eq!(sharded_line, 32813);

        // same thing for "fields" columns:

        // change a column that is not part of the hashring columns
        let line = parse_line("mem,t1=1,t2=2,t3=3 f1=1,f2=2,f3=30 10");
        let sharded_line = shard_config.shard(&line).expect("cannot shard a line");
        assert_eq!(sharded_line, 710570);

        // change a column that is part of the hashring
        let line = parse_line("mem,t1=10,t2=2,t3=3 f1=1,f2=2,f3=3 10");
        let sharded_line = shard_config.shard(&line).expect("cannot shard a line");
        assert_eq!(sharded_line, 342220);

        // ensure columns can be optional and yet cannot be mixed up
        let line = parse_line("mem,t1=1,t3=3 f1=10,f2=2,f3=3 10");
        let sharded_line = shard_config.shard(&line).expect("cannot shard a line");
        assert_eq!(sharded_line, 49366);
        let line = parse_line("mem,t2=1,t3=3 f1=10,f2=2,f3=3 10");
        let sharded_line = shard_config.shard(&line).expect("cannot shard a line");
        assert_eq!(sharded_line, 637504);
    }

    #[test]
    fn test_sharder_no_shards() {
        let shard_config = ShardConfig {
            hash_ring: Some(HashRing {
                table_name: true,
                columns: vec!["t1", "t2", "f1", "f2"]
                    .into_iter()
                    .map(|i| i.to_string())
                    .collect(),
                shards: ConsistentHasher::new(&[]),
            }),
            ..Default::default()
        };

        let line = parse_line("cpu,t1=1,t2=2,t3=3 f1=1,f2=2,f3=3 10");
        let err = shard_config.shard(&line).unwrap_err();

        assert!(matches!(err, Error::NoShardsDefined));
    }

    fn parsed_lines(lp: &str) -> Vec<ParsedLine<'_>> {
        parse_lines(lp).map(|l| l.unwrap()).collect()
    }

    fn parse_line(line: &str) -> ParsedLine<'_> {
        parsed_lines(line).pop().unwrap()
    }
}
