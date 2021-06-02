use crate::{consistent_hasher::ConsistentHasher, server_id::ServerId, DatabaseName};
use chrono::{TimeZone, Utc};
use influxdb_line_protocol::ParsedLine;
use regex::Regex;
use snafu::{OptionExt, Snafu};
use std::num::NonZeroU64;
use std::time::Duration;
use std::{
    collections::HashMap,
    hash::{Hash, Hasher},
    num::{NonZeroU32, NonZeroUsize},
    sync::Arc,
};

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

    /// When set, this will buffer writes in memory based on the
    /// configuration.
    pub write_buffer_config: Option<WriteBufferConfig>,

    /// Configure how data flows through the system
    pub lifecycle_rules: LifecycleRules,

    /// An optional config to delegate data plane operations to one or more
    /// remote servers.
    pub routing_rules: Option<RoutingRules>,

    /// Duration for which the cleanup loop should sleep on avarage.
    pub worker_cleanup_avg_sleep: Duration,
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
            write_buffer_config: None,
            lifecycle_rules: Default::default(),
            routing_rules: None,
            worker_cleanup_avg_sleep: Duration::from_secs(500),
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

/// Configures how data automatically flows through the system
#[derive(Debug, Default, Eq, PartialEq, Clone)]
pub struct LifecycleRules {
    /// A chunk of data within a partition that has been cold for writes for
    /// this many seconds will be frozen and compacted (moved to the read
    /// buffer) if the chunk is older than mutable_min_lifetime_seconds
    ///
    /// Represents the chunk transition open -> moving and closed -> moving
    pub mutable_linger_seconds: Option<NonZeroU32>,

    /// A chunk of data within a partition is guaranteed to remain mutable
    /// for at least this number of seconds unless it exceeds the mutable_size_threshold
    pub mutable_minimum_age_seconds: Option<NonZeroU32>,

    /// Once a chunk of data within a partition reaches this number of bytes
    /// writes outside its keyspace will be directed to a new chunk and this
    /// chunk will be compacted to the read buffer as soon as possible
    pub mutable_size_threshold: Option<NonZeroUsize>,

    /// Once the total amount of buffered data in memory reaches this size start
    /// dropping data from memory based on the [`sort_order`](Self::sort_order)
    pub buffer_size_soft: Option<NonZeroUsize>,

    /// Once the amount of data in memory reaches this size start
    /// rejecting writes
    ///
    /// TODO: Implement this limit
    pub buffer_size_hard: Option<NonZeroUsize>,

    /// Configure order to transition data
    ///
    /// In the case of multiple candidates, data will be
    /// compacted, persisted and dropped in this order
    pub sort_order: SortOrder,

    /// Allow dropping data that has not been persisted to object storage
    /// once the database size has exceeded the configured limits
    pub drop_non_persisted: bool,

    /// Persists chunks to object storage.
    pub persist: bool,

    /// Do not allow writing new data to this database
    pub immutable: bool,

    /// If the background worker doesn't find anything to do it
    /// will sleep for this many milliseconds before looking again
    pub worker_backoff_millis: Option<NonZeroU64>,
}

/// This struct specifies the rules for the order to sort partitions
/// from the mutable buffer. This is used to determine which order to drop them
/// in. The last partition in the list will be dropped, until enough space has
/// been freed up to be below the max size.
///
/// For example, to drop the partition that has been open longest:
/// ```
/// use data_types::database_rules::{SortOrder, Order, Sort};
///
/// let rules = SortOrder{
///     order: Order::Desc,
///     sort: Sort::CreatedAtTime,
/// };
/// ```
#[derive(Debug, Default, Eq, PartialEq, Clone)]
pub struct SortOrder {
    /// Sort partitions by this order. Last will be dropped.
    pub order: Order,
    /// Sort by either a column value, or when the partition was opened, or when
    /// it last received a write.
    pub sort: Sort,
}

/// What to sort the partition by.
#[derive(Debug, Eq, PartialEq, Clone)]
pub enum Sort {
    /// The last time the partition received a write.
    LastWriteTime,
    /// When the partition was opened in the mutable buffer.
    CreatedAtTime,
    /// A column name, its expected type, and whether to use the min or max
    /// value. The ColumnType is necessary because the column can appear in
    /// any number of tables and be of a different type. This specifies that
    /// when sorting partitions, only columns with the given name and type
    /// should be used for the purposes of determining the partition order. If a
    /// partition doesn't have the given column in any way, the partition will
    /// appear at the beginning of the list with a null value where all
    /// partitions having null for that value will then be
    /// sorted by created_at_time desc. So if none of the partitions in the
    /// mutable buffer had this column with this type, then the partition
    /// that was created first would appear last in the list and thus be the
    /// first up to be dropped.
    Column(String, ColumnType, ColumnValue),
}

impl Default for Sort {
    fn default() -> Self {
        Self::CreatedAtTime
    }
}

/// The sort order.
#[derive(Debug, Eq, PartialEq, Clone)]
pub enum Order {
    Asc,
    Desc,
}

impl Default for Order {
    fn default() -> Self {
        Self::Asc
    }
}

/// Use columns of this type.
#[derive(Debug, Eq, PartialEq, Clone)]
pub enum ColumnType {
    I64,
    U64,
    F64,
    String,
    Bool,
}

/// Use either the min or max summary statistic.
#[derive(Debug, Eq, PartialEq, Clone)]
pub enum ColumnValue {
    Min,
    Max,
}

/// `WriteBufferConfig` defines the configuration for buffering data from writes
/// in memory. This buffer is used for asynchronous replication and to collect
/// segments before sending them to object storage.
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct WriteBufferConfig {
    /// The size the Write Buffer should be limited to. Once the buffer gets to
    /// this size, it will drop old segments to remain below this size, but
    /// still try to hold as much in memory as possible while remaining
    /// below this threshold
    pub buffer_size: usize,
    /// Write Buffer segments become read-only after crossing over this size,
    /// which means that segments will always be <= this size. When old segments
    /// are dropped from of memory, at least this much space will be freed from
    /// the buffer.
    pub segment_size: usize,
    /// What should happen if a write comes in that would exceed the Write
    /// Buffer size and the oldest segment that could be dropped hasn't yet been
    /// persisted to object storage. If the oldest segment has been persisted,
    /// then it will be dropped from the buffer so that new writes can be
    /// accepted. This option is only for defining the behaviour of what happens
    /// if that segment hasn't been persisted. If set to return an error, new
    /// writes will be rejected until the oldest segment has been persisted so
    /// that it can be cleared from memory. Alternatively, this can be set so
    /// that old segments are dropped even if they haven't been persisted. This
    /// setting is also useful for cases where persistence isn't being used and
    /// this is only for in-memory buffering.
    pub buffer_rollover: WriteBufferRollover,
    /// If set to true, buffer segments will be written to object storage.
    pub store_segments: bool,
    /// If set, segments will be rolled over after this period of time even
    /// if they haven't hit the size threshold. This allows them to be written
    /// out to object storage as they must be immutable first.
    pub close_segment_after: Option<std::time::Duration>,
}

/// `WriteBufferRollover` defines the behavior of what should happen if a write
/// comes in that would cause the buffer to exceed its max size AND the oldest
/// segment can't be dropped because it has not yet been persisted.
#[derive(Debug, Clone, Eq, PartialEq, Copy)]
pub enum WriteBufferRollover {
    /// Drop the old segment even though it hasn't been persisted. This part of
    /// the Write Buffer will be lost on this server.
    DropOldSegment,
    /// Drop the incoming write and fail silently. This favors making sure that
    /// older Write Buffer data will be backed up.
    DropIncoming,
    /// Reject the incoming write and return an error. The client may retry the
    /// request, which will succeed once the oldest segment has been
    /// persisted to object storage.
    ReturnError,
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
    use super::*;
    use influxdb_line_protocol::parse_lines;

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
