use influxdb_line_protocol::ParsedLine;

use chrono::{DateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error in {}: {}", source_module, source))]
    PassThrough {
        source_module: &'static str,
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// DatabaseRules contains the rules for replicating data, sending data to
/// subscribers, and querying data for a single database.
#[derive(Debug, Serialize, Deserialize, Default, Eq, PartialEq, Clone)]
pub struct DatabaseRules {
    /// The unencoded name of the database. This gets put in by the create
    /// database call, so an empty default is fine.
    #[serde(default)]
    pub name: String,
    /// Template that generates a partition key for each row inserted into the
    /// db
    #[serde(default)]
    pub partition_template: PartitionTemplate,
    /// The set of host groups that data should be replicated to. Which host a
    /// write goes to within a host group is determined by consistent hashing of
    /// the partition key. We'd use this to create a host group per
    /// availability zone, so you might have 5 availability zones with 2
    /// hosts in each. Replication will ensure that N of those zones get a
    /// write. For each zone, only a single host needs to get the write.
    /// Replication is for ensuring a write exists across multiple hosts
    /// before returning success. Its purpose is to ensure write durability,
    /// rather than write availability for query (this is covered by
    /// subscriptions).
    #[serde(default)]
    pub replication: Vec<HostGroupId>,
    /// The minimum number of host groups to replicate a write to before success
    /// is returned. This can be overridden on a per request basis.
    /// Replication will continue to write to the other host groups in the
    /// background.
    #[serde(default)]
    pub replication_count: u8,
    /// How long the replication queue can get before either rejecting writes or
    /// dropping missed writes. The queue is kept in memory on a
    /// per-database basis. A queue size of zero means it will only try to
    /// replicate synchronously and drop any failures.
    #[serde(default)]
    pub replication_queue_max_size: usize,
    /// `subscriptions` are used for query servers to get data via either push
    /// or pull as it arrives. They are separate from replication as they
    /// have a different purpose. They're for query servers or other clients
    /// that want to subscribe to some subset of data being written in. This
    /// could either be specific partitions, ranges of partitions, tables, or
    /// rows matching some predicate. This is step #3 from the diagram.
    #[serde(default)]
    pub subscriptions: Vec<Subscription>,

    /// If set to `true`, this server should answer queries from one or more of
    /// of its local write buffer and any read-only partitions that it knows
    /// about. In this case, results will be merged with any others from the
    /// remote goups or read only partitions.
    #[serde(default)]
    pub query_local: bool,
    /// Set `primary_query_group` to a host group if remote servers should be
    /// issued queries for this database. All hosts in the group should be
    /// queried with this server acting as the coordinator that merges
    /// results together. If a specific host in the group is unavailable,
    /// another host in the same position from a secondary group should be
    /// queried. For example, imagine we've partitioned the data in this DB into
    /// 4 partitions and we are replicating the data across 3 availability
    /// zones. We have 4 hosts in each of those AZs, thus they each have 1
    /// partition. We'd set the primary group to be the 4 hosts in the same
    /// AZ as this one, and the secondary groups as the hosts in the other 2
    /// AZs.
    #[serde(default)]
    pub primary_query_group: Option<HostGroupId>,
    #[serde(default)]
    pub secondary_query_groups: Vec<HostGroupId>,

    /// Use `read_only_partitions` when a server should answer queries for
    /// partitions that come from object storage. This can be used to start
    /// up a new query server to handle queries by pointing it at a
    /// collection of partitions and then telling it to also pull
    /// data from the replication servers (writes that haven't been snapshotted
    /// into a partition).
    #[serde(default)]
    pub read_only_partitions: Vec<PartitionId>,

    /// When set this will buffer WAL writes in memory based on the
    /// configuration.
    #[serde(default)]
    pub wal_buffer_config: Option<WalBufferConfig>,

    /// Unless explicitly disabled by setting this to None (or null in JSON),
    /// writes will go into a queryable in-memory database
    /// called the Mutable Buffer. It is optimized to receive writes so they
    /// can be batched together later to the Read Buffer or to Parquet files
    /// in object storage.
    #[serde(default = "MutableBufferConfig::default_option")]
    pub mutable_buffer_config: Option<MutableBufferConfig>,
}

impl DatabaseRules {
    pub fn partition_key(
        &self,
        line: &ParsedLine<'_>,
        default_time: &DateTime<Utc>,
    ) -> Result<String> {
        self.partition_template.partition_key(line, default_time)
    }

    pub fn new() -> Self {
        Self {
            mutable_buffer_config: MutableBufferConfig::default_option(),
            ..Default::default()
        }
    }
}

/// MutableBufferConfig defines the configuration for the in-memory database
/// that is hot for writes as they arrive. Operators can define rules for
/// evicting data once the mutable buffer passes a set memory threshold.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct MutableBufferConfig {
    /// The size the mutable buffer should be limited to. Once the buffer gets
    /// to this size it will drop partitions in the given order. If unable
    /// to drop partitions (because of later rules in this config) it will
    /// reject writes until it is able to drop partitions.
    pub buffer_size: u64,
    /// If set, the mutable buffer will not drop partitions that have chunks
    /// that have not yet been persisted. Thus it will reject writes if it
    /// is over size and is unable to drop partitions. The default is to
    /// drop partitions in the sort order, regardless of whether they have
    /// unpersisted chunks or not. The WAL Buffer can be used to ensure
    /// persistence, but this may cause longer recovery times.
    pub reject_if_not_persisted: bool,
    /// Drop partitions to free up space in this order. Can be by the oldest
    /// created at time, the longest since the last write, or the min or max of
    /// some column.
    pub partition_drop_order: PartitionSortRules,
    /// Attempt to persist partitions after they haven't received a write for
    /// this number of seconds. If not set, partitions won't be
    /// automatically persisted.
    pub persist_after_cold_seconds: Option<u32>,
}

const DEFAULT_MUTABLE_BUFFER_SIZE: u64 = 2_147_483_648; // 2 GB
const DEFAULT_PERSIST_AFTER_COLD_SECONDS: u32 = 900; // 15 minutes

impl MutableBufferConfig {
    fn default_option() -> Option<Self> {
        Some(Self::default())
    }
}

impl Default for MutableBufferConfig {
    fn default() -> Self {
        Self {
            buffer_size: DEFAULT_MUTABLE_BUFFER_SIZE,
            // keep taking writes and drop partitions on the floor
            reject_if_not_persisted: false,
            partition_drop_order: PartitionSortRules {
                order: Order::Desc,
                sort: PartitionSort::CreatedAtTime,
            },
            // rollover the chunk and persist it after the partition has been cold for
            // 15 minutes
            persist_after_cold_seconds: Some(DEFAULT_PERSIST_AFTER_COLD_SECONDS),
        }
    }
}

/// This struct specifies the rules for the order to sort partitions
/// from the mutable buffer. This is used to determine which order to drop them
/// in. The last partition in the list will be dropped, until enough space has
/// been freed up to be below the max size.
///
/// For example, to drop the partition that has been open longest:
/// ```
/// use data_types::database_rules::{PartitionSortRules, Order, PartitionSort};
///
/// let rules = PartitionSortRules{
///     order: Order::Desc,
///     sort: PartitionSort::CreatedAtTime,
/// };
/// ```
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct PartitionSortRules {
    /// Sort partitions by this order. Last will be dropped.
    pub order: Order,
    /// Sort by either a column value, or when the partition was opened, or when
    /// it last received a write.
    pub sort: PartitionSort,
}

/// What to sort the partition by.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub enum PartitionSort {
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

/// The sort order.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub enum Order {
    Asc,
    Desc,
}

/// Use columns of this type.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub enum ColumnType {
    I64,
    U64,
    F64,
    String,
    Bool,
}

/// Use either the min or max summary statistic.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub enum ColumnValue {
    Min,
    Max,
}

/// WalBufferConfig defines the configuration for buffering data from the WAL in
/// memory. This buffer is used for asynchronous replication and to collect
/// segments before sending them to object storage.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct WalBufferConfig {
    /// The size the WAL buffer should be limited to. Once the buffer gets to
    /// this size it will drop old segments to remain below this size, but
    /// still try to hold as much in memory as possible while remaining
    /// below this threshold
    pub buffer_size: u64,
    /// WAL segments become read only after crossing over this size. Which means
    /// that segments will always be >= this size. When old segments are
    /// dropped from of memory, at least this much space will be freed from
    /// the buffer.
    pub segment_size: u64,
    /// What should happen if a write comes in that would exceed the WAL buffer
    /// size and the oldest segment that could be dropped hasn't yet been
    /// persisted to object storage. If the oldest segment has been
    /// persisted, then it will be dropped from the buffer so that new writes
    /// can be accepted. This option is only for defining the behavior of what
    /// happens if that segment hasn't been persisted. If set to return an
    /// error, new writes will be rejected until the oldest segment has been
    /// persisted so that it can be cleared from memory. Alternatively, this
    /// can be set so that old segments are dropped even if they haven't been
    /// persisted. This setting is also useful for cases where persistence
    /// isn't being used and this is only for in-memory buffering.
    pub buffer_rollover: WalBufferRollover,
    /// If set to true, buffer segments will be written to object storage.
    pub store_segments: bool,
    /// If set, segments will be rolled over after this period of time even
    /// if they haven't hit the size threshold. This allows them to be written
    /// out to object storage as they must be immutable first.
    pub close_segment_after: Option<std::time::Duration>,
}

/// WalBufferRollover defines the behavior of what should happen if a write
/// comes in that would cause the buffer to exceed its max size AND the oldest
/// segment can't be dropped because it has not yet been persisted.
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, Copy)]
pub enum WalBufferRollover {
    /// Drop the old segment even though it hasn't been persisted. This part of
    /// the WAl will be lost on this server.
    DropOldSegment,
    /// Drop the incoming write and fail silently. This favors making sure that
    /// older WAL data will be backed up.
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
#[derive(Debug, Serialize, Deserialize, Default, Eq, PartialEq, Clone)]
pub struct PartitionTemplate {
    pub parts: Vec<TemplatePart>,
}

impl PartitionTemplate {
    pub fn partition_key(
        &self,
        line: &ParsedLine<'_>,
        default_time: &DateTime<Utc>,
    ) -> Result<String> {
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
                TemplatePart::TimeFormat(format) => match line.timestamp {
                    Some(t) => Utc.timestamp_nanos(t).format(&format).to_string(),
                    None => default_time.format(&format).to_string(),
                },
                _ => unimplemented!(),
            })
            .collect();

        Ok(parts.join("-"))
    }
}

/// `TemplatePart` specifies what part of a row should be used to compute this
/// part of a partition key.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub enum TemplatePart {
    Table,
    Column(String),
    TimeFormat(String),
    RegexCapture(RegexCapture),
    StrftimeColumn(StrftimeColumn),
}

/// `RegexCapture` is for pulling parts of a string column into the partition
/// key.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct RegexCapture {
    column: String,
    regex: String,
}

/// `StrftimeColumn` can be used to create a time based partition key off some
/// column other than the builtin `time` column.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct StrftimeColumn {
    column: String,
    format: String,
}

/// `PartitionId` is the object storage identifier for a specific partition. It
/// should be a path that can be used against an object store to locate all the
/// files and subdirectories for a partition. It takes the form of `/<writer
/// ID>/<database>/<partition key>/`.
pub type PartitionId = String;
pub type WriterId = u32;

/// `Subscription` represents a group of hosts that want to receive data as it
/// arrives. The subscription has a matcher that is used to determine what data
/// will match it, and an optional queue for storing matched writes. Subscribers
/// that recieve some subeset of an individual replicated write will get a new
/// replicated write, but with the same originating writer ID and sequence
/// number for the consuming subscriber's tracking purposes.
///
/// For pull based subscriptions, the requester will send a matcher, which the
/// receiver will execute against its in-memory WAL.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct Subscription {
    pub name: String,
    pub host_group_id: HostGroupId,
    pub matcher: Matcher,
}

/// `Matcher` specifies the rule against the table name and/or a predicate
/// against the row to determine if it matches the write rule.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct Matcher {
    pub tables: MatchTables,
    // TODO: make this work with query::Predicate
    #[serde(skip_serializing_if = "Option::is_none")]
    pub predicate: Option<String>,
}

/// `MatchTables` looks at the table name of a row to determine if it should
/// match the rule.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
#[serde(rename_all = "camelCase")]
pub enum MatchTables {
    #[serde(rename = "*")]
    All,
    Table(String),
    Regex(String),
}

pub type HostGroupId = String;

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct HostGroup {
    pub id: HostGroupId,
    /// `hosts` is a vector of connection strings for remote hosts.
    pub hosts: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use influxdb_line_protocol::parse_lines;

    type TestError = Box<dyn std::error::Error + Send + Sync + 'static>;
    type Result<T = (), E = TestError> = std::result::Result<T, E>;

    #[test]
    fn partition_key_with_table() -> Result {
        let template = PartitionTemplate {
            parts: vec![TemplatePart::Table],
        };

        let line = parse_line("cpu foo=1 10");
        assert_eq!("cpu", template.partition_key(&line, &Utc::now()).unwrap());

        Ok(())
    }

    #[test]
    fn partition_key_with_int_field() -> Result {
        let template = PartitionTemplate {
            parts: vec![TemplatePart::Column("foo".to_string())],
        };

        let line = parse_line("cpu foo=1 10");
        assert_eq!("foo_1", template.partition_key(&line, &Utc::now()).unwrap());

        Ok(())
    }

    #[test]
    fn partition_key_with_float_field() -> Result {
        let template = PartitionTemplate {
            parts: vec![TemplatePart::Column("foo".to_string())],
        };

        let line = parse_line("cpu foo=1.1 10");
        assert_eq!(
            "foo_1.1",
            template.partition_key(&line, &Utc::now()).unwrap()
        );

        Ok(())
    }

    #[test]
    fn partition_key_with_string_field() -> Result {
        let template = PartitionTemplate {
            parts: vec![TemplatePart::Column("foo".to_string())],
        };

        let line = parse_line("cpu foo=\"asdf\" 10");
        assert_eq!(
            "foo_asdf",
            template.partition_key(&line, &Utc::now()).unwrap()
        );

        Ok(())
    }

    #[test]
    fn partition_key_with_bool_field() -> Result {
        let template = PartitionTemplate {
            parts: vec![TemplatePart::Column("bar".to_string())],
        };

        let line = parse_line("cpu bar=true 10");
        assert_eq!(
            "bar_true",
            template.partition_key(&line, &Utc::now()).unwrap()
        );

        Ok(())
    }

    #[test]
    fn partition_key_with_tag_column() -> Result {
        let template = PartitionTemplate {
            parts: vec![TemplatePart::Column("region".to_string())],
        };

        let line = parse_line("cpu,region=west usage_user=23.2 10");
        assert_eq!(
            "region_west",
            template.partition_key(&line, &Utc::now()).unwrap()
        );

        Ok(())
    }

    #[test]
    fn partition_key_with_missing_column() -> Result {
        let template = PartitionTemplate {
            parts: vec![TemplatePart::Column("not_here".to_string())],
        };

        let line = parse_line("cpu,foo=asdf bar=true 10");
        assert_eq!("", template.partition_key(&line, &Utc::now()).unwrap());

        Ok(())
    }

    #[test]
    fn partition_key_with_time() -> Result {
        let template = PartitionTemplate {
            parts: vec![TemplatePart::TimeFormat("%Y-%m-%d %H:%M:%S".to_string())],
        };

        let line = parse_line("cpu,foo=asdf bar=true 1602338097000000000");
        assert_eq!(
            "2020-10-10 13:54:57",
            template.partition_key(&line, &Utc::now()).unwrap()
        );

        Ok(())
    }

    #[test]
    fn partition_key_with_default_time() -> Result {
        let format_string = "%Y-%m-%d %H:%M:%S";
        let template = PartitionTemplate {
            parts: vec![TemplatePart::TimeFormat(format_string.to_string())],
        };

        let default_time = Utc::now();
        let line = parse_line("cpu,foo=asdf bar=true");
        assert_eq!(
            default_time.format(format_string).to_string(),
            template.partition_key(&line, &default_time).unwrap()
        );

        Ok(())
    }

    #[test]
    fn partition_key_with_many_parts() -> Result {
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
            template.partition_key(&line, &Utc::now()).unwrap()
        );

        Ok(())
    }

    fn parsed_lines(lp: &str) -> Vec<ParsedLine<'_>> {
        parse_lines(lp).map(|l| l.unwrap()).collect()
    }

    fn parse_line(line: &str) -> ParsedLine<'_> {
        parsed_lines(line).pop().unwrap()
    }
}
