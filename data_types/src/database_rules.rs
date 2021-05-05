use crate::{
    consistent_hasher::ConsistentHasher,
    field_validation::{FromField, FromFieldOpt, FromFieldString, FromFieldVec},
    server_id::ServerId,
    DatabaseName,
};
use chrono::{DateTime, TimeZone, Utc};
use generated_types::{
    google::{protobuf::Empty, FieldViolation, FieldViolationExt},
    influxdata::iox::management::v1 as management,
};
use influxdb_line_protocol::ParsedLine;
use regex::Regex;
use snafu::{OptionExt, Snafu};
use std::num::NonZeroU64;
use std::{
    collections::HashMap,
    convert::{TryFrom, TryInto},
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

    #[snafu(context(false))]
    ProstDecodeError { source: prost::DecodeError },

    #[snafu(context(false))]
    ProstEncodeError { source: prost::EncodeError },

    #[snafu(context(false))]
    FieldViolation { source: FieldViolation },

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

    /// An optional config to split writes into different "shards". A shard
    /// is a logical concept, but the usage is meant to split data into
    /// mutually exclusive areas. The rough order of organization is:
    /// database -> shard -> partition -> chunk. For example, you could shard
    /// based on table name and assign to 1 of 10 shards. Within each
    /// shard you would have partitions, which would likely be based off time.
    /// This makes it possible to horizontally scale out writes.
    pub shard_config: Option<ShardConfig>,
}

impl DatabaseRules {
    pub fn partition_key(
        &self,
        line: &ParsedLine<'_>,
        default_time: &DateTime<Utc>,
    ) -> Result<String> {
        self.partition_template.partition_key(line, default_time)
    }

    pub fn new(name: DatabaseName<'static>) -> Self {
        Self {
            name,
            partition_template: Default::default(),
            write_buffer_config: None,
            lifecycle_rules: Default::default(),
            shard_config: None,
        }
    }

    pub fn db_name(&self) -> &str {
        &self.name.as_str()
    }
}

impl DatabaseRules {
    pub fn decode(bytes: prost::bytes::Bytes) -> Result<Self> {
        let message: management::DatabaseRules = prost::Message::decode(bytes)?;
        Ok(message.try_into()?)
    }

    pub fn encode(self, bytes: &mut prost::bytes::BytesMut) -> Result<()> {
        let encoded: management::DatabaseRules = self.into();
        Ok(prost::Message::encode(&encoded, bytes)?)
    }
}

/// Generates a partition key based on the line and the default time.
pub trait Partitioner {
    fn partition_key(
        &self,
        _line: &ParsedLine<'_>,
        _default_time: &DateTime<Utc>,
    ) -> Result<String>;
}

impl Partitioner for DatabaseRules {
    fn partition_key(&self, line: &ParsedLine<'_>, default_time: &DateTime<Utc>) -> Result<String> {
        self.partition_key(&line, &default_time)
    }
}

impl From<DatabaseRules> for management::DatabaseRules {
    fn from(rules: DatabaseRules) -> Self {
        Self {
            name: rules.name.into(),
            partition_template: Some(rules.partition_template.into()),
            write_buffer_config: rules.write_buffer_config.map(Into::into),
            lifecycle_rules: Some(rules.lifecycle_rules.into()),
            shard_config: rules.shard_config.map(Into::into),
        }
    }
}

impl TryFrom<management::DatabaseRules> for DatabaseRules {
    type Error = FieldViolation;

    fn try_from(proto: management::DatabaseRules) -> Result<Self, Self::Error> {
        let name = DatabaseName::new(proto.name.clone()).field("name")?;

        let write_buffer_config = proto.write_buffer_config.optional("write_buffer_config")?;

        let lifecycle_rules = proto
            .lifecycle_rules
            .optional("lifecycle_rules")?
            .unwrap_or_default();

        let partition_template = proto
            .partition_template
            .optional("partition_template")?
            .unwrap_or_default();

        let shard_config = proto
            .shard_config
            .optional("shard_config")
            .unwrap_or_default();

        Ok(Self {
            name,
            partition_template,
            write_buffer_config,
            lifecycle_rules,
            shard_config,
        })
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

impl From<LifecycleRules> for management::LifecycleRules {
    fn from(config: LifecycleRules) -> Self {
        Self {
            mutable_linger_seconds: config
                .mutable_linger_seconds
                .map(Into::into)
                .unwrap_or_default(),
            mutable_minimum_age_seconds: config
                .mutable_minimum_age_seconds
                .map(Into::into)
                .unwrap_or_default(),
            mutable_size_threshold: config
                .mutable_size_threshold
                .map(|x| x.get() as u64)
                .unwrap_or_default(),
            buffer_size_soft: config
                .buffer_size_soft
                .map(|x| x.get() as u64)
                .unwrap_or_default(),
            buffer_size_hard: config
                .buffer_size_hard
                .map(|x| x.get() as u64)
                .unwrap_or_default(),
            sort_order: Some(config.sort_order.into()),
            drop_non_persisted: config.drop_non_persisted,
            persist: config.persist,
            immutable: config.immutable,
            worker_backoff_millis: config.worker_backoff_millis.map_or(0, NonZeroU64::get),
        }
    }
}

impl TryFrom<management::LifecycleRules> for LifecycleRules {
    type Error = FieldViolation;

    fn try_from(proto: management::LifecycleRules) -> Result<Self, Self::Error> {
        Ok(Self {
            mutable_linger_seconds: proto.mutable_linger_seconds.try_into().ok(),
            mutable_minimum_age_seconds: proto.mutable_minimum_age_seconds.try_into().ok(),
            mutable_size_threshold: (proto.mutable_size_threshold as usize).try_into().ok(),
            buffer_size_soft: (proto.buffer_size_soft as usize).try_into().ok(),
            buffer_size_hard: (proto.buffer_size_hard as usize).try_into().ok(),
            sort_order: proto.sort_order.optional("sort_order")?.unwrap_or_default(),
            drop_non_persisted: proto.drop_non_persisted,
            persist: proto.persist,
            immutable: proto.immutable,
            worker_backoff_millis: NonZeroU64::new(proto.worker_backoff_millis),
        })
    }
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

impl From<SortOrder> for management::lifecycle_rules::SortOrder {
    fn from(ps: SortOrder) -> Self {
        let order: management::Order = ps.order.into();

        Self {
            order: order as _,
            sort: Some(ps.sort.into()),
        }
    }
}

impl TryFrom<management::lifecycle_rules::SortOrder> for SortOrder {
    type Error = FieldViolation;

    fn try_from(proto: management::lifecycle_rules::SortOrder) -> Result<Self, Self::Error> {
        Ok(Self {
            order: proto.order().scope("order")?,
            sort: proto.sort.optional("sort")?.unwrap_or_default(),
        })
    }
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

impl From<Sort> for management::lifecycle_rules::sort_order::Sort {
    fn from(ps: Sort) -> Self {
        use management::lifecycle_rules::sort_order::ColumnSort;

        match ps {
            Sort::LastWriteTime => Self::LastWriteTime(Empty {}),
            Sort::CreatedAtTime => Self::CreatedAtTime(Empty {}),
            Sort::Column(column_name, column_type, column_value) => {
                let column_type: management::ColumnType = column_type.into();
                let column_value: management::Aggregate = column_value.into();

                Self::Column(ColumnSort {
                    column_name,
                    column_type: column_type as _,
                    column_value: column_value as _,
                })
            }
        }
    }
}

impl TryFrom<management::lifecycle_rules::sort_order::Sort> for Sort {
    type Error = FieldViolation;

    fn try_from(proto: management::lifecycle_rules::sort_order::Sort) -> Result<Self, Self::Error> {
        use management::lifecycle_rules::sort_order::Sort;

        Ok(match proto {
            Sort::LastWriteTime(_) => Self::LastWriteTime,
            Sort::CreatedAtTime(_) => Self::CreatedAtTime,
            Sort::Column(column_sort) => {
                let column_type = column_sort.column_type().scope("column.column_type")?;
                let column_value = column_sort.column_value().scope("column.column_value")?;
                Self::Column(
                    column_sort.column_name.required("column.column_name")?,
                    column_type,
                    column_value,
                )
            }
        })
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

impl From<Order> for management::Order {
    fn from(o: Order) -> Self {
        match o {
            Order::Asc => Self::Asc,
            Order::Desc => Self::Desc,
        }
    }
}

impl TryFrom<management::Order> for Order {
    type Error = FieldViolation;

    fn try_from(proto: management::Order) -> Result<Self, Self::Error> {
        Ok(match proto {
            management::Order::Unspecified => Self::default(),
            management::Order::Asc => Self::Asc,
            management::Order::Desc => Self::Desc,
        })
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

impl From<ColumnType> for management::ColumnType {
    fn from(t: ColumnType) -> Self {
        match t {
            ColumnType::I64 => Self::I64,
            ColumnType::U64 => Self::U64,
            ColumnType::F64 => Self::F64,
            ColumnType::String => Self::String,
            ColumnType::Bool => Self::Bool,
        }
    }
}

impl TryFrom<management::ColumnType> for ColumnType {
    type Error = FieldViolation;

    fn try_from(proto: management::ColumnType) -> Result<Self, Self::Error> {
        Ok(match proto {
            management::ColumnType::Unspecified => return Err(FieldViolation::required("")),
            management::ColumnType::I64 => Self::I64,
            management::ColumnType::U64 => Self::U64,
            management::ColumnType::F64 => Self::F64,
            management::ColumnType::String => Self::String,
            management::ColumnType::Bool => Self::Bool,
        })
    }
}

/// Use either the min or max summary statistic.
#[derive(Debug, Eq, PartialEq, Clone)]
pub enum ColumnValue {
    Min,
    Max,
}

impl From<ColumnValue> for management::Aggregate {
    fn from(v: ColumnValue) -> Self {
        match v {
            ColumnValue::Min => Self::Min,
            ColumnValue::Max => Self::Max,
        }
    }
}

impl TryFrom<management::Aggregate> for ColumnValue {
    type Error = FieldViolation;

    fn try_from(proto: management::Aggregate) -> Result<Self, Self::Error> {
        use management::Aggregate;

        Ok(match proto {
            Aggregate::Unspecified => return Err(FieldViolation::required("")),
            Aggregate::Min => Self::Min,
            Aggregate::Max => Self::Max,
        })
    }
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

impl From<WriteBufferConfig> for management::WriteBufferConfig {
    fn from(rollover: WriteBufferConfig) -> Self {
        let buffer_rollover: management::write_buffer_config::Rollover =
            rollover.buffer_rollover.into();

        Self {
            buffer_size: rollover.buffer_size as u64,
            segment_size: rollover.segment_size as u64,
            buffer_rollover: buffer_rollover as _,
            persist_segments: rollover.store_segments,
            close_segment_after: rollover.close_segment_after.map(Into::into),
        }
    }
}

impl TryFrom<management::WriteBufferConfig> for WriteBufferConfig {
    type Error = FieldViolation;

    fn try_from(proto: management::WriteBufferConfig) -> Result<Self, Self::Error> {
        let buffer_rollover = proto.buffer_rollover().scope("buffer_rollover")?;
        let close_segment_after = proto
            .close_segment_after
            .map(TryInto::try_into)
            .transpose()
            .map_err(|_| FieldViolation {
                field: "closeSegmentAfter".to_string(),
                description: "Duration must be positive".to_string(),
            })?;

        Ok(Self {
            buffer_size: proto.buffer_size as usize,
            segment_size: proto.segment_size as usize,
            buffer_rollover,
            store_segments: proto.persist_segments,
            close_segment_after,
        })
    }
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

impl From<WriteBufferRollover> for management::write_buffer_config::Rollover {
    fn from(rollover: WriteBufferRollover) -> Self {
        match rollover {
            WriteBufferRollover::DropOldSegment => Self::DropOldSegment,
            WriteBufferRollover::DropIncoming => Self::DropIncoming,
            WriteBufferRollover::ReturnError => Self::ReturnError,
        }
    }
}

impl TryFrom<management::write_buffer_config::Rollover> for WriteBufferRollover {
    type Error = FieldViolation;

    fn try_from(proto: management::write_buffer_config::Rollover) -> Result<Self, Self::Error> {
        use management::write_buffer_config::Rollover;
        Ok(match proto {
            Rollover::Unspecified => return Err(FieldViolation::required("")),
            Rollover::DropOldSegment => Self::DropOldSegment,
            Rollover::DropIncoming => Self::DropIncoming,
            Rollover::ReturnError => Self::ReturnError,
        })
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
    fn partition_key(&self, line: &ParsedLine<'_>, default_time: &DateTime<Utc>) -> Result<String> {
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

impl From<PartitionTemplate> for management::PartitionTemplate {
    fn from(pt: PartitionTemplate) -> Self {
        Self {
            parts: pt.parts.into_iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<management::PartitionTemplate> for PartitionTemplate {
    type Error = FieldViolation;

    fn try_from(proto: management::PartitionTemplate) -> Result<Self, Self::Error> {
        let parts = proto.parts.vec_field("parts")?;
        Ok(Self { parts })
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
    column: String,
    regex: String,
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
    column: String,
    format: String,
}

impl From<TemplatePart> for management::partition_template::part::Part {
    fn from(part: TemplatePart) -> Self {
        use management::partition_template::part::ColumnFormat;

        match part {
            TemplatePart::Table => Self::Table(Empty {}),
            TemplatePart::Column(column) => Self::Column(column),
            TemplatePart::RegexCapture(RegexCapture { column, regex }) => {
                Self::Regex(ColumnFormat {
                    column,
                    format: regex,
                })
            }
            TemplatePart::StrftimeColumn(StrftimeColumn { column, format }) => {
                Self::StrfTime(ColumnFormat { column, format })
            }
            TemplatePart::TimeFormat(format) => Self::Time(format),
        }
    }
}

impl TryFrom<management::partition_template::part::Part> for TemplatePart {
    type Error = FieldViolation;

    fn try_from(proto: management::partition_template::part::Part) -> Result<Self, Self::Error> {
        use management::partition_template::part::{ColumnFormat, Part};

        Ok(match proto {
            Part::Table(_) => Self::Table,
            Part::Column(column) => Self::Column(column.required("column")?),
            Part::Regex(ColumnFormat { column, format }) => Self::RegexCapture(RegexCapture {
                column: column.required("regex.column")?,
                regex: format.required("regex.format")?,
            }),
            Part::StrfTime(ColumnFormat { column, format }) => {
                Self::StrftimeColumn(StrftimeColumn {
                    column: column.required("strf_time.column")?,
                    format: format.required("strf_time.format")?,
                })
            }
            Part::Time(format) => Self::TimeFormat(format.required("time")?),
        })
    }
}

impl From<TemplatePart> for management::partition_template::Part {
    fn from(part: TemplatePart) -> Self {
        Self {
            part: Some(part.into()),
        }
    }
}

impl TryFrom<management::partition_template::Part> for TemplatePart {
    type Error = FieldViolation;

    fn try_from(proto: management::partition_template::Part) -> Result<Self, Self::Error> {
        proto.part.required("part")
    }
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
    pub shards: Arc<HashMap<ShardId, NodeGroup>>,
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

impl From<ShardConfig> for management::ShardConfig {
    fn from(shard_config: ShardConfig) -> Self {
        Self {
            specific_targets: shard_config
                .specific_targets
                .into_iter()
                .map(|i| i.into())
                .collect(),
            hash_ring: shard_config.hash_ring.map(|i| i.into()),
            ignore_errors: shard_config.ignore_errors,
            shards: shard_config
                .shards
                .iter()
                .map(|(k, v)| (*k, from_node_group_for_management_node_group(v.clone())))
                .collect(),
        }
    }
}

impl TryFrom<management::ShardConfig> for ShardConfig {
    type Error = FieldViolation;

    fn try_from(proto: management::ShardConfig) -> Result<Self, Self::Error> {
        Ok(Self {
            specific_targets: proto
                .specific_targets
                .into_iter()
                .map(|i| i.try_into())
                .collect::<Result<Vec<MatcherToShard>, _>>()?,
            hash_ring: proto
                .hash_ring
                .map(|i| i.try_into())
                .map_or(Ok(None), |r| r.map(Some))?,
            ignore_errors: proto.ignore_errors,
            shards: Arc::new(
                proto
                    .shards
                    .into_iter()
                    .map(|(k, v)| {
                        try_from_management_node_group_for_node_group(v).map(|ng| (k, ng))
                    })
                    .collect::<Result<HashMap<u32, NodeGroup>, FieldViolation>>()?,
            ),
        })
    }
}

/// Returns none if v matches its default value.
fn none_if_default<T: Default + PartialEq>(v: T) -> Option<T> {
    if v == Default::default() {
        None
    } else {
        Some(v)
    }
}

impl From<MatcherToShard> for management::MatcherToShard {
    fn from(matcher_to_shard: MatcherToShard) -> Self {
        Self {
            matcher: none_if_default(matcher_to_shard.matcher.into()),
            shard: matcher_to_shard.shard,
        }
    }
}

impl TryFrom<management::MatcherToShard> for MatcherToShard {
    type Error = FieldViolation;

    fn try_from(proto: management::MatcherToShard) -> Result<Self, Self::Error> {
        Ok(Self {
            matcher: proto.matcher.unwrap_or_default().try_into()?,
            shard: proto.shard,
        })
    }
}

impl From<HashRing> for management::HashRing {
    fn from(hash_ring: HashRing) -> Self {
        Self {
            table_name: hash_ring.table_name,
            columns: hash_ring.columns,
            shards: hash_ring.shards.into(),
        }
    }
}

impl TryFrom<management::HashRing> for HashRing {
    type Error = FieldViolation;

    fn try_from(proto: management::HashRing) -> Result<Self, Self::Error> {
        Ok(Self {
            table_name: proto.table_name,
            columns: proto.columns,
            shards: proto.shards.into(),
        })
    }
}

// cannot (and/or don't know how to) add impl From inside prost generated code
fn from_node_group_for_management_node_group(node_group: NodeGroup) -> management::NodeGroup {
    management::NodeGroup {
        nodes: node_group
            .into_iter()
            .map(|id| management::node_group::Node { id: id.get_u32() })
            .collect(),
    }
}

fn try_from_management_node_group_for_node_group(
    proto: management::NodeGroup,
) -> Result<NodeGroup, FieldViolation> {
    proto
        .nodes
        .into_iter()
        .map(|i| {
            ServerId::try_from(i.id).map_err(|_| FieldViolation {
                field: "id".to_string(),
                description: "Node ID must be nonzero".to_string(),
            })
        })
        .collect()
}

impl From<Matcher> for management::Matcher {
    fn from(matcher: Matcher) -> Self {
        Self {
            table_name_regex: matcher
                .table_name_regex
                .map(|r| r.to_string())
                .unwrap_or_default(),
            predicate: matcher.predicate.unwrap_or_default(),
        }
    }
}

impl TryFrom<management::Matcher> for Matcher {
    type Error = FieldViolation;

    fn try_from(proto: management::Matcher) -> Result<Self, Self::Error> {
        let table_name_regex = match &proto.table_name_regex as &str {
            "" => None,
            re => Some(Regex::new(re).map_err(|e| FieldViolation {
                field: "table_name_regex".to_string(),
                description: e.to_string(),
            })?),
        };
        let predicate = match proto.predicate {
            p if p.is_empty() => None,
            p => Some(p),
        };

        Ok(Self {
            table_name_regex,
            predicate,
        })
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

    #[test]
    fn partition_key_with_table() {
        let template = PartitionTemplate {
            parts: vec![TemplatePart::Table],
        };

        let line = parse_line("cpu foo=1 10");
        assert_eq!("cpu", template.partition_key(&line, &Utc::now()).unwrap());
    }

    #[test]
    fn partition_key_with_int_field() {
        let template = PartitionTemplate {
            parts: vec![TemplatePart::Column("foo".to_string())],
        };

        let line = parse_line("cpu foo=1 10");
        assert_eq!("foo_1", template.partition_key(&line, &Utc::now()).unwrap());
    }

    #[test]
    fn partition_key_with_float_field() {
        let template = PartitionTemplate {
            parts: vec![TemplatePart::Column("foo".to_string())],
        };

        let line = parse_line("cpu foo=1.1 10");
        assert_eq!(
            "foo_1.1",
            template.partition_key(&line, &Utc::now()).unwrap()
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
            template.partition_key(&line, &Utc::now()).unwrap()
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
            template.partition_key(&line, &Utc::now()).unwrap()
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
            template.partition_key(&line, &Utc::now()).unwrap()
        );
    }

    #[test]
    fn partition_key_with_missing_column() {
        let template = PartitionTemplate {
            parts: vec![TemplatePart::Column("not_here".to_string())],
        };

        let line = parse_line("cpu,foo=asdf bar=true 10");
        assert_eq!("", template.partition_key(&line, &Utc::now()).unwrap());
    }

    #[test]
    fn partition_key_with_time() {
        let template = PartitionTemplate {
            parts: vec![TemplatePart::TimeFormat("%Y-%m-%d %H:%M:%S".to_string())],
        };

        let line = parse_line("cpu,foo=asdf bar=true 1602338097000000000");
        assert_eq!(
            "2020-10-10 13:54:57",
            template.partition_key(&line, &Utc::now()).unwrap()
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
            template.partition_key(&line, &default_time).unwrap()
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
            template.partition_key(&line, &Utc::now()).unwrap()
        );
    }

    fn parsed_lines(lp: &str) -> Vec<ParsedLine<'_>> {
        parse_lines(lp).map(|l| l.unwrap()).collect()
    }

    fn parse_line(line: &str) -> ParsedLine<'_> {
        parsed_lines(line).pop().unwrap()
    }

    #[test]
    fn test_database_rules_defaults() {
        let protobuf = management::DatabaseRules {
            name: "database".to_string(),
            ..Default::default()
        };

        let rules: DatabaseRules = protobuf.clone().try_into().unwrap();
        let back: management::DatabaseRules = rules.clone().into();

        assert_eq!(rules.name.as_str(), protobuf.name.as_str());
        assert_eq!(protobuf.name, back.name);

        assert_eq!(rules.partition_template.parts.len(), 0);

        // These will be defaulted as optionality not preserved on non-protobuf
        // DatabaseRules
        assert_eq!(back.partition_template, Some(Default::default()));
        assert_eq!(back.lifecycle_rules, Some(LifecycleRules::default().into()));

        // These should be none as preserved on non-protobuf DatabaseRules
        assert!(back.write_buffer_config.is_none());
        assert!(back.shard_config.is_none());
    }

    #[test]
    fn test_partition_template_default() {
        let protobuf = management::DatabaseRules {
            name: "database".to_string(),
            partition_template: Some(management::PartitionTemplate { parts: vec![] }),
            ..Default::default()
        };

        let rules: DatabaseRules = protobuf.clone().try_into().unwrap();
        let back: management::DatabaseRules = rules.clone().into();

        assert_eq!(rules.partition_template.parts.len(), 0);
        assert_eq!(protobuf.partition_template, back.partition_template);
    }

    #[test]
    fn test_partition_template_no_part() {
        let protobuf = management::DatabaseRules {
            name: "database".to_string(),
            partition_template: Some(management::PartitionTemplate {
                parts: vec![Default::default()],
            }),
            ..Default::default()
        };

        let res: Result<DatabaseRules, _> = protobuf.try_into();
        let err = res.expect_err("expected failure");

        assert_eq!(&err.field, "partition_template.parts.0.part");
        assert_eq!(&err.description, "Field is required");
    }

    #[test]
    fn test_partition_template() {
        use management::partition_template::part::{ColumnFormat, Part};

        let protobuf = management::PartitionTemplate {
            parts: vec![
                management::partition_template::Part {
                    part: Some(Part::Time("time".to_string())),
                },
                management::partition_template::Part {
                    part: Some(Part::Table(Empty {})),
                },
                management::partition_template::Part {
                    part: Some(Part::Regex(ColumnFormat {
                        column: "column".to_string(),
                        format: "format".to_string(),
                    })),
                },
            ],
        };

        let pt: PartitionTemplate = protobuf.clone().try_into().unwrap();
        let back: management::PartitionTemplate = pt.clone().into();

        assert_eq!(
            pt.parts,
            vec![
                TemplatePart::TimeFormat("time".to_string()),
                TemplatePart::Table,
                TemplatePart::RegexCapture(RegexCapture {
                    column: "column".to_string(),
                    regex: "format".to_string()
                })
            ]
        );
        assert_eq!(protobuf, back);
    }

    #[test]
    fn test_partition_template_empty() {
        use management::partition_template::part::{ColumnFormat, Part};

        let protobuf = management::PartitionTemplate {
            parts: vec![management::partition_template::Part {
                part: Some(Part::Regex(ColumnFormat {
                    ..Default::default()
                })),
            }],
        };

        let res: Result<PartitionTemplate, _> = protobuf.try_into();
        let err = res.expect_err("expected failure");

        assert_eq!(&err.field, "parts.0.part.regex.column");
        assert_eq!(&err.description, "Field is required");
    }

    #[test]
    fn test_write_buffer_config_default() {
        let protobuf: management::WriteBufferConfig = Default::default();

        let res: Result<WriteBufferConfig, _> = protobuf.try_into();
        let err = res.expect_err("expected failure");

        assert_eq!(&err.field, "buffer_rollover");
        assert_eq!(&err.description, "Field is required");
    }

    #[test]
    fn test_write_buffer_config_rollover() {
        let protobuf = management::WriteBufferConfig {
            buffer_rollover: management::write_buffer_config::Rollover::DropIncoming as _,
            ..Default::default()
        };

        let config: WriteBufferConfig = protobuf.clone().try_into().unwrap();
        let back: management::WriteBufferConfig = config.clone().into();

        assert_eq!(config.buffer_rollover, WriteBufferRollover::DropIncoming);
        assert_eq!(protobuf, back);
    }

    #[test]
    fn test_write_buffer_config_negative_duration() {
        use generated_types::google::protobuf::Duration;

        let protobuf = management::WriteBufferConfig {
            buffer_rollover: management::write_buffer_config::Rollover::DropOldSegment as _,
            close_segment_after: Some(Duration {
                seconds: -1,
                nanos: -40,
            }),
            ..Default::default()
        };

        let res: Result<WriteBufferConfig, _> = protobuf.try_into();
        let err = res.expect_err("expected failure");

        assert_eq!(&err.field, "closeSegmentAfter");
        assert_eq!(&err.description, "Duration must be positive");
    }

    #[test]
    fn lifecycle_rules() {
        let protobuf = management::LifecycleRules {
            mutable_linger_seconds: 123,
            mutable_minimum_age_seconds: 5345,
            mutable_size_threshold: 232,
            buffer_size_soft: 353,
            buffer_size_hard: 232,
            sort_order: None,
            drop_non_persisted: true,
            persist: true,
            immutable: true,
            worker_backoff_millis: 1000,
        };

        let config: LifecycleRules = protobuf.clone().try_into().unwrap();
        let back: management::LifecycleRules = config.clone().into();

        assert_eq!(config.sort_order, SortOrder::default());
        assert_eq!(
            config.mutable_linger_seconds.unwrap().get(),
            protobuf.mutable_linger_seconds
        );
        assert_eq!(
            config.mutable_minimum_age_seconds.unwrap().get(),
            protobuf.mutable_minimum_age_seconds
        );
        assert_eq!(
            config.mutable_size_threshold.unwrap().get(),
            protobuf.mutable_size_threshold as usize
        );
        assert_eq!(
            config.buffer_size_soft.unwrap().get(),
            protobuf.buffer_size_soft as usize
        );
        assert_eq!(
            config.buffer_size_hard.unwrap().get(),
            protobuf.buffer_size_hard as usize
        );
        assert_eq!(config.drop_non_persisted, protobuf.drop_non_persisted);
        assert_eq!(config.immutable, protobuf.immutable);

        assert_eq!(back.mutable_linger_seconds, protobuf.mutable_linger_seconds);
        assert_eq!(
            back.mutable_minimum_age_seconds,
            protobuf.mutable_minimum_age_seconds
        );
        assert_eq!(back.mutable_size_threshold, protobuf.mutable_size_threshold);
        assert_eq!(back.buffer_size_soft, protobuf.buffer_size_soft);
        assert_eq!(back.buffer_size_hard, protobuf.buffer_size_hard);
        assert_eq!(back.drop_non_persisted, protobuf.drop_non_persisted);
        assert_eq!(back.immutable, protobuf.immutable);
        assert_eq!(back.worker_backoff_millis, protobuf.worker_backoff_millis);
    }

    #[test]
    fn default_background_worker_backoff_millis() {
        let protobuf = management::LifecycleRules {
            worker_backoff_millis: 0,
            ..Default::default()
        };

        let config: LifecycleRules = protobuf.clone().try_into().unwrap();
        let back: management::LifecycleRules = config.into();
        assert_eq!(back.worker_backoff_millis, protobuf.worker_backoff_millis);
    }

    #[test]
    fn sort_order_default() {
        let protobuf: management::lifecycle_rules::SortOrder = Default::default();
        let config: SortOrder = protobuf.try_into().unwrap();

        assert_eq!(config, SortOrder::default());
        assert_eq!(config.order, Order::default());
        assert_eq!(config.sort, Sort::default());
    }

    #[test]
    fn sort_order() {
        use management::lifecycle_rules::sort_order;
        let protobuf = management::lifecycle_rules::SortOrder {
            order: management::Order::Asc as _,
            sort: Some(sort_order::Sort::CreatedAtTime(Empty {})),
        };
        let config: SortOrder = protobuf.clone().try_into().unwrap();
        let back: management::lifecycle_rules::SortOrder = config.clone().into();

        assert_eq!(protobuf, back);
        assert_eq!(config.order, Order::Asc);
        assert_eq!(config.sort, Sort::CreatedAtTime);
    }

    #[test]
    fn sort() {
        use management::lifecycle_rules::sort_order;

        let created_at: Sort = sort_order::Sort::CreatedAtTime(Empty {})
            .try_into()
            .unwrap();
        let last_write: Sort = sort_order::Sort::LastWriteTime(Empty {})
            .try_into()
            .unwrap();
        let column: Sort = sort_order::Sort::Column(sort_order::ColumnSort {
            column_name: "column".to_string(),
            column_type: management::ColumnType::Bool as _,
            column_value: management::Aggregate::Min as _,
        })
        .try_into()
        .unwrap();

        assert_eq!(created_at, Sort::CreatedAtTime);
        assert_eq!(last_write, Sort::LastWriteTime);
        assert_eq!(
            column,
            Sort::Column("column".to_string(), ColumnType::Bool, ColumnValue::Min)
        );
    }

    #[test]
    fn partition_sort_column_sort() {
        use management::lifecycle_rules::sort_order;

        let res: Result<Sort, _> = sort_order::Sort::Column(Default::default()).try_into();
        let err1 = res.expect_err("expected failure");

        let res: Result<Sort, _> = sort_order::Sort::Column(sort_order::ColumnSort {
            column_type: management::ColumnType::F64 as _,
            ..Default::default()
        })
        .try_into();
        let err2 = res.expect_err("expected failure");

        let res: Result<Sort, _> = sort_order::Sort::Column(sort_order::ColumnSort {
            column_type: management::ColumnType::F64 as _,
            column_value: management::Aggregate::Max as _,
            ..Default::default()
        })
        .try_into();
        let err3 = res.expect_err("expected failure");

        assert_eq!(err1.field, "column.column_type");
        assert_eq!(err1.description, "Field is required");

        assert_eq!(err2.field, "column.column_value");
        assert_eq!(err2.description, "Field is required");

        assert_eq!(err3.field, "column.column_name");
        assert_eq!(err3.description, "Field is required");
    }

    #[test]
    fn test_matcher_default() {
        let protobuf = management::Matcher {
            ..Default::default()
        };

        let matcher: Matcher = protobuf.clone().try_into().unwrap();
        let back: management::Matcher = matcher.clone().into();

        assert!(matcher.table_name_regex.is_none());
        assert_eq!(protobuf.table_name_regex, back.table_name_regex);

        assert_eq!(matcher.predicate, None);
        assert_eq!(protobuf.predicate, back.predicate);
    }

    #[test]
    fn test_matcher_regexp() {
        let protobuf = management::Matcher {
            table_name_regex: "^foo$".into(),
            ..Default::default()
        };

        let matcher: Matcher = protobuf.clone().try_into().unwrap();
        let back: management::Matcher = matcher.clone().into();

        assert_eq!(matcher.table_name_regex.unwrap().to_string(), "^foo$");
        assert_eq!(protobuf.table_name_regex, back.table_name_regex);
    }

    #[test]
    fn test_matcher_bad_regexp() {
        let protobuf = management::Matcher {
            table_name_regex: "*".into(),
            ..Default::default()
        };

        let matcher: Result<Matcher, FieldViolation> = protobuf.try_into();
        assert!(matcher.is_err());
        assert_eq!(matcher.err().unwrap().field, "table_name_regex");
    }

    #[test]
    fn test_hash_ring_default() {
        let protobuf = management::HashRing {
            ..Default::default()
        };

        let hash_ring: HashRing = protobuf.clone().try_into().unwrap();
        let back: management::HashRing = hash_ring.clone().into();

        assert_eq!(hash_ring.table_name, false);
        assert_eq!(protobuf.table_name, back.table_name);
        assert!(hash_ring.columns.is_empty());
        assert_eq!(protobuf.columns, back.columns);
        assert!(hash_ring.shards.is_empty());
        assert_eq!(protobuf.shards, back.shards);
    }

    #[test]
    fn test_hash_ring_nodes() {
        let protobuf = management::HashRing {
            shards: vec![1, 2],
            ..Default::default()
        };

        let hash_ring: HashRing = protobuf.try_into().unwrap();

        assert_eq!(hash_ring.shards.len(), 2);
        assert_eq!(hash_ring.shards.find(1), Some(2));
        assert_eq!(hash_ring.shards.find(2), Some(1));
    }

    #[test]
    fn test_matcher_to_shard_default() {
        let protobuf = management::MatcherToShard {
            ..Default::default()
        };

        let matcher_to_shard: MatcherToShard = protobuf.clone().try_into().unwrap();
        let back: management::MatcherToShard = matcher_to_shard.clone().into();

        assert_eq!(
            matcher_to_shard.matcher,
            Matcher {
                ..Default::default()
            }
        );
        assert_eq!(protobuf.matcher, back.matcher);

        assert_eq!(matcher_to_shard.shard, 0);
        assert_eq!(protobuf.shard, back.shard);
    }

    #[test]
    fn test_shard_config_default() {
        let protobuf = management::ShardConfig {
            ..Default::default()
        };

        let shard_config: ShardConfig = protobuf.clone().try_into().unwrap();
        let back: management::ShardConfig = shard_config.clone().into();

        assert!(shard_config.specific_targets.is_empty());
        assert_eq!(protobuf.specific_targets, back.specific_targets);

        assert!(shard_config.hash_ring.is_none());
        assert_eq!(protobuf.hash_ring, back.hash_ring);

        assert_eq!(shard_config.ignore_errors, false);
        assert_eq!(protobuf.ignore_errors, back.ignore_errors);

        assert!(shard_config.shards.is_empty());
        assert_eq!(protobuf.shards, back.shards);
    }

    #[test]
    fn test_database_rules_shard_config() {
        let protobuf = management::DatabaseRules {
            name: "database".to_string(),
            shard_config: Some(management::ShardConfig {
                ..Default::default()
            }),
            ..Default::default()
        };

        let rules: DatabaseRules = protobuf.try_into().unwrap();
        let back: management::DatabaseRules = rules.into();

        assert!(back.shard_config.is_some());
    }

    #[test]
    fn test_shard_config_shards() {
        let protobuf = management::ShardConfig {
            shards: vec![
                (
                    1,
                    management::NodeGroup {
                        nodes: vec![
                            management::node_group::Node { id: 10 },
                            management::node_group::Node { id: 11 },
                            management::node_group::Node { id: 12 },
                        ],
                    },
                ),
                (
                    2,
                    management::NodeGroup {
                        nodes: vec![management::node_group::Node { id: 20 }],
                    },
                ),
            ]
            .into_iter()
            .collect(),
            ..Default::default()
        };

        let shard_config: ShardConfig = protobuf.try_into().unwrap();

        assert_eq!(shard_config.shards.len(), 2);
        assert_eq!(shard_config.shards[&1].len(), 3);
        assert_eq!(shard_config.shards[&2].len(), 1);
    }

    #[test]
    fn test_sharder() {
        let protobuf = management::ShardConfig {
            specific_targets: vec![management::MatcherToShard {
                matcher: Some(management::Matcher {
                    table_name_regex: "pu$".to_string(),
                    ..Default::default()
                }),
                shard: 1,
            }],
            hash_ring: Some(management::HashRing {
                table_name: true,
                columns: vec!["t1", "t2", "f1", "f2"]
                    .into_iter()
                    .map(|i| i.to_string())
                    .collect(),
                // in practice we won't have that many shards
                // but for tests it's better to have more distinct values
                // so we don't hide bugs due to sheer luck.
                shards: (1000..1000000).collect(),
            }),
            ..Default::default()
        };

        let shard_config: ShardConfig = protobuf.try_into().unwrap();

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
        let protobuf = management::ShardConfig {
            hash_ring: Some(management::HashRing {
                table_name: true,
                columns: vec!["t1", "t2", "f1", "f2"]
                    .into_iter()
                    .map(|i| i.to_string())
                    .collect(),
                shards: vec![],
            }),
            ..Default::default()
        };

        let shard_config: ShardConfig = protobuf.try_into().unwrap();

        let line = parse_line("cpu,t1=1,t2=2,t3=3 f1=1,f2=2,f3=3 10");
        let err = shard_config.shard(&line).unwrap_err();
        assert!(matches!(err, Error::NoShardsDefined));
    }
}
