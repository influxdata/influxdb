use data_types::{SequenceNumber, TopicId};

/// Magic number to be used shard indices and shard ids in "kafkaless".
pub(crate) const TRANSITION_SHARD_NUMBER: i32 = 1234;
/// In kafkaless mode all new persisted data uses this shard id.
pub(crate) const TRANSITION_SHARD_ID: ShardId = ShardId::new(TRANSITION_SHARD_NUMBER as i64);
/// In kafkaless mode all new persisted data uses this shard index.
pub(crate) const TRANSITION_SHARD_INDEX: ShardIndex = ShardIndex::new(TRANSITION_SHARD_NUMBER);

/// Unique ID for a `Shard`, assigned by the catalog. Joins to other catalog tables to uniquely
/// identify shards independently of the underlying write buffer implementation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type)]
#[sqlx(transparent)]
pub(crate) struct ShardId(i64);

#[allow(missing_docs)]
impl ShardId {
    pub(crate) const fn new(v: i64) -> Self {
        Self(v)
    }
}

impl std::fmt::Display for ShardId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// The index of the shard in the set of shards. When Kafka is used as the write buffer, this is
/// the Kafka Partition ID. Used by the router and write buffer to shard requests to a particular
/// index in a set of shards.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type)]
#[sqlx(transparent)]
pub(crate) struct ShardIndex(i32);

#[allow(missing_docs)]
impl ShardIndex {
    pub(crate) const fn new(v: i32) -> Self {
        Self(v)
    }
}

impl std::fmt::Display for ShardIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for ShardIndex {
    type Err = std::num::ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let v: i32 = s.parse()?;
        Ok(Self(v))
    }
}

/// Data object for a shard. Only one shard record can exist for a given topic and shard
/// index (enforced via uniqueness constraint).
#[derive(Debug, Copy, Clone, PartialEq, Eq, sqlx::FromRow)]
pub(crate) struct Shard {
    /// the id of the shard, assigned by the catalog
    pub(crate) id: ShardId,
    /// the topic the shard is reading from
    pub(crate) topic_id: TopicId,
    /// the shard index of the shard the sequence numbers are coming from, sharded by the router
    /// and write buffer
    pub(crate) shard_index: ShardIndex,
    /// The minimum unpersisted sequence number. Because different tables
    /// can be persisted at different times, it is possible some data has been persisted
    /// with a higher sequence number than this. However, all data with a sequence number
    /// lower than this must have been persisted to Parquet.
    pub(crate) min_unpersisted_sequence_number: SequenceNumber,
}

/// Shard index plus offset
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) struct Sequence {
    /// The shard index
    pub(crate) shard_index: ShardIndex,
    /// The sequence number
    pub(crate) sequence_number: SequenceNumber,
}
