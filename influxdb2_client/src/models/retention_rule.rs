use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RetentionRule {
    #[serde(rename = "type")]
    pub _type: Type,
    /// Duration in seconds for how long data will be kept in the database. 0
    /// means infinite.
    #[serde(rename = "everySeconds")]
    pub every_seconds: i32,
    /// Shard duration measured in seconds.
    #[serde(
        rename = "shardGroupDurationSeconds",
        skip_serializing_if = "Option::is_none"
    )]
    pub shard_group_duration_seconds: Option<i64>,
}

impl RetentionRule {
    pub fn new(_type: Type, every_seconds: i32) -> RetentionRule {
        RetentionRule {
            _type,
            every_seconds,
            shard_group_duration_seconds: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum Type {
    #[serde(rename = "expire")]
    Expire,
}
