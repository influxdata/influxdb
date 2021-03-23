//! Retention Rules

use serde::{Deserialize, Serialize};

/// RetentionRule
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct RetentionRule {
    /// Expiry
    #[serde(rename = "type")]
    pub r#type: Type,
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
    /// Returns instance of RetentionRule
    pub fn new(r#type: Type, every_seconds: i32) -> Self {
        Self {
            r#type,
            every_seconds,
            shard_group_duration_seconds: None,
        }
    }
}

/// Set Retention Rule expired or not
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum Type {
    /// RetentionRule Expired
    #[serde(rename = "expire")]
    Expire,
}
