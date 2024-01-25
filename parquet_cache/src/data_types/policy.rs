use serde::{Deserialize, Serialize};

/// TODO: clap_blocks
#[derive(Debug, Default, Clone, Copy)]
pub struct PolicyConfig {
    pub max_capacity: u64,
    pub event_recency_max_duration_nanoseconds: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, Default)]
pub struct ObjectParams {
    pub namespace_id: i64,
    pub table_id: i64,
    pub min_time: i64,
    pub max_time: i64,
    pub file_size_bytes: i64,
}
