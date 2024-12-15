use std::{sync::Arc, time::Duration};

use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct SuccessInfo {
    #[serde(skip_serializing)]
    pub duration: Duration,
    pub db_name: Arc<str>,
    pub table_name: Arc<str>,
    pub new_generations: Vec<u8>,
    pub removed_generations: Vec<u8>,
    pub summary_sequence_number: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct FailedInfo {
    #[serde(skip_serializing)]
    pub duration: Duration,
    pub error: String,
    pub summary_sequence_number: u64,
}

#[derive(Debug, Clone)]
pub enum CompactionConsumed {
    Success(SuccessInfo),
    Failed(FailedInfo),
}
