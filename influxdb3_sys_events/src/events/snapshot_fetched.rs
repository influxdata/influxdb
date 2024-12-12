use std::{sync::Arc, time::Duration};

use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct SuccessInfo {
    pub host: Arc<str>,
    pub sequence_number: u64,
    #[serde(skip_serializing)]
    pub fetch_duration: Duration,
    pub db_count: u64,
    pub table_count: u64,
    pub file_count: u64,
}

impl SuccessInfo {
    pub fn new(
        host: &str,
        sequence_number: u64,
        duration: Duration,
        db_table_file_counts: (u64, u64, u64),
    ) -> Self {
        Self {
            host: Arc::from(host),
            sequence_number,
            fetch_duration: duration,
            db_count: db_table_file_counts.0,
            table_count: db_table_file_counts.1,
            file_count: db_table_file_counts.2,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct FailedInfo {
    pub host: Arc<str>,
    pub sequence_number: u64,
    #[serde(skip_serializing)]
    pub duration: Duration,
    pub error: String,
}

#[derive(Debug, Clone)]
pub enum SnapshotFetched {
    Success(SuccessInfo),
    Failed(FailedInfo),
}
