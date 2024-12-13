use std::{sync::Arc, time::Duration};

use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct SuccessInfo {
    pub num_input_generations: u64,
    pub input_paths: Vec<Arc<str>>,
    pub output_level: u8,
    pub db_name: Arc<str>,
    pub table_name: Arc<str>,
    #[serde(skip_serializing)]
    pub duration: Duration,
    pub left_over_gen1_files: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct FailedInfo {
    pub duration: Duration,
    pub error: String,
}

#[derive(Debug, Clone, Serialize)]
pub enum CompactionPlanned {
    SuccessInfo(SuccessInfo),
    FailedInfo(FailedInfo),
}
