use std::{sync::Arc, time::Duration};

use serde::Serialize;

use crate::events::{EventData, EventOutcome};

#[derive(Debug, Clone, Serialize)]
pub struct SuccessInfo {
    pub input_generations: Vec<u8>,
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

#[derive(Serialize, Debug, Clone)]
#[serde(untagged)]
pub enum CompactionPlanned {
    Success(SuccessInfo),
    Failed(FailedInfo),
}

impl EventData for CompactionPlanned {
    fn name(&self) -> &'static str {
        "COMPACTION_PLANNED"
    }

    fn outcome(&self) -> EventOutcome {
        match self {
            CompactionPlanned::Success(_) => EventOutcome::Success,
            CompactionPlanned::Failed(_) => EventOutcome::Failed,
        }
    }

    fn duration(&self) -> Duration {
        match self {
            CompactionPlanned::Success(success) => success.duration,
            CompactionPlanned::Failed(failed) => failed.duration,
        }
    }
}
