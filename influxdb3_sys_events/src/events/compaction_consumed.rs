use std::{sync::Arc, time::Duration};

use serde::Serialize;

use crate::events::{EventData, EventOutcome};

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

#[derive(Serialize, Debug, Clone)]
#[serde(untagged)]
pub enum CompactionConsumed {
    Success(SuccessInfo),
    Failed(FailedInfo),
}

impl EventData for CompactionConsumed {
    fn name(&self) -> &'static str {
        "COMPACTION_CONSUMED"
    }

    fn outcome(&self) -> EventOutcome {
        match self {
            CompactionConsumed::Success(_) => EventOutcome::Success,
            CompactionConsumed::Failed(_) => EventOutcome::Failed,
        }
    }

    fn duration(&self) -> Duration {
        match self {
            CompactionConsumed::Success(success) => success.duration,
            CompactionConsumed::Failed(failed) => failed.duration,
        }
    }
}
