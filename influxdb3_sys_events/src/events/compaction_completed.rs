use std::{sync::Arc, time::Duration};

use serde::Serialize;

use crate::events::{EventData, EventOutcome};

#[derive(Debug, Clone, Serialize)]
pub struct PlanIdentifier {
    pub db_name: Arc<str>,
    pub table_name: Arc<str>,
    pub output_generation: u8,
}

#[derive(Debug, Clone, Serialize)]
pub struct PlanGroupRunSuccessInfo {
    #[serde(skip_serializing)]
    pub duration: Duration,
    pub plans_ran: Vec<PlanIdentifier>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PlanGroupRunFailedInfo {
    #[serde(skip_serializing)]
    pub duration: Duration,
    pub error: String,
    pub plans_ran: Vec<PlanIdentifier>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PlanRunSuccessInfo {
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
pub struct PlanRunFailedInfo {
    #[serde(skip_serializing)]
    pub duration: Duration,
    pub error: String,
    pub identifier: PlanIdentifier,
}

#[derive(Serialize, Debug, Clone)]
#[serde(untagged)]
pub enum PlanCompactionCompleted {
    Success(PlanRunSuccessInfo),
    Failed(PlanRunFailedInfo),
}

impl EventData for PlanCompactionCompleted {
    fn name(&self) -> &'static str {
        "plan_run_completed"
    }

    fn outcome(&self) -> EventOutcome {
        match self {
            PlanCompactionCompleted::Success(_) => EventOutcome::Success,
            PlanCompactionCompleted::Failed(_) => EventOutcome::Failed,
        }
    }

    fn duration(&self) -> Duration {
        match self {
            PlanCompactionCompleted::Success(success) => success.duration,
            PlanCompactionCompleted::Failed(failed) => failed.duration,
        }
    }
}

#[derive(Serialize, Debug, Clone)]
#[serde(untagged)]
pub enum PlanGroupCompactionCompleted {
    Success(PlanGroupRunSuccessInfo),
    Failed(PlanGroupRunFailedInfo),
}

impl EventData for PlanGroupCompactionCompleted {
    fn name(&self) -> &'static str {
        "plan_group_run_completed"
    }

    fn outcome(&self) -> EventOutcome {
        match self {
            PlanGroupCompactionCompleted::Success(_) => EventOutcome::Success,
            PlanGroupCompactionCompleted::Failed(_) => EventOutcome::Failed,
        }
    }

    fn duration(&self) -> Duration {
        match self {
            PlanGroupCompactionCompleted::Success(success) => success.duration,
            PlanGroupCompactionCompleted::Failed(failed) => failed.duration,
        }
    }
}
