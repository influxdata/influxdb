use std::{sync::Arc, time::Duration};

use serde::Serialize;

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

#[derive(Debug, Clone, Serialize)]
pub enum PlanCompactionCompleted {
    PlanRunSuccessInfo(PlanRunSuccessInfo),
    PlanRunFailedInfo(PlanRunFailedInfo),
}

#[derive(Debug, Clone, Serialize)]
pub enum PlanGroupCompactionCompleted {
    PlanGroupRunSuccessInfo(PlanGroupRunSuccessInfo),
    PlanGroupRunFailedInfo(PlanGroupRunFailedInfo),
}
