use std::{sync::Arc, time::Duration};

use serde::Serialize;

use crate::events::{EventData, EventOutcome};

#[derive(Debug, Clone, Serialize)]
pub struct SuccessInfo {
    pub host: Arc<str>,
    pub catalog_sequence_number: u32,
    #[serde(skip_serializing)]
    pub duration: Duration,
}

impl SuccessInfo {
    pub fn new(host: &str, catalog_sequence_number: u32, fetch_duration: Duration) -> Self {
        Self {
            host: Arc::from(host),
            catalog_sequence_number,
            duration: fetch_duration,
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

#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum CatalogFetched {
    Success(SuccessInfo),
    Failed(FailedInfo),
}

impl EventData for CatalogFetched {
    fn name(&self) -> &'static str {
        "catalog_fetched"
    }

    fn outcome(&self) -> EventOutcome {
        match self {
            CatalogFetched::Success(_) => EventOutcome::Success,
            CatalogFetched::Failed(_) => EventOutcome::Failed,
        }
    }

    fn duration(&self) -> Duration {
        match self {
            CatalogFetched::Success(success) => success.duration,
            CatalogFetched::Failed(failed) => failed.duration,
        }
    }
}
