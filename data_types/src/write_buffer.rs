use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// The persistence metadata associated with a given segment
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct SegmentPersistence {
    pub location: String,
    pub time: DateTime<Utc>,
}

/// The summary information for a segment
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct SegmentSummary {
    pub size: u64,
    pub created_at: DateTime<Utc>,
    pub persisted: Option<SegmentPersistence>,
}
