use crate::database_rules::WriterId;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// The summary information for a writer that has data in a segment
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct WriterSummary {
    pub start_sequence: u64,
    pub end_sequence: u64,
    pub missing_sequence: bool,
}

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
    pub writers: BTreeMap<WriterId, WriterSummary>,
}
