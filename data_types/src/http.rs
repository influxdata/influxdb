//! This module contains structs for the HTTP API
use crate::write_buffer::SegmentSummary;
use serde::{Deserialize, Serialize};

/// Query string for the Write Buffer metadata endpoint
#[derive(Debug, Clone, Serialize, Deserialize, Default, Eq, PartialEq)]
pub struct WriteBufferMetadataQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub newer_than: Option<chrono::DateTime<chrono::Utc>>,
}

/// Response for the Write Buffer metadata endpoint
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct WriteBufferMetadataResponse {
    pub segments: Vec<SegmentSummary>,
}
