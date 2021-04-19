//! Health

use serde::{Deserialize, Serialize};

/// HealthCheck
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthCheck {
    /// Name of the influxdb instance
    pub name: String,
    /// Message
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// Checks
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub checks: Vec<crate::models::HealthCheck>,
    /// Status
    pub status: Status,
    /// Version
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    /// Commit
    #[serde(skip_serializing_if = "Option::is_none")]
    pub commit: Option<String>,
}

impl HealthCheck {
    /// Returns instance of HealthCheck
    pub fn new(name: String, status: Status) -> Self {
        Self {
            name,
            status,
            message: None,
            checks: Vec::new(),
            version: None,
            commit: None,
        }
    }
}

/// Status
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Status {
    /// Pass
    Pass,
    /// Fail
    Fail,
}
