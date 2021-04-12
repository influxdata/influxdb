//! Health

use serde::{Deserialize, Serialize};

/// HealthCheck
#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthCheck {
    /// Name of the infludb instance
    pub name: String,
    /// Message
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// Checks
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub checks: Vec<crate::models::HealthCheck>,
    /// Status
    pub status: Option<Status>,
    /// Version
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    /// Commit
    #[serde(skip_serializing_if = "Option::is_none")]
    pub commit: Option<String>,
}

impl HealthCheck {
    /// Returns instance of HealthCheck
    pub fn new(name: String) -> Self {
        Self {
            name,
            ..Default::default()
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
