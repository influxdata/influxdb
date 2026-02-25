//! Duration

use serde::{Deserialize, Serialize};

/// Duration : A pair consisting of length of time and the unit of time
/// measured. It is the atomic unit from which all duration literals are
/// composed.
#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Duration {
    /// Type of AST node
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub r#type: Option<String>,
    /// Duration Magnitude
    #[serde(skip_serializing_if = "Option::is_none")]
    pub magnitude: Option<i32>,
    /// Duration unit
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unit: Option<String>,
}

impl Duration {
    /// A pair consisting of length of time and the unit of time measured. It is
    /// the atomic unit from which all duration literals are composed.
    pub fn new() -> Self {
        Self::default()
    }
}
