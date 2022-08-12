//! PropertyKey

use serde::{Deserialize, Serialize};

/// Key value pair
#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct PropertyKey {
    /// Type of AST node
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub r#type: Option<String>,
    /// PropertyKey name
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// PropertyKey value
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
}

impl PropertyKey {
    /// Returns an instance of PropertyKey
    pub fn new() -> Self {
        Self::default()
    }
}
