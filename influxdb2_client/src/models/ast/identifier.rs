//! Idendifier

use serde::{Deserialize, Serialize};

/// A valid Flux identifier
#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Identifier {
    /// Type of AST node
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub r#type: Option<String>,
    /// Identifier Name
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

impl Identifier {
    /// A valid Flux identifier
    pub fn new() -> Self {
        Self::default()
    }
}
