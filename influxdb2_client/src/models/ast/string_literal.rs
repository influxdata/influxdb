//! StringLiteral

use serde::{Deserialize, Serialize};

/// Expressions begin and end with double quote marks
#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct StringLiteral {
    /// Type of AST node
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub r#type: Option<String>,
    /// StringLiteral Value
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
}

impl StringLiteral {
    /// Expressions begin and end with double quote marks
    pub fn new() -> Self {
        Self::default()
    }
}
