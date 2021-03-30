//! CallExpression

use serde::{Deserialize, Serialize};

/// Represents a function call
#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct CallExpression {
    /// Type of AST node
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub _type: Option<String>,
    /// Callee
    #[serde(rename = "callee", skip_serializing_if = "Option::is_none")]
    pub callee: Option<Box<crate::models::ast::Expression>>,
    /// Function arguments
    #[serde(rename = "arguments", skip_serializing_if = "Option::is_none")]
    pub arguments: Option<Vec<crate::models::ast::Expression>>,
}

impl CallExpression {
    /// Represents a function call
    pub fn new() -> Self {
        Self::default()
    }
}
