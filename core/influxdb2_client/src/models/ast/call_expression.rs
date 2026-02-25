//! CallExpression

use serde::{Deserialize, Serialize};

/// Represents a function call
#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct CallExpression {
    /// Type of AST node
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub r#type: Option<String>,
    /// Callee
    #[serde(skip_serializing_if = "Option::is_none")]
    pub callee: Option<Box<crate::models::ast::Expression>>,
    /// Function arguments
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub arguments: Vec<crate::models::ast::Expression>,
}

impl CallExpression {
    /// Represents a function call
    pub fn new() -> Self {
        Self::default()
    }
}
