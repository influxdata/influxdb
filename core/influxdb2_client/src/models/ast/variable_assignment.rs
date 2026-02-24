//! VariableAssignment

use serde::{Deserialize, Serialize};

/// Represents the declaration of a variable
#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct VariableAssignment {
    /// Type of AST node
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub r#type: Option<String>,
    /// Variable Identifier
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<crate::models::ast::Identifier>,
    /// Variable initial value
    #[serde(skip_serializing_if = "Option::is_none")]
    pub init: Option<crate::models::ast::Expression>,
}

impl VariableAssignment {
    /// Represents the declaration of a variable
    pub fn new() -> Self {
        Self::default()
    }
}
