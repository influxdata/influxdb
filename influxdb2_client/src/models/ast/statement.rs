//! Statement

use serde::{Deserialize, Serialize};

/// Expression Statement
#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Statement {
    /// Type of AST node
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub r#type: Option<String>,
    /// Raw source text
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    /// Statement identitfier
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<crate::models::ast::Identifier>,
    /// Initial Value
    #[serde(skip_serializing_if = "Option::is_none")]
    pub init: Option<crate::models::ast::Expression>,
    /// Member
    #[serde(skip_serializing_if = "Option::is_none")]
    pub member: Option<crate::models::ast::MemberExpression>,
    /// Expression
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expression: Option<crate::models::ast::Expression>,
    /// Argument
    #[serde(skip_serializing_if = "Option::is_none")]
    pub argument: Option<crate::models::ast::Expression>,
    /// Assignment
    #[serde(skip_serializing_if = "Option::is_none")]
    pub assignment: Option<crate::models::ast::VariableAssignment>,
}

impl Statement {
    /// Returns an instance of Statement
    pub fn new() -> Self {
        Self::default()
    }
}
