//! MemberExpression

use serde::{Deserialize, Serialize};

/// Represents accessing a property of an object
#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct MemberExpression {
    /// Type of AST node
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub r#type: Option<String>,
    /// Member object
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object: Option<crate::models::ast::Expression>,
    /// Member Property
    #[serde(skip_serializing_if = "Option::is_none")]
    pub property: Option<crate::models::ast::PropertyKey>,
}

impl MemberExpression {
    /// Represents accessing a property of an object
    pub fn new() -> Self {
        Self::default()
    }
}
