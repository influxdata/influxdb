//! ImportDeclaration

use serde::{Deserialize, Serialize};

/// Declares a package import
#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ImportDeclaration {
    /// Type of AST node
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub r#type: Option<String>,
    /// Import Identifier
    #[serde(rename = "as", skip_serializing_if = "Option::is_none")]
    pub r#as: Option<crate::models::ast::Identifier>,
    /// Import Path
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<crate::models::ast::StringLiteral>,
}

impl ImportDeclaration {
    /// Declares a package import
    pub fn new() -> Self {
        Self::default()
    }
}
