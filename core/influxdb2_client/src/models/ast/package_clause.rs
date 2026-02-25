//! PackageClause

use serde::{Deserialize, Serialize};

/// Defines a package identifier
#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct PackageClause {
    /// Type of AST node
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub r#type: Option<String>,
    /// Package name
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<crate::models::ast::Identifier>,
}

impl PackageClause {
    /// Defines a package identifier
    pub fn new() -> Self {
        Self::default()
    }
}
