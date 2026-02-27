//! File

use serde::{Deserialize, Serialize};

/// Represents a source from a single file
#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct File {
    /// Type of AST node
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub r#type: Option<String>,
    /// The name of the file.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// PackageClause
    #[serde(skip_serializing_if = "Option::is_none")]
    pub package: Option<crate::models::ast::PackageClause>,
    /// A list of package imports
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub imports: Vec<crate::models::ast::ImportDeclaration>,
    /// List of Flux statements
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub body: Vec<crate::models::ast::Statement>,
}

impl File {
    /// Represents a source from a single file
    pub fn new() -> Self {
        Self::default()
    }
}
