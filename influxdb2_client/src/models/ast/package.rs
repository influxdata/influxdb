//! Package

use crate::models::File;
use serde::{Deserialize, Serialize};

/// Represents a complete package source tree.
#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Package {
    /// Type of AST node
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub r#type: Option<String>,
    /// Package import path
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// Package name
    #[serde(skip_serializing_if = "Option::is_none")]
    pub package: Option<String>,
    /// Package files
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub files: Vec<File>,
}

impl Package {
    /// Represents a complete package source tree.
    pub fn new() -> Self {
        Self::default()
    }
}
