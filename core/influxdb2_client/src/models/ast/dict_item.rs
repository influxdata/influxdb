//! DictItem

use serde::{Deserialize, Serialize};

/// A key/value pair in a dictionary
#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct DictItem {
    /// Type of AST node
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub r#type: Option<String>,
    /// Key
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<crate::models::ast::Expression>,
    /// Value
    #[serde(skip_serializing_if = "Option::is_none")]
    pub val: Option<crate::models::ast::Expression>,
}

impl DictItem {
    /// A key/value pair in a dictionary
    pub fn new() -> Self {
        Self::default()
    }
}
