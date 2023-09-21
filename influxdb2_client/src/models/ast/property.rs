//! Property

use serde::{Deserialize, Serialize};

/// The value associated with a key
#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Property {
    /// Type of AST node
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub r#type: Option<String>,
    /// Property Key
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<crate::models::ast::PropertyKey>,
    /// Property Value
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<crate::models::ast::Expression>,
}

impl Property {
    /// The value associated with a key
    pub fn new() -> Self {
        Self::default()
    }
}
