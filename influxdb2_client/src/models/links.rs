//! Links

use serde::{Deserialize, Serialize};

/// Links
#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Links {
    /// URI of resource.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next: Option<String>,
    /// URI of resource.
    #[serde(rename = "self")]
    pub self_: String,
    /// URI of resource.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prev: Option<String>,
}

impl Links {
    /// Returns instance of Links
    pub fn new(self_: String) -> Self {
        Self {
            self_,
            ..Default::default()
        }
    }
}
