//! Links

use serde::{Deserialize, Serialize};

/// Links
#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Links {
    /// Next link
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next: Option<String>,
    /// Link to self
    #[serde(rename = "self")]
    pub self_: String,
    /// Previous Link
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prev: Option<String>,
}

impl Links {
    /// Returns list of Links
    pub fn new(self_: String) -> Self {
        Self {
            self_,
            ..Default::default()
        }
    }
}
