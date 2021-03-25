//! Labels

use serde::{Deserialize, Serialize};

/// Label
#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Label {
    /// Label ID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// Org ID
    #[serde(rename = "orgID", skip_serializing_if = "Option::is_none")]
    pub org_id: Option<String>,
    /// Label name
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Key/Value pairs associated with this label. Keys can be removed by
    /// sending an update with an empty value.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<::std::collections::HashMap<String, String>>,
}

impl Label {
    /// Returns instance of Label
    pub fn new() -> Self {
        Self::default()
    }
}
