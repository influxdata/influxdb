use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Label {
    #[serde(rename = "id", skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(rename = "orgID", skip_serializing_if = "Option::is_none")]
    pub org_id: Option<String>,
    #[serde(rename = "name", skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Key/Value pairs associated with this label. Keys can be removed by
    /// sending an update with an empty value.
    #[serde(rename = "properties", skip_serializing_if = "Option::is_none")]
    pub properties: Option<::std::collections::HashMap<String, String>>,
}

impl Label {
    pub fn new() -> Label {
        Label {
            id: None,
            org_id: None,
            name: None,
            properties: None,
        }
    }
}
