//! Labels

use serde::{Deserialize, Serialize};

/// Post create label request, to create a new label
#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LabelCreateRequest {
    /// Organisation ID
    #[serde(rename = "orgID")]
    pub org_id: String,
    /// Label name
    pub name: String,
    /// Key/Value pairs associated with this label.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<::std::collections::HashMap<String, String>>,
}

impl LabelCreateRequest {
    /// Return instance of LabelCreateRequest
    pub fn new(org_id: String, name: String) -> Self {
        Self {
            org_id,
            name,
            ..Default::default()
        }
    }
}

/// LabelResponse
#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LabelResponse {
    /// Label
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<crate::models::Label>,
    /// Links
    #[serde(skip_serializing_if = "Option::is_none")]
    pub links: Option<crate::models::Links>,
}

impl LabelResponse {
    /// Returns instance of LabelResponse
    pub fn new() -> Self {
        Self::default()
    }
}

///LabelsResponse
#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LabelsResponse {
    /// Labels
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub labels: Vec<crate::models::Label>,
    /// Links
    #[serde(skip_serializing_if = "Option::is_none")]
    pub links: Option<crate::models::Links>,
}

impl LabelsResponse {
    /// Returns List of Labels
    pub fn new() -> Self {
        Self::default()
    }
}

///LabelUpdateRequest
#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LabelUpdate {
    /// Name
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Key/Value pairs associated with this label.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<::std::collections::HashMap<String, String>>,
}

impl LabelUpdate {
    /// Returns an instance of LabelUpdate
    pub fn new() -> Self {
        Self::default()
    }
}

/// Label
#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
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
    /// Returns an instance of Label
    pub fn new() -> Self {
        Self::default()
    }
}
