//! Organization

use serde::{Deserialize, Serialize};

/// Organization Schema
#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Organization {
    /// Links
    #[serde(skip_serializing_if = "Option::is_none")]
    pub links: Option<crate::models::OrganizationLinks>,
    /// Organization ID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// Organization Name
    pub name: String,
    /// Organization description
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Organization created timestamp
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
    /// Organization updated timestamp
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
    /// If inactive the organization is inactive.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<Status>,
}

impl Organization {
    /// Returns instance of Organization
    pub fn new(name: String) -> Self {
        Self {
            name,
            ..Default::default()
        }
    }
}

/// If inactive the organization is inactive.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Status {
    /// Organization is active
    Active,
    /// Organization is inactive
    Inactive,
}

/// Organization Links
#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct OrganizationLinks {
    /// Link to self
    #[serde(rename = "self", skip_serializing_if = "Option::is_none")]
    pub self_: Option<String>,
    /// Links to members
    #[serde(skip_serializing_if = "Option::is_none")]
    pub members: Option<String>,
    /// Links to owners
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owners: Option<String>,
    /// Links to labels
    #[serde(skip_serializing_if = "Option::is_none")]
    pub labels: Option<String>,
    /// Links to secrets
    #[serde(skip_serializing_if = "Option::is_none")]
    pub secrets: Option<String>,
    /// Links to buckets
    #[serde(skip_serializing_if = "Option::is_none")]
    pub buckets: Option<String>,
    /// Links to tasks
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tasks: Option<String>,
    /// Links to dashboards
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dashboards: Option<String>,
}

impl OrganizationLinks {
    /// Returns instance of Organization Links
    pub fn new() -> Self {
        Self::default()
    }
}

/// Organizations
#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct Organizations {
    /// Links
    #[serde(skip_serializing_if = "Option::is_none")]
    pub links: Option<crate::models::Links>,
    /// List of organizations
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub orgs: Vec<crate::models::Organization>,
}

impl Organizations {
    /// Returns instance of Organizations
    pub fn new() -> Self {
        Self::default()
    }
}
