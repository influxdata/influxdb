use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Organization {
    #[serde(rename = "links", skip_serializing_if = "Option::is_none")]
    pub links: Option<crate::models::OrganizationLinks>,
    #[serde(rename = "id", skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(rename = "name")]
    pub name: String,
    #[serde(rename = "description", skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(rename = "createdAt", skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
    #[serde(rename = "updatedAt", skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
    /// If inactive the organization is inactive.
    #[serde(rename = "status", skip_serializing_if = "Option::is_none")]
    pub status: Option<Status>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OrganizationLinks {
    /// URI of resource.
    #[serde(rename = "self", skip_serializing_if = "Option::is_none")]
    pub _self: Option<String>,
    /// URI of resource.
    #[serde(rename = "members", skip_serializing_if = "Option::is_none")]
    pub members: Option<String>,
    /// URI of resource.
    #[serde(rename = "owners", skip_serializing_if = "Option::is_none")]
    pub owners: Option<String>,
    /// URI of resource.
    #[serde(rename = "labels", skip_serializing_if = "Option::is_none")]
    pub labels: Option<String>,
    /// URI of resource.
    #[serde(rename = "secrets", skip_serializing_if = "Option::is_none")]
    pub secrets: Option<String>,
    /// URI of resource.
    #[serde(rename = "buckets", skip_serializing_if = "Option::is_none")]
    pub buckets: Option<String>,
    /// URI of resource.
    #[serde(rename = "tasks", skip_serializing_if = "Option::is_none")]
    pub tasks: Option<String>,
    /// URI of resource.
    #[serde(rename = "dashboards", skip_serializing_if = "Option::is_none")]
    pub dashboards: Option<String>,
}

impl Organization {
    pub fn new(name: String) -> Organization {
        Organization {
            links: None,
            id: None,
            name,
            description: None,
            created_at: None,
            updated_at: None,
            status: None,
        }
    }
}

/// If inactive the organization is inactive.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum Status {
    #[serde(rename = "active")]
    Active,
    #[serde(rename = "inactive")]
    Inactive,
}

impl OrganizationLinks {
    pub fn new() -> OrganizationLinks {
        OrganizationLinks {
            _self: None,
            members: None,
            owners: None,
            labels: None,
            secrets: None,
            buckets: None,
            tasks: None,
            dashboards: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Organizations {
    #[serde(rename = "links", skip_serializing_if = "Option::is_none")]
    pub links: Option<crate::models::Links>,
    #[serde(rename = "orgs", skip_serializing_if = "Option::is_none")]
    pub orgs: Option<Vec<crate::models::Organization>>,
}

impl Organizations {
    pub fn new() -> Organizations {
        Organizations {
            links: None,
            orgs: None,
        }
    }
}
