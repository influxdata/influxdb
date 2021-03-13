use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Resource {
    #[serde(rename = "type")]
    pub _type: Type,
    /// If ID is set that is a permission for a specific resource. if it is not
    /// set it is a permission for all resources of that resource type.
    #[serde(rename = "id", skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// Optional name of the resource if the resource has a name field.
    #[serde(rename = "name", skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// If orgID is set that is a permission for all resources owned my that
    /// org. if it is not set it is a permission for all resources of that
    /// resource type.
    #[serde(rename = "orgID", skip_serializing_if = "Option::is_none")]
    pub org_id: Option<String>,
    /// Optional name of the organization of the organization with orgID.
    #[serde(rename = "org", skip_serializing_if = "Option::is_none")]
    pub org: Option<String>,
}

impl Resource {
    pub fn new(_type: Type) -> Resource {
        Resource {
            _type,
            id: None,
            name: None,
            org_id: None,
            org: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum Type {
    #[serde(rename = "authorizations")]
    Authorizations,
    #[serde(rename = "buckets")]
    Buckets,
    #[serde(rename = "dashboards")]
    Dashboards,
    #[serde(rename = "orgs")]
    Orgs,
    #[serde(rename = "sources")]
    Sources,
    #[serde(rename = "tasks")]
    Tasks,
    #[serde(rename = "telegrafs")]
    Telegrafs,
    #[serde(rename = "users")]
    Users,
    #[serde(rename = "variables")]
    Variables,
    #[serde(rename = "scrapers")]
    Scrapers,
    #[serde(rename = "secrets")]
    Secrets,
    #[serde(rename = "labels")]
    Labels,
    #[serde(rename = "views")]
    Views,
    #[serde(rename = "documents")]
    Documents,
    #[serde(rename = "notificationRules")]
    NotificationRules,
    #[serde(rename = "notificationEndpoints")]
    NotificationEndpoints,
    #[serde(rename = "checks")]
    Checks,
    #[serde(rename = "dbrp")]
    Dbrp,
}
