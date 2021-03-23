//! Resources

use serde::{Deserialize, Serialize};

/// Construct a resource
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Resource {
    /// Resource Type
    #[serde(rename = "type")]
    pub r#type: Type,
    /// If ID is set that is a permission for a specific resource. if it is not
    /// set it is a permission for all resources of that resource type.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// Optional name of the resource if the resource has a name field.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// If orgID is set that is a permission for all resources owned my that
    /// org. if it is not set it is a permission for all resources of that
    /// resource type.
    #[serde(rename = "orgID", skip_serializing_if = "Option::is_none")]
    pub org_id: Option<String>,
    /// Optional name of the organization of the organization with orgID.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub org: Option<String>,
}

impl Resource {
    /// Returns instance of Resource
    pub fn new(r#type: Type) -> Self {
        Self {
            r#type,
            id: None,
            name: None,
            org_id: None,
            org: None,
        }
    }
}

/// Resource Type
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum Type {
    /// Authorizations
    #[serde(rename = "authorizations")]
    Authorizations,
    /// Buckets
    #[serde(rename = "buckets")]
    Buckets,
    /// Dashboards
    #[serde(rename = "dashboards")]
    Dashboards,
    /// Organizations
    #[serde(rename = "orgs")]
    Orgs,
    /// Sources
    #[serde(rename = "sources")]
    Sources,
    /// Tasks
    #[serde(rename = "tasks")]
    Tasks,
    /// Telegrafs
    #[serde(rename = "telegrafs")]
    Telegrafs,
    /// Users
    #[serde(rename = "users")]
    Users,
    /// Variables
    #[serde(rename = "variables")]
    Variables,
    /// Scrapers
    #[serde(rename = "scrapers")]
    Scrapers,
    /// Secrets
    #[serde(rename = "secrets")]
    Secrets,
    /// Labels
    #[serde(rename = "labels")]
    Labels,
    /// Views
    #[serde(rename = "views")]
    Views,
    /// Documents
    #[serde(rename = "documents")]
    Documents,
    /// Notification Rules
    #[serde(rename = "notificationRules")]
    NotificationRules,
    /// Notification Endpoints
    #[serde(rename = "notificationEndpoints")]
    NotificationEndpoints,
    /// Checks
    #[serde(rename = "checks")]
    Checks,
    /// DBRP
    #[serde(rename = "dbrp")]
    Dbrp,
}
