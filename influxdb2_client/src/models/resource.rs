//! Resources

use serde::{Deserialize, Serialize};

/// Construct a resource
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
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
#[serde(rename_all = "camelCase")]
pub enum Type {
    /// Authorizations
    Authorizations,
    /// Buckets
    Buckets,
    /// Dashboards
    Dashboards,
    /// Organizations
    Orgs,
    /// Sources
    Sources,
    /// Tasks
    Tasks,
    /// Telegrafs
    Telegrafs,
    /// Users
    Users,
    /// Variables
    Variables,
    /// Scrapers
    Scrapers,
    /// Secrets
    Secrets,
    /// Labels
    Labels,
    /// Views
    Views,
    /// Documents
    Documents,
    /// Notification Rules
    NotificationRules,
    /// Notification Endpoints
    NotificationEndpoints,
    /// Checks
    Checks,
    /// DBRP
    Dbrp,
}
