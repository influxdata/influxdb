//! Authorization
//!
//! Auth tokens for InfluxDB

use serde::{Deserialize, Serialize};

/// Authorization to create
#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Authorization {
    /// If inactive the token is inactive and requests using the token will be
    /// rejected.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<Status>,
    /// A description of the token.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Auth created_at
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
    /// Auth updated_at
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
    /// ID of org that authorization is scoped to.
    #[serde(rename = "orgID")]
    pub org_id: String,
    /// List of permissions for an auth. An auth must have at least one
    /// Permission.
    pub permissions: Vec<crate::models::Permission>,
    /// Auth ID.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// Passed via the Authorization Header and Token Authentication type.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    /// ID of user that created and owns the token.
    #[serde(rename = "userID", skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,
    /// Name of user that created and owns the token.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    /// Name of the org token is scoped to.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub org: Option<String>,
    /// Links
    #[serde(skip_serializing_if = "Option::is_none")]
    pub links: Option<crate::models::AuthorizationAllOfLinks>,
}

impl Authorization {
    /// Returns an Authorization with the given orgID and permissions
    pub fn new(org_id: String, permissions: Vec<crate::models::Permission>) -> Self {
        Self {
            org_id,
            permissions,
            ..Default::default()
        }
    }
}

/// If inactive the token is inactive and requests using the token will be
/// rejected.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Status {
    /// Token is active.
    Active,
    /// Token is inactive.
    Inactive,
}

/// AuthorizationAllOfLinks
#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct AuthorizationAllOfLinks {
    /// Self
    #[serde(rename = "self", skip_serializing_if = "Option::is_none")]
    pub self_: Option<String>,
    /// User
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
}

impl AuthorizationAllOfLinks {
    /// Return an instance of AuthorizationAllOfLinks
    pub fn new() -> Self {
        Self::default()
    }
}
