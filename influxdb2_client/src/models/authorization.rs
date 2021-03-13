use serde::{Deserialize, Serialize};

/// Authorization to create
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Authorization {
    /// If inactive the token is inactive and requests using the token will be
    /// rejected.
    #[serde(rename = "status", skip_serializing_if = "Option::is_none")]
    pub status: Option<Status>,
    /// A description of the token.
    #[serde(rename = "description", skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(rename = "createdAt", skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
    #[serde(rename = "updatedAt", skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
    /// ID of org that authorization is scoped to.
    #[serde(rename = "orgID")]
    pub org_id: String,
    /// List of permissions for an auth.  An auth must have at least one
    /// Permission.
    #[serde(rename = "permissions")]
    pub permissions: Vec<crate::models::Permission>,
    #[serde(rename = "id", skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// Passed via the Authorization Header and Token Authentication type.
    #[serde(rename = "token", skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    /// ID of user that created and owns the token.
    #[serde(rename = "userID", skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,
    /// Name of user that created and owns the token.
    #[serde(rename = "user", skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    /// Name of the org token is scoped to.
    #[serde(rename = "org", skip_serializing_if = "Option::is_none")]
    pub org: Option<String>,
    #[serde(rename = "links", skip_serializing_if = "Option::is_none")]
    pub links: Option<crate::models::AuthorizationAllOfLinks>,
}

impl Authorization {
    pub fn new(org_id: String, permissions: Vec<crate::models::Permission>) -> Authorization {
        Authorization {
            status: None,
            description: None,
            created_at: None,
            updated_at: None,
            org_id,
            permissions,
            id: None,
            token: None,
            user_id: None,
            user: None,
            org: None,
            links: None,
        }
    }
}

/// If inactive the token is inactive and requests using the token will be
/// rejected.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum Status {
    #[serde(rename = "active")]
    Active,
    #[serde(rename = "inactive")]
    Inactive,
}

///
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AuthorizationAllOfLinks {
    /// URI of resource.
    #[serde(rename = "self", skip_serializing_if = "Option::is_none")]
    pub _self: Option<String>,
    /// URI of resource.
    #[serde(rename = "user", skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
}

impl AuthorizationAllOfLinks {
    pub fn new() -> AuthorizationAllOfLinks {
        AuthorizationAllOfLinks {
            _self: None,
            user: None,
        }
    }
}
