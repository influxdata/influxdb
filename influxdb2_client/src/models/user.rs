use serde::{Deserialize, Serialize};

/// User Information
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct User {
    #[serde(rename = "id", skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(rename = "oauthID", skip_serializing_if = "Option::is_none")]
    pub oauth_id: Option<String>,
    #[serde(rename = "name")]
    pub name: String,
    /// If inactive the user is inactive.
    #[serde(rename = "status", skip_serializing_if = "Option::is_none")]
    pub status: Option<Status>,
    #[serde(rename = "links", skip_serializing_if = "Option::is_none")]
    pub links: Option<crate::models::UserLinks>,
}

impl User {
    pub fn new(name: String) -> User {
        User {
            id: None,
            oauth_id: None,
            name,
            status: None,
            links: None,
        }
    }
}

/// If inactive the user is inactive.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum Status {
    #[serde(rename = "active")]
    Active,
    #[serde(rename = "inactive")]
    Inactive,
}

///
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UserLinks {
    #[serde(rename = "self", skip_serializing_if = "Option::is_none")]
    pub _self: Option<String>,
}

impl UserLinks {
    pub fn new() -> UserLinks {
        UserLinks { _self: None }
    }
}

/// List of Users
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Users {
    #[serde(rename = "links", skip_serializing_if = "Option::is_none")]
    pub links: Option<crate::models::UsersLinks>,
    #[serde(rename = "users", skip_serializing_if = "Option::is_none")]
    pub users: Option<Vec<crate::models::User>>,
}

impl Users {
    pub fn new() -> Users {
        Users {
            links: None,
            users: None,
        }
    }
}

///
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UsersLinks {
    #[serde(rename = "self", skip_serializing_if = "Option::is_none")]
    pub _self: Option<String>,
}

impl UsersLinks {
    pub fn new() -> UsersLinks {
        UsersLinks { _self: None }
    }
}
