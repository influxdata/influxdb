//! Users

use serde::{Deserialize, Serialize};

/// User Schema
#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct User {
    /// User ID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// User oauth token id
    #[serde(rename = "oauthID", skip_serializing_if = "Option::is_none")]
    pub oauth_id: Option<String>,
    /// User name
    pub name: String,
    /// If inactive the user is inactive.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<Status>,
    /// User links
    #[serde(skip_serializing_if = "Option::is_none")]
    pub links: Option<crate::models::UserLinks>,
}

impl User {
    /// Returns instance of user
    pub fn new(name: String) -> Self {
        Self {
            name,
            ..Default::default()
        }
    }
}

/// If inactive the user is inactive.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Status {
    /// User is active
    Active,
    /// User is inactive
    Inactive,
}

/// User links
#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct UserLinks {
    /// User link to Self
    #[serde(rename = "self", skip_serializing_if = "Option::is_none")]
    pub self_: Option<String>,
}

impl UserLinks {
    /// Returns instance of UserLinks
    pub fn new() -> Self {
        Self::default()
    }
}

/// List of Users
#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Users {
    /// List of user links
    #[serde(skip_serializing_if = "Option::is_none")]
    pub links: Option<crate::models::UsersLinks>,
    /// List of users
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub users: Vec<crate::models::User>,
}

impl Users {
    /// Returns instance of Users
    pub fn new() -> Self {
        Self::default()
    }
}

/// UsersLinks
#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct UsersLinks {
    /// Users Link to Self
    #[serde(rename = "self", skip_serializing_if = "Option::is_none")]
    pub self_: Option<String>,
}

impl UsersLinks {
    /// Returns instance of UsersLinks
    pub fn new() -> Self {
        Self::default()
    }
}
