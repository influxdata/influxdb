//! Permissions

use serde::{Deserialize, Serialize};

/// Permissions for a resource
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Permission {
    /// Access Type
    pub action: Action,
    /// Resource object
    pub resource: crate::models::Resource,
}

impl Permission {
    /// Return instance of Permission
    pub fn new(action: Action, resource: crate::models::Resource) -> Self {
        Self { action, resource }
    }
}

/// Allowed Permission Action
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Action {
    /// Read access
    Read,
    /// Write access
    Write,
}
