use serde::{Deserialize, Serialize};

/// Whether a permission applies to all resources of a type or a specific one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResourceIdentifier<T> {
    All,
    Identifier(T),
}

impl<T: PartialEq> ResourceIdentifier<T> {
    /// Returns true if this identifier covers the required identifier.
    /// `All` covers anything. `Identifier` covers only an exact match.
    pub fn covers(&self, required: &ResourceIdentifier<T>) -> bool {
        match (self, required) {
            (ResourceIdentifier::All, _) => true,
            (ResourceIdentifier::Identifier(have), ResourceIdentifier::Identifier(need)) => {
                have == need
            }
            (ResourceIdentifier::Identifier(_), ResourceIdentifier::All) => false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DatabaseAction {
    Describe,
    Read,
    Write,
    Create,
    Delete,
    GrantUsage,
}

/// Actions that can be performed on token resources
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TokenAction {
    Read,
    Create,
    Delete,
    GrantUsage,
}

/// Actions that can be performed on user resources
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum UserAction {
    Read,
    Create,
    Update,
    Delete,
    GrantUsage,
}

/// Actions that can be performed on role resources
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RoleAction {
    Read,
    Create,
    Update,
    Delete,
}

/// Actions that can be performed on admin token resources
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AdminTokenAction {
    Create,
    Delete,
}
