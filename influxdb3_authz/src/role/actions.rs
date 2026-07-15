use crate::{SystemActions, SystemResourceIdentifier};
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

/// Actions that can be performed on the system resource.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SystemAction {
    Read,
}

/// The set of system resources whose access is mediated by the `system` ABAC
/// resource type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SystemResource {
    Health,
    Metrics,
    Ping,
    Ready,
}

/// Error returned when converting from a `SystemResourceIdentifier` bitmap value
/// that does not correspond to a known [`SystemResource`] variant.
#[derive(Debug, Clone, Copy, thiserror::Error)]
#[error("unknown system resource identifier")]
pub struct UnknownSystemResourceError;

impl TryFrom<SystemResourceIdentifier> for SystemResource {
    type Error = UnknownSystemResourceError;

    fn try_from(id: SystemResourceIdentifier) -> Result<Self, Self::Error> {
        match id.as_u16() {
            SystemResourceIdentifier::HEALTH => Ok(SystemResource::Health),
            SystemResourceIdentifier::METRICS => Ok(SystemResource::Metrics),
            SystemResourceIdentifier::PING => Ok(SystemResource::Ping),
            SystemResourceIdentifier::READY => Ok(SystemResource::Ready),
            _ => Err(UnknownSystemResourceError),
        }
    }
}

impl SystemAction {
    pub fn from_bitmap(bits: SystemActions) -> Vec<SystemAction> {
        let mut out = Vec::new();
        if bits.contains(SystemActions::READ) {
            out.push(SystemAction::Read);
        }
        out
    }
}
