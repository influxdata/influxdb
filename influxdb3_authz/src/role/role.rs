use influxdb3_id::RoleId;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::Permission;

const ROLE_NAME_MAX_LENGTH: usize = 64;
const ROLE_NAME_ALLOWED_SPECIAL_CHARS: &[char] = &[' ', '-', '_'];
const ROLE_DESCRIPTION_MAX_LENGTH: usize = 512;
const ROLE_DESCRIPTION_ALLOWED_SPECIAL_CHARS: &[char] = &[
    ' ', '-', '_', '/', '.', ',', '!', '?', '@', '#', '%', '*', '(', ')',
];

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Role {
    pub id: RoleId,
    pub name: RoleName,
    pub description: RoleDescription,
    pub permissions: Vec<Permission>,
    pub is_required_role: bool,
    pub created_at: i64,
    pub updated_at: i64,
}

impl Role {
    pub fn new(
        id: RoleId,
        name: RoleName,
        description: RoleDescription,
        permissions: Vec<Permission>,
        is_required_role: bool,
        created_at: i64,
    ) -> Self {
        Self {
            id,
            name,
            description,
            permissions,
            is_required_role,
            created_at,
            updated_at: created_at,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoleName(String);

impl RoleName {
    pub fn new(name: &str) -> Result<Self, NewRoleNameError> {
        if name.is_empty() {
            return Err(NewRoleNameError::Empty);
        }

        if name.len() > ROLE_NAME_MAX_LENGTH {
            return Err(NewRoleNameError::TooLong);
        }

        if !name
            .chars()
            .all(|c| c.is_alphanumeric() || ROLE_NAME_ALLOWED_SPECIAL_CHARS.contains(&c))
        {
            return Err(NewRoleNameError::InvalidCharacters(format_allowed_chars(
                ROLE_NAME_ALLOWED_SPECIAL_CHARS,
            )));
        }

        Ok(Self(name.to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_inner(self) -> String {
        self.0
    }
}

impl std::fmt::Display for RoleName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Error)]
pub enum NewRoleNameError {
    #[error("role name cannot be empty")]
    Empty,
    #[error("role name exceeds maximum length of {ROLE_NAME_MAX_LENGTH} characters")]
    TooLong,
    #[error("role name contains invalid characters: only alphanumeric and [{0}] are allowed")]
    InvalidCharacters(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoleDescription(String);

impl RoleDescription {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn new(description: &str) -> Result<Self, NewRoleDescriptionError> {
        if description.is_empty() {
            return Err(NewRoleDescriptionError::Empty);
        }

        if description.len() > ROLE_DESCRIPTION_MAX_LENGTH {
            return Err(NewRoleDescriptionError::TooLong);
        }

        if !description
            .chars()
            .all(|c| c.is_alphanumeric() || ROLE_DESCRIPTION_ALLOWED_SPECIAL_CHARS.contains(&c))
        {
            return Err(NewRoleDescriptionError::InvalidCharacters(
                format_allowed_chars(ROLE_DESCRIPTION_ALLOWED_SPECIAL_CHARS),
            ));
        }

        Ok(Self(description.to_string()))
    }

    pub fn into_inner(self) -> String {
        self.0
    }

    pub fn new_unchecked(description: String) -> Self {
        Self(description)
    }
}

#[derive(Debug, Clone, Error)]
pub enum NewRoleDescriptionError {
    #[error("role description cannot be empty")]
    Empty,
    #[error("role description exceeds maximum length of {ROLE_DESCRIPTION_MAX_LENGTH} characters")]
    TooLong,
    #[error(
        "role description contains invalid characters: only alphanumeric and [{0}] are allowed"
    )]
    InvalidCharacters(String),
}

fn format_allowed_chars(chars: &[char]) -> String {
    chars
        .iter()
        .map(|c| format!("'{c}'"))
        .collect::<Vec<_>>()
        .join(", ")
}
