use std::sync::Arc;

use influxdb3_authz::permissions::PermissionDetailsSpec;
use influxdb3_id::TokenId;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct PermissionDetails {
    pub resource_type: String,
    pub resource_identifier: Vec<String>,
    pub actions: Vec<String>,
}

impl From<PermissionDetailsSpec> for PermissionDetails {
    fn from(value: PermissionDetailsSpec) -> Self {
        Self {
            resource_type: value.resource_type,
            resource_identifier: value.resource_identifier,
            actions: value.actions,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct CreateDatabaseTokenDetails {
    pub token_id: TokenId,
    pub name: Arc<str>,
    pub hash: Vec<u8>,
    pub created_at: i64,
    pub expiry: Option<i64>,
    pub permissions: Vec<PermissionDetails>,
}

impl From<CreateDatabaseTokenDetails> for CreateTokenDetails {
    fn from(value: CreateDatabaseTokenDetails) -> Self {
        Self {
            token_id: value.token_id,
            name: value.name,
            hash: value.hash,
            created_at: value.created_at,
            expiry: value.expiry,
            permissions: value.permissions,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct CreateTokenDetails {
    pub token_id: TokenId,
    pub name: Arc<str>,
    pub hash: Vec<u8>,
    pub created_at: i64,
    pub expiry: Option<i64>,
    pub permissions: Vec<PermissionDetails>,
}
