//! Enterprise OAuth login identity records (record_ids e4-e5).

use std::sync::Arc;

use crate::catalog::versions::v3::events::CatalogEvent;
use crate::catalog::versions::v3::inner::InnerCatalog;
use crate::catalog::versions::v3::schema::user::LoginIdentityOAuth;
use crate::format::apply::ApplyError;
use crate::format::records::impl_bitcode_encoding;
use crate::format::{CatalogRecord, RecordFlags, RecordId, RegisteredRecord, record_ids};
use influxdb3_id::UserId;

/// Add an OAuth login identity to a user.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, bitcode::Encode, bitcode::Decode)]
pub struct CreateLoginIdentityOAuth {
    /// User ID.
    pub user_id: u64,
    /// OAuth provider ID.
    pub oauth_id: String,
    /// Creation timestamp in milliseconds.
    pub created_at: i64,
}

impl CatalogRecord for CreateLoginIdentityOAuth {
    const ID: RecordId = record_ids::CREATE_LOGIN_IDENTITY_OAUTH;
    const FLAGS: RecordFlags = RecordFlags::none();
    const NAME: &'static str = "CreateLoginIdentityOAuth";

    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        let user_id = UserId::new(self.user_id);
        let identity = LoginIdentityOAuth {
            oauth_id: Arc::from(self.oauth_id.as_str()),
            created_at: self.created_at,
        };
        catalog
            .users
            .add_login_identity_oauth(user_id, identity)
            .map_err(|e| {
                ApplyError(format!(
                    "{}: add OAuth identity (user_id={}, oauth_id='{}'): {e}",
                    Self::NAME,
                    self.user_id,
                    self.oauth_id
                ))
            })
    }

    fn event(&self) -> CatalogEvent {
        CatalogEvent::LoginIdentityCreated {
            user_id: UserId::new(self.user_id),
        }
    }
}

inventory::submit! {
    RegisteredRecord::new::<CreateLoginIdentityOAuth>()
}

/// Delete the OAuth login identity for a user.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, bitcode::Encode, bitcode::Decode)]
pub struct DeleteLoginIdentityOAuth {
    /// User ID.
    pub user_id: u64,
}

impl CatalogRecord for DeleteLoginIdentityOAuth {
    const ID: RecordId = record_ids::DELETE_LOGIN_IDENTITY_OAUTH;
    const FLAGS: RecordFlags = RecordFlags::none();
    const NAME: &'static str = "DeleteLoginIdentityOAuth";

    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        let user_id = UserId::new(self.user_id);
        catalog
            .users
            .delete_login_identity_oauth(&user_id)
            .map_err(|e| {
                ApplyError(format!(
                    "{}: delete OAuth identity (user_id={}): {e}",
                    Self::NAME,
                    self.user_id
                ))
            })
    }

    fn event(&self) -> CatalogEvent {
        CatalogEvent::LoginIdentityDeleted {
            user_id: UserId::new(self.user_id),
        }
    }
}

inventory::submit! {
    RegisteredRecord::new::<DeleteLoginIdentityOAuth>()
}

impl_bitcode_encoding!(CreateLoginIdentityOAuth, DeleteLoginIdentityOAuth);

#[cfg(test)]
mod tests;
