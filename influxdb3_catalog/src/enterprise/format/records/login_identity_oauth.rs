//! Enterprise OAuth login identity records (record_ids e4-e5).

use std::sync::Arc;

use influxdb3_catalog_macros::catalog_record;

use crate::catalog::versions::v3::events::CatalogEvent;
use crate::catalog::versions::v3::inner::InnerCatalog;
use crate::catalog::versions::v3::schema::user::LoginIdentityOAuth;
use crate::format::apply::ApplyError;
use crate::format::{CatalogRecord, RecordApply, record_ids};
use influxdb3_id::UserId;

/// Add an OAuth login identity to a user.
#[catalog_record(id = record_ids::CREATE_LOGIN_IDENTITY_OAUTH, shape = 0x7bd5ec8d)]
pub struct CreateLoginIdentityOAuth {
    /// User ID.
    pub user_id: u64,
    /// OAuth provider ID.
    pub oauth_id: String,
    /// Creation timestamp in milliseconds.
    pub created_at: i64,
}

impl RecordApply for CreateLoginIdentityOAuth {
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

/// Delete the OAuth login identity for a user.
#[catalog_record(id = record_ids::DELETE_LOGIN_IDENTITY_OAUTH, shape = 0x12f9b977)]
#[derive(Copy)]
pub struct DeleteLoginIdentityOAuth {
    /// User ID.
    pub user_id: u64,
}

impl RecordApply for DeleteLoginIdentityOAuth {
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

#[cfg(test)]
mod tests;
