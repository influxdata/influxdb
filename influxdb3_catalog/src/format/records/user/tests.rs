//! Tests for user records.

use std::sync::Arc;
use uuid::Uuid;

use crate::catalog::versions::v3::inner::InnerCatalog;
use crate::format::CatalogRecord;
use crate::format::records::assert_roundtrip;

use super::*;

fn test_catalog() -> InnerCatalog {
    InnerCatalog::new(Arc::from("test"), Uuid::nil())
}

// ---------------------------------------------------------------------------
// Roundtrip tests
// ---------------------------------------------------------------------------

#[test]
fn create_user_roundtrip() {
    assert_roundtrip!(
        CreateUser {
            user_id: 1,
            display_name: Some("Test User".to_string()),
            created_at: 1234567890,
        },
        "0601010954657374205573657202d2029649"
    );
}

#[test]
fn create_user_no_display_name_roundtrip() {
    assert_roundtrip!(
        CreateUser {
            user_id: 2,
            display_name: None,
            created_at: 1234567890,
        },
        "06020002d2029649"
    );
}

#[test]
fn update_user_display_name_roundtrip() {
    assert_roundtrip!(
        UpdateUserDisplayName {
            user_id: 1,
            display_name: Some("New Name".to_string()),
            updated_at: 1234567890,
        },
        "060101084e6577204e616d6502d2029649"
    );
}

#[test]
fn delete_user_roundtrip() {
    assert_roundtrip!(
        DeleteUser {
            user_id: 1,
            deleted_at: 1234567890,
        },
        "060102d2029649"
    );
}

#[test]
fn restore_user_roundtrip() {
    assert_roundtrip!(
        RestoreUser {
            user_id: 1,
            display_name: Some("Restored User".to_string()),
            restored_at: 1234567890,
        },
        "0601010d526573746f726564205573657202d2029649"
    );
}

#[test]
fn create_login_identity_username_password_roundtrip() {
    assert_roundtrip!(
        CreateLoginIdentityUsernamePassword {
            user_id: 1,
            username: "testuser".to_string(),
            password_hash: "$2b$12$hash".to_string(),
            requires_password_reset: false,
            created_at: 1234567890,
        },
        "06010874657374757365720b24326224313224686173680002d2029649"
    );
}

#[test]
fn update_login_identity_password_hash_roundtrip() {
    assert_roundtrip!(
        UpdateLoginIdentityPasswordHash {
            user_id: 1,
            password_hash: "$2b$12$newhash".to_string(),
            updated_at: 1234567890,
        },
        "06010e243262243132246e65776861736802d2029649"
    );
}

#[test]
fn update_login_identity_requires_password_reset_roundtrip() {
    assert_roundtrip!(
        UpdateLoginIdentityRequiresPasswordReset {
            user_id: 1,
            requires_password_reset: true,
            updated_at: 1234567890,
        },
        "06010102d2029649"
    );
}

#[test]
fn delete_login_identity_username_password_roundtrip() {
    assert_roundtrip!(DeleteLoginIdentityUsernamePassword { user_id: 1 }, "0601");
}

#[test]
fn create_refresh_token_roundtrip() {
    assert_roundtrip!(
        CreateRefreshToken {
            user_id: 1,
            token_hash: "sha256hash".to_string(),
            created_at: 1234567890,
            expires_at: 2234567890,
        },
        "06010a7368613235366861736802d202964902d2cc3085"
    );
}

#[test]
fn revoke_refresh_token_roundtrip() {
    assert_roundtrip!(
        RevokeRefreshToken {
            user_id: 1,
            token_hash: "sha256hash".to_string(),
            revoked_at: 1234567890,
        },
        "06010a7368613235366861736802d2029649"
    );
}

#[test]
fn revoke_all_refresh_tokens_for_user_roundtrip() {
    assert_roundtrip!(
        RevokeAllRefreshTokensForUser {
            user_id: 1,
            revoked_at: 1234567890,
        },
        "060102d2029649"
    );
}

#[test]
fn update_user_roles_roundtrip() {
    assert_roundtrip!(
        UpdateUserRoles {
            user_id: 1,
            role_ids: vec![1, 2, 3],
            updated_at: 1234567890,
        },
        "06010306063902d2029649"
    );
}

// ---------------------------------------------------------------------------
// Apply tests
// ---------------------------------------------------------------------------

#[test]
fn apply_create_user() {
    let mut catalog = test_catalog();
    CreateUser {
        user_id: 1,
        display_name: Some("Test User".to_string()),
        created_at: 1234567890,
    }
    .apply(&mut catalog)
    .unwrap();

    let user = catalog
        .users
        .get_by_id(&influxdb3_id::UserId::new(1))
        .expect("user should exist");
    assert_eq!(user.id, influxdb3_id::UserId::new(1));
    assert_eq!(user.display_name.as_deref(), Some("Test User"));
    assert_eq!(user.created_at, 1234567890);
}

#[test]
fn apply_user_lifecycle() {
    let mut catalog = test_catalog();

    // Create user
    CreateUser {
        user_id: 1,
        display_name: Some("Test User".to_string()),
        created_at: 1000,
    }
    .apply(&mut catalog)
    .unwrap();

    // Add login identity
    CreateLoginIdentityUsernamePassword {
        user_id: 1,
        username: "testuser".to_string(),
        password_hash: "$2b$12$hash".to_string(),
        requires_password_reset: false,
        created_at: 1000,
    }
    .apply(&mut catalog)
    .unwrap();

    let user = catalog
        .users
        .get_by_username("testuser")
        .expect("user should be findable by username");
    assert_eq!(user.id, influxdb3_id::UserId::new(1));

    // Update display name
    UpdateUserDisplayName {
        user_id: 1,
        display_name: Some("New Name".to_string()),
        updated_at: 2000,
    }
    .apply(&mut catalog)
    .unwrap();

    let user = catalog
        .users
        .get_by_id(&influxdb3_id::UserId::new(1))
        .unwrap();
    assert_eq!(user.display_name.as_deref(), Some("New Name"));

    // Delete user
    DeleteUser {
        user_id: 1,
        deleted_at: 3000,
    }
    .apply(&mut catalog)
    .unwrap();

    let user = catalog
        .users
        .get_by_id(&influxdb3_id::UserId::new(1))
        .unwrap();
    assert!(user.is_deleted());
}
