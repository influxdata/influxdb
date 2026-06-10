use std::sync::Arc;

use uuid::Uuid;

use crate::catalog::versions::v3::inner::InnerCatalog;
use crate::format::CatalogRecord;
use crate::format::records::assert_roundtrip;
use crate::format::records::token::{CreateAdminToken, DeleteToken, RegenerateAdminToken};

/// Helper to create a test catalog.
fn test_catalog() -> InnerCatalog {
    InnerCatalog::new(Arc::from("test"), Uuid::nil())
}

#[test]
fn create_admin_token_round_trip() {
    assert_roundtrip!(
        CreateAdminToken {
            token_id: 1,
            name: "admin".to_string(),
            hash: vec![1, 2, 3, 4],
            created_at: 1234567890,
            updated_at: None,
            expiry: None,
            description: None,
            created_by: None,
            updated_by: None,
        },
        "06010561646d696e0404790402d20296490000000000"
    );
}

#[test]
fn create_admin_token_with_metadata_round_trip() {
    assert_roundtrip!(
        CreateAdminToken {
            token_id: 2,
            name: "svc".to_string(),
            hash: vec![9, 8, 7],
            created_at: 1_700_000_000_000,
            updated_at: Some(1_700_000_005_000),
            expiry: Some(1_800_000_000_000),
            description: Some("service token".to_string()),
            created_by: Some(1),
            updated_by: Some(1),
        },
        "06020373766303028907000068e5cf8b0100000100887be5cf8b010000010000505c18a3010000010d7365727669636520746f6b656e010601010601"
    );
}

#[test]
fn regenerate_admin_token_round_trip() {
    assert_roundtrip!(
        RegenerateAdminToken {
            token_id: 1,
            hash: vec![5, 6, 7, 8],
            updated_at: Some(9876543210),
        },
        "0601040265870100ea16b04c02000000"
    );
}

#[test]
fn delete_token_round_trip() {
    assert_roundtrip!(DeleteToken { token_id: 99 }, "0663");
}

#[test]
fn record_ids() {
    assert_eq!(CreateAdminToken::ID.raw(), 19);
    assert_eq!(RegenerateAdminToken::ID.raw(), 20);
    assert_eq!(DeleteToken::ID.raw(), 21);

    assert!(!CreateAdminToken::FLAGS.is_upgrade_safe());
    assert!(!RegenerateAdminToken::FLAGS.is_upgrade_safe());
    assert!(!DeleteToken::FLAGS.is_upgrade_safe());
}

// ---------------------------------------------------------------------------
// Apply tests
// ---------------------------------------------------------------------------

#[test]
fn apply_token_lifecycle() {
    let mut catalog = test_catalog();

    assert_eq!(catalog.tokens.repo().len(), 0);

    // Create admin token
    CreateAdminToken {
        token_id: 1,
        name: "admin".to_string(),
        hash: vec![1, 2, 3],
        created_at: 1000,
        updated_at: None,
        expiry: None,
        description: None,
        created_by: None,
        updated_by: None,
    }
    .apply(&mut catalog)
    .unwrap();

    assert_eq!(catalog.tokens.repo().len(), 1);
    assert!(catalog.tokens.repo().get_by_name("admin").is_some());
    assert!(catalog.tokens.hash_to_info(vec![1, 2, 3]).is_some());

    // Regenerate with new hash
    RegenerateAdminToken {
        token_id: 1,
        hash: vec![4, 5, 6],
        updated_at: Some(2000),
    }
    .apply(&mut catalog)
    .unwrap();

    // New hash works, old hash doesn't
    assert!(catalog.tokens.hash_to_info(vec![4, 5, 6]).is_some());
    assert!(catalog.tokens.hash_to_info(vec![1, 2, 3]).is_none());

    // Delete
    DeleteToken { token_id: 1 }.apply(&mut catalog).unwrap();

    assert_eq!(catalog.tokens.repo().len(), 0);
    assert!(catalog.tokens.repo().get_by_name("admin").is_none());
}
