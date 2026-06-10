//! Tests for user CatalogOp implementations.

use super::*;
use crate::catalog::versions::v3::ops::test_util::{apply_batch, test_catalog};
use crate::format::RecordBatch;

#[test]
fn create_user_op() {
    let mut catalog = test_catalog();
    let mut batch = RecordBatch::new(1);
    let op = CreateUserOp::prepare(
        &CreateUserArgs {
            display_name: Some("Test User".to_string()),
            created_at: 1234567890,
        },
        &catalog,
        &mut batch,
    )
    .unwrap();
    apply_batch(&batch, &mut catalog);
    let user = op.output(&catalog);
    assert_eq!(user.display_name.as_deref(), Some("Test User"));
    assert_eq!(user.created_at, 1234567890);
    assert_eq!(user.updated_at, 1234567890);
    assert_eq!(user.deleted_at, None);
}

#[test]
fn update_user_display_name_op() {
    let mut catalog = test_catalog();

    // Create user first
    let mut batch = RecordBatch::new(1);
    let create_op = CreateUserOp::prepare(
        &CreateUserArgs {
            display_name: Some("Original Name".to_string()),
            created_at: 1000,
        },
        &catalog,
        &mut batch,
    )
    .unwrap();
    apply_batch(&batch, &mut catalog);
    let user_id = create_op.output(&catalog).id;

    // Update display name
    let mut batch = RecordBatch::new(1);
    let update_op = UpdateUserDisplayNameOp::prepare(
        &UpdateUserDisplayNameArgs {
            user_id,
            display_name: Some("New Name".to_string()),
            updated_at: 2000,
        },
        &catalog,
        &mut batch,
    )
    .unwrap();
    apply_batch(&batch, &mut catalog);
    let user = update_op.output(&catalog);
    assert_eq!(user.display_name.as_deref(), Some("New Name"));
}

#[test]
fn delete_user_op() {
    let mut catalog = test_catalog();

    // Create user first
    let mut batch = RecordBatch::new(1);
    let create_op = CreateUserOp::prepare(
        &CreateUserArgs {
            display_name: None,
            created_at: 1000,
        },
        &catalog,
        &mut batch,
    )
    .unwrap();
    apply_batch(&batch, &mut catalog);
    let user_id = create_op.output(&catalog).id;

    // Delete user
    let mut batch = RecordBatch::new(1);
    DeleteUserOp::prepare(
        &DeleteUserArgs {
            user_id,
            deleted_at: 2000,
        },
        &catalog,
        &mut batch,
    )
    .unwrap();
    apply_batch(&batch, &mut catalog);

    // Verify user is deleted
    let user = catalog.users.get_by_id(&user_id).unwrap();
    assert!(user.is_deleted());
}
