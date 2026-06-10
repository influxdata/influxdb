//! Tests for role CatalogOp implementations.

use super::*;
use crate::catalog::versions::v3::ops::test_util::{apply_batch, test_catalog};
use crate::format::RecordBatch;

#[test]
fn create_role_op() {
    let mut catalog = test_catalog();
    let mut batch = RecordBatch::new(1);
    let op = CreateRoleOp::prepare(
        &CreateRoleArgs {
            name: "admin".to_string(),
            description: Some("Administrator role".to_string()),
            permissions: vec![],
            is_required_role: false,
            created_at: 1234567890,
        },
        &catalog,
        &mut batch,
    )
    .unwrap();
    apply_batch(&batch, &mut catalog);
    let role = op.output(&catalog);
    assert_eq!(role.name.as_str(), "admin");
    assert_eq!(role.created_at, 1234567890);
}

#[test]
fn update_role_op() {
    let mut catalog = test_catalog();

    // Create role first
    let mut batch = RecordBatch::new(1);
    let create_op = CreateRoleOp::prepare(
        &CreateRoleArgs {
            name: "editor".to_string(),
            description: None,
            permissions: vec![],
            is_required_role: false,
            created_at: 1000,
        },
        &catalog,
        &mut batch,
    )
    .unwrap();
    apply_batch(&batch, &mut catalog);
    let role_id = create_op.output(&catalog).id;

    // Update role name
    let mut batch = RecordBatch::new(1);
    let update_op = UpdateRoleOp::prepare(
        &UpdateRoleArgs {
            role_id,
            name: Some("content-editor".to_string()),
            description: Some("Content editor role".to_string()),
            updated_at: 2000,
        },
        &catalog,
        &mut batch,
    )
    .unwrap();
    apply_batch(&batch, &mut catalog);
    let role = update_op.output(&catalog);
    assert_eq!(role.name.as_str(), "content-editor");
}

#[test]
fn delete_role_op() {
    let mut catalog = test_catalog();

    // Create role first
    let mut batch = RecordBatch::new(1);
    let create_op = CreateRoleOp::prepare(
        &CreateRoleArgs {
            name: "temp-role".to_string(),
            description: None,
            permissions: vec![],
            is_required_role: false,
            created_at: 1000,
        },
        &catalog,
        &mut batch,
    )
    .unwrap();
    apply_batch(&batch, &mut catalog);
    let role_id = create_op.output(&catalog).id;

    // Delete role
    let mut batch = RecordBatch::new(1);
    DeleteRoleOp::prepare(
        &DeleteRoleArgs {
            role_id,
            deleted_at: 2000,
        },
        &catalog,
        &mut batch,
    )
    .unwrap();
    apply_batch(&batch, &mut catalog);

    // Verify role is deleted
    assert!(catalog.roles.get_by_id(&role_id).is_none());
}
