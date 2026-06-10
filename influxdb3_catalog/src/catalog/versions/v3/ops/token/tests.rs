use influxdb3_authz::DatabaseActions;

use crate::CatalogError;
use crate::catalog::versions::v3::ops::CatalogOp;
use crate::catalog::versions::v3::ops::database::{CreateDatabaseArgs, CreateDatabaseOp};
use crate::catalog::versions::v3::ops::test_util::{apply_batch, test_catalog};
use crate::format::RecordBatch;
use crate::format::records::types::{
    Actions as WireActions, Permission as WirePermission, ResourceIdentifier as WireResourceIdent,
    ResourceType as WireResourceType,
};

use super::{
    CreateAdminTokenArgs, CreateAdminTokenOp, CreateResourceScopedTokenArgs,
    CreateResourceScopedTokenOp, DeleteTokenArgs, DeleteTokenOp, RegenerateAdminTokenArgs,
    RegenerateAdminTokenOp,
};

#[test]
fn prepare_admin_token_lifecycle() {
    let mut catalog = test_catalog();

    assert_eq!(catalog.tokens.repo().len(), 0);

    // Create
    let mut batch = RecordBatch::new(1);
    let op = CreateAdminTokenOp::prepare(
        &CreateAdminTokenArgs {
            name: "admin".to_string(),
            hash: vec![1, 2, 3],
            created_at: 1000,
            updated_at: None,
            expiry: None,
            description: None,
            created_by: None,
            updated_by: None,
        },
        &catalog,
        &mut batch,
    )
    .unwrap();
    assert_eq!(batch.len(), 1);
    apply_batch(&batch, &mut catalog);

    let token = op.output(&catalog);
    assert_eq!(token.name.as_ref(), "admin");
    assert_eq!(catalog.tokens.repo().len(), 1);

    // Regenerate
    batch = RecordBatch::new(1);
    let op = RegenerateAdminTokenOp::prepare(
        &RegenerateAdminTokenArgs {
            token_id: token.id,
            new_hash: vec![4, 5, 6],
            updated_at: 2000,
        },
        &catalog,
        &mut batch,
    )
    .unwrap();
    apply_batch(&batch, &mut catalog);

    let _token = op.output(&catalog);
    assert!(catalog.tokens.hash_to_info(vec![4, 5, 6]).is_some());
    assert!(catalog.tokens.hash_to_info(vec![1, 2, 3]).is_none());

    // Delete
    batch = RecordBatch::new(1);
    let op = DeleteTokenOp::prepare(
        &DeleteTokenArgs {
            token_name: "admin".to_string(),
        },
        &catalog,
        &mut batch,
    )
    .unwrap();
    apply_batch(&batch, &mut catalog);

    let deleted = op.output(&catalog);
    assert_eq!(deleted.name.as_ref(), "admin");
    assert_eq!(catalog.tokens.repo().len(), 0);
}

#[test]
fn prepare_create_admin_token_duplicate() {
    let mut catalog = test_catalog();

    let mut batch = RecordBatch::new(1);
    CreateAdminTokenOp::prepare(
        &CreateAdminTokenArgs {
            name: "admin".to_string(),
            hash: vec![1, 2, 3],
            created_at: 1000,
            updated_at: None,
            expiry: None,
            description: None,
            created_by: None,
            updated_by: None,
        },
        &catalog,
        &mut batch,
    )
    .unwrap();
    apply_batch(&batch, &mut catalog);

    batch = RecordBatch::new(1);
    let result = CreateAdminTokenOp::prepare(
        &CreateAdminTokenArgs {
            name: "admin".to_string(),
            hash: vec![4, 5, 6],
            created_at: 2000,
            updated_at: None,
            expiry: None,
            description: None,
            created_by: None,
            updated_by: None,
        },
        &catalog,
        &mut batch,
    );
    assert!(matches!(
        result,
        Err(CatalogError::TokenNameAlreadyExists(_))
    ));
}

#[test]
fn prepare_delete_token_not_found() {
    let catalog = test_catalog();
    let mut batch = RecordBatch::new(1);

    let result = DeleteTokenOp::prepare(
        &DeleteTokenArgs {
            token_name: "nonexistent".to_string(),
        },
        &catalog,
        &mut batch,
    );
    assert!(matches!(result, Err(CatalogError::NotFound(_))));
}

#[test]
fn prepare_create_resource_scoped_token() {
    let mut catalog = test_catalog();

    // Need a database for permission resolution
    let mut batch = RecordBatch::new(1);
    CreateDatabaseOp::prepare(
        &CreateDatabaseArgs {
            name: "mydb".to_string(),
            retention_period: None,
        },
        &catalog,
        &mut batch,
    )
    .unwrap();
    apply_batch(&batch, &mut catalog);

    batch = RecordBatch::new(1);
    let op = CreateResourceScopedTokenOp::prepare(
        &CreateResourceScopedTokenArgs {
            name: "db_reader".to_string(),
            hash: vec![10, 11, 12],
            created_at: 1000,
            updated_at: None,
            expiry: None,
            description: None,
            created_by: None,
            updated_by: None,
            permissions: vec![WirePermission {
                resource_type: WireResourceType::Database,
                resource_identifier: WireResourceIdent::Database(vec![0]),
                actions: WireActions::Database(DatabaseActions::READ),
                resource_names: vec![],
            }],
        },
        &catalog,
        &mut batch,
    )
    .unwrap();
    assert_eq!(batch.len(), 1);
    apply_batch(&batch, &mut catalog);

    let token = op.output(&catalog);
    assert_eq!(token.name.as_ref(), "db_reader");
    assert_eq!(token.permissions.len(), 1);
    let perm = &token.permissions[0];
    assert!(matches!(
        perm.resource_type,
        influxdb3_authz::ResourceType::Database
    ));
    assert!(matches!(
        perm.resource_identifier,
        influxdb3_authz::ResourceIdentifier::Database(ref ids) if ids.len() == 1
    ));
    assert!(matches!(
        perm.actions,
        influxdb3_authz::Actions::Database(_)
    ));
}
