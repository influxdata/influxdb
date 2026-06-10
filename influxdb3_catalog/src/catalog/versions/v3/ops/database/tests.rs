use std::time::Duration;

use iox_time::Time;

use crate::CatalogError;
use crate::catalog::versions::v3::deletes::DeletionScope;
use crate::catalog::versions::v3::ops::CatalogOp;
use crate::catalog::versions::v3::ops::test_util::{apply_batch, create_db, test_catalog};
use crate::format::RecordBatch;

use super::{
    CreateDatabaseArgs, CreateDatabaseOp, HardDeleteDatabaseArgs, HardDeleteDatabaseOp,
    SoftDeleteDatabaseArgs, SoftDeleteDatabaseOp,
};

#[test]
fn prepare_create_database() {
    let catalog = test_catalog();
    let mut batch = RecordBatch::new(1);

    let op = CreateDatabaseOp::prepare(
        &CreateDatabaseArgs {
            name: "mydb".to_string(),
            retention_period: None,
        },
        &catalog,
        &mut batch,
    )
    .unwrap();
    assert_eq!(batch.len(), 1);

    let mut catalog = catalog;
    apply_batch(&batch, &mut catalog);

    let db = op.output(&catalog);
    assert_eq!(db.name().as_ref(), "mydb");
}

#[test]
fn prepare_create_database_with_retention() {
    let catalog = test_catalog();
    let mut batch = RecordBatch::new(1);

    CreateDatabaseOp::prepare(
        &CreateDatabaseArgs {
            name: "mydb".to_string(),
            retention_period: Some(Duration::from_secs(86400)),
        },
        &catalog,
        &mut batch,
    )
    .unwrap();

    let mut catalog = catalog;
    apply_batch(&batch, &mut catalog);

    let db = catalog.databases.get_by_name("mydb").unwrap();
    assert!(matches!(
        db.retention_period,
        crate::catalog::versions::v3::schema::retention::RetentionPeriod::Duration(d)
        if d == Duration::from_secs(86400)
    ));
}

#[test]
fn prepare_create_database_duplicate() {
    let mut catalog = test_catalog();
    create_db(&mut catalog, "mydb");

    let mut batch = RecordBatch::new(1);
    let result = CreateDatabaseOp::prepare(
        &CreateDatabaseArgs {
            name: "mydb".to_string(),
            retention_period: None,
        },
        &catalog,
        &mut batch,
    );
    assert!(matches!(result, Err(CatalogError::AlreadyExists)));
}

#[test]
fn prepare_soft_delete_database() {
    let mut catalog = test_catalog();
    create_db(&mut catalog, "mydb");

    let mut batch = RecordBatch::new(1);
    let op = SoftDeleteDatabaseOp::prepare(
        &SoftDeleteDatabaseArgs {
            database_name: "mydb".to_string(),
            deletion_time: Time::from_timestamp_nanos(1000),
            hard_delete_time: Some(Time::from_timestamp_nanos(2000)),
            hard_delete_scope: Some(DeletionScope::DataAndCatalog),
        },
        &catalog,
        &mut batch,
    )
    .unwrap();
    assert_eq!(batch.len(), 1);

    apply_batch(&batch, &mut catalog);

    let db = op.output(&catalog);
    assert!(db.deleted);
}

#[test]
fn prepare_soft_delete_database_not_found() {
    let catalog = test_catalog();
    let mut batch = RecordBatch::new(1);

    let result = SoftDeleteDatabaseOp::prepare(
        &SoftDeleteDatabaseArgs {
            database_name: "nonexistent".to_string(),
            deletion_time: Time::from_timestamp_nanos(1000),
            hard_delete_time: None,
            hard_delete_scope: None,
        },
        &catalog,
        &mut batch,
    );
    assert!(matches!(result, Err(CatalogError::NotFound(_))));
}

#[test]
fn prepare_re_soft_delete_database_updates_hard_delete_time() {
    let mut catalog = test_catalog();
    let db_id = create_db(&mut catalog, "mydb");

    // First soft delete — apply renames the db
    let mut batch = RecordBatch::new(1);
    SoftDeleteDatabaseOp::prepare(
        &SoftDeleteDatabaseArgs {
            database_name: "mydb".to_string(),
            deletion_time: Time::from_timestamp_nanos(1000),
            hard_delete_time: Some(Time::from_timestamp_nanos(2000)),
            hard_delete_scope: Some(DeletionScope::DataAndCatalog),
        },
        &catalog,
        &mut batch,
    )
    .unwrap();
    apply_batch(&batch, &mut catalog);

    // Re-soft delete by the renamed name, with a different hard_delete_time, succeeds
    let renamed = catalog
        .databases
        .get_by_id(&db_id)
        .unwrap()
        .name()
        .to_string();
    batch = RecordBatch::new(1);
    SoftDeleteDatabaseOp::prepare(
        &SoftDeleteDatabaseArgs {
            database_name: renamed.clone(),
            deletion_time: Time::from_timestamp_nanos(1500),
            hard_delete_time: Some(Time::from_timestamp_nanos(5000)),
            hard_delete_scope: Some(DeletionScope::DataAndCatalog),
        },
        &catalog,
        &mut batch,
    )
    .unwrap();
    apply_batch(&batch, &mut catalog);
    assert_eq!(
        catalog
            .databases
            .get_by_id(&db_id)
            .unwrap()
            .hard_delete_time,
        Some(Time::from_timestamp_nanos(5000))
    );

    // Re-soft delete with the same hard_delete_time is a no-op rejection
    batch = RecordBatch::new(1);
    let result = SoftDeleteDatabaseOp::prepare(
        &SoftDeleteDatabaseArgs {
            database_name: renamed,
            deletion_time: Time::from_timestamp_nanos(2000),
            hard_delete_time: Some(Time::from_timestamp_nanos(5000)),
            hard_delete_scope: Some(DeletionScope::DataAndCatalog),
        },
        &catalog,
        &mut batch,
    );
    assert!(matches!(result, Err(CatalogError::AlreadyDeleted(_))));
}

#[test]
fn prepare_hard_delete_database() {
    let mut catalog = test_catalog();
    let db_id = create_db(&mut catalog, "mydb");

    let mut batch = RecordBatch::new(1);
    SoftDeleteDatabaseOp::prepare(
        &SoftDeleteDatabaseArgs {
            database_name: "mydb".to_string(),
            deletion_time: Time::from_timestamp_nanos(1000),
            hard_delete_time: Some(Time::from_timestamp_nanos(2000)),
            hard_delete_scope: Some(DeletionScope::DataAndCatalog),
        },
        &catalog,
        &mut batch,
    )
    .unwrap();
    apply_batch(&batch, &mut catalog);

    batch = RecordBatch::new(1);
    let op = HardDeleteDatabaseOp::prepare(&HardDeleteDatabaseArgs { db_id }, &catalog, &mut batch)
        .unwrap();
    assert_eq!(batch.len(), 1);

    apply_batch(&batch, &mut catalog);

    let db = op.output(&catalog);
    assert!(db.deleted);

    assert!(catalog.databases.get_by_id(&db_id).is_none());
}
