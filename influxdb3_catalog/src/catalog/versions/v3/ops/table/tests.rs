use iox_time::Time;

use crate::CatalogError;
use crate::catalog::versions::v3::deletes::DeletionScope;
use crate::catalog::versions::v3::ops::CatalogOp;
use crate::catalog::versions::v3::ops::test_util::{apply_batch, create_db, test_catalog};
use crate::format::RecordBatch;
use crate::format::records::CreateTable;
use crate::format::records::types::{FieldFamilyMode, RetentionPeriod};

use super::{HardDeleteTableArgs, HardDeleteTableOp, SoftDeleteTableArgs, SoftDeleteTableOp};

#[test]
fn prepare_soft_delete_table_rejects_internal_db() {
    let catalog = test_catalog();
    let mut batch = RecordBatch::new(1);

    let result = SoftDeleteTableOp::prepare(
        &SoftDeleteTableArgs {
            db_name: "_internal".to_string(),
            table_name: "cpu".to_string(),
            deletion_time: Time::from_timestamp_nanos(1000),
            hard_delete_time: None,
            hard_delete_scope: None,
        },
        &catalog,
        &mut batch,
    );
    assert!(matches!(
        result,
        Err(CatalogError::CannotModifyInternalDatabase)
    ));
}

#[test]
fn prepare_soft_delete_table() {
    let mut catalog = test_catalog();
    create_db(&mut catalog, "mydb");

    let mut batch = RecordBatch::new(1);
    batch.push(&CreateTable {
        database_id: 0,
        database_name: "mydb".to_string(),
        table_name: "cpu".to_string(),
        table_id: 0,
        retention_period: RetentionPeriod::Indefinite,
        field_family_mode: FieldFamilyMode::Auto,
    });
    apply_batch(&batch, &mut catalog);

    batch = RecordBatch::new(1);
    let op = SoftDeleteTableOp::prepare(
        &SoftDeleteTableArgs {
            db_name: "mydb".to_string(),
            table_name: "cpu".to_string(),
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

    let table = op.output(&catalog);
    assert!(table.deleted);
}

#[test]
fn prepare_hard_delete_table() {
    let mut catalog = test_catalog();
    create_db(&mut catalog, "mydb");

    let mut batch = RecordBatch::new(1);
    batch.push(&CreateTable {
        database_id: 0,
        database_name: "mydb".to_string(),
        table_name: "cpu".to_string(),
        table_id: 0,
        retention_period: RetentionPeriod::Indefinite,
        field_family_mode: FieldFamilyMode::Auto,
    });
    apply_batch(&batch, &mut catalog);
    let db = catalog.databases.get_by_name("mydb").unwrap();
    let db_id = db.id;
    let table = db.tables.get_by_name("cpu").unwrap();

    batch = RecordBatch::new(1);
    SoftDeleteTableOp::prepare(
        &SoftDeleteTableArgs {
            db_name: "mydb".to_string(),
            table_name: "cpu".to_string(),
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
    let op = HardDeleteTableOp::prepare(
        &HardDeleteTableArgs {
            db_id,
            table_id: table.table_id,
        },
        &catalog,
        &mut batch,
    )
    .unwrap();
    assert_eq!(batch.len(), 1);
    apply_batch(&batch, &mut catalog);

    let deleted_table = op.output(&catalog);
    assert!(deleted_table.deleted);

    let db = catalog.databases.get_by_id(&db_id).unwrap();
    assert!(db.table_definition_by_id(&table.table_id).is_none());
}
