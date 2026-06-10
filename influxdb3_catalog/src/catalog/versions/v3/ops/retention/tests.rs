use std::time::Duration;

use crate::CatalogError;
use crate::catalog::versions::v3::ops::CatalogOp;
use crate::catalog::versions::v3::ops::test_util::{
    apply_batch, create_db, create_table, test_catalog,
};
use crate::catalog::versions::v3::schema::retention::RetentionPeriod;
use crate::format::RecordBatch;

use super::{
    ClearDbRetentionPeriodArgs, ClearDbRetentionPeriodOp, ClearTableRetentionPeriodArgs,
    ClearTableRetentionPeriodOp, SetDbRetentionPeriodArgs, SetDbRetentionPeriodOp,
    SetTableRetentionPeriodArgs, SetTableRetentionPeriodOp,
};

#[test]
fn prepare_set_and_clear_db_retention() {
    let mut catalog = test_catalog();
    create_db(&mut catalog, "mydb");

    // Set
    let mut batch = RecordBatch::new(1);
    let op = SetDbRetentionPeriodOp::prepare(
        &SetDbRetentionPeriodArgs {
            db_name: "mydb".to_string(),
            retention_period: Duration::from_secs(86400),
        },
        &catalog,
        &mut batch,
    )
    .unwrap();
    assert_eq!(batch.len(), 1);
    apply_batch(&batch, &mut catalog);

    let db = op.output(&catalog);
    assert!(matches!(
        db.retention_period,
        RetentionPeriod::Duration(d) if d == Duration::from_secs(86400)
    ));

    // Clear
    batch = RecordBatch::new(1);
    let op = ClearDbRetentionPeriodOp::prepare(
        &ClearDbRetentionPeriodArgs {
            db_name: "mydb".to_string(),
        },
        &catalog,
        &mut batch,
    )
    .unwrap();
    apply_batch(&batch, &mut catalog);

    let db = op.output(&catalog);
    assert!(matches!(db.retention_period, RetentionPeriod::Indefinite));
}

#[test]
fn prepare_set_db_retention_not_found() {
    let catalog = test_catalog();
    let mut batch = RecordBatch::new(1);

    let result = SetDbRetentionPeriodOp::prepare(
        &SetDbRetentionPeriodArgs {
            db_name: "nonexistent".to_string(),
            retention_period: Duration::from_secs(3600),
        },
        &catalog,
        &mut batch,
    );
    assert!(matches!(result, Err(CatalogError::DatabaseNotFound { .. })));
}

#[test]
fn prepare_set_and_clear_table_retention() {
    let mut catalog = test_catalog();
    create_db(&mut catalog, "mydb");
    create_table(&mut catalog, "mydb", "cpu");

    // Set
    let mut batch = RecordBatch::new(1);
    let op = SetTableRetentionPeriodOp::prepare(
        &SetTableRetentionPeriodArgs {
            db_name: "mydb".to_string(),
            table_name: "cpu".to_string(),
            retention_period: Duration::from_secs(7200),
        },
        &catalog,
        &mut batch,
    )
    .unwrap();
    assert_eq!(batch.len(), 1);
    apply_batch(&batch, &mut catalog);

    let table = op.output(&catalog);
    assert!(matches!(
        table.retention_period,
        RetentionPeriod::Duration(d) if d == Duration::from_secs(7200)
    ));

    // Clear
    batch = RecordBatch::new(1);
    let op = ClearTableRetentionPeriodOp::prepare(
        &ClearTableRetentionPeriodArgs {
            db_name: "mydb".to_string(),
            table_name: "cpu".to_string(),
        },
        &catalog,
        &mut batch,
    )
    .unwrap();
    apply_batch(&batch, &mut catalog);

    let table = op.output(&catalog);
    assert!(matches!(
        table.retention_period,
        RetentionPeriod::Indefinite
    ));
}

#[test]
fn prepare_set_table_retention_not_found() {
    let mut catalog = test_catalog();
    create_db(&mut catalog, "mydb");

    let mut batch = RecordBatch::new(1);
    let result = SetTableRetentionPeriodOp::prepare(
        &SetTableRetentionPeriodArgs {
            db_name: "mydb".to_string(),
            table_name: "nonexistent".to_string(),
            retention_period: Duration::from_secs(3600),
        },
        &catalog,
        &mut batch,
    );
    assert!(matches!(result, Err(CatalogError::TableNotFound { .. })));
}
