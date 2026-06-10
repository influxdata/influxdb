use crate::CatalogError;
use crate::catalog::versions::v3::ops::CatalogOp;
use crate::catalog::versions::v3::ops::test_util::{apply_batch, create_db, test_catalog};
use crate::catalog::versions::v3::schema::node::NodeSpec;
use crate::catalog::versions::v3::schema::trigger::{
    TriggerSettings, TriggerSpecificationDefinition,
};
use crate::format::RecordBatch;

use super::{
    CreateTriggerArgs, CreateTriggerOp, DeleteTriggerArgs, DeleteTriggerOp, DisableTriggerArgs,
    DisableTriggerOp, EnableTriggerArgs, EnableTriggerOp,
};

fn create_trigger_args(disabled: bool) -> CreateTriggerArgs {
    CreateTriggerArgs {
        db_name: "mydb".to_string(),
        trigger_name: "my_trigger".to_string(),
        plugin_filename: "plugin.wasm".to_string(),
        node_spec: NodeSpec::All,
        trigger_specification: TriggerSpecificationDefinition::AllTablesWalWrite,
        trigger_settings: TriggerSettings::default(),
        trigger_arguments: None,
        disabled,
    }
}

#[test]
fn prepare_create_trigger() {
    let mut catalog = test_catalog();
    create_db(&mut catalog, "mydb");

    let mut batch = RecordBatch::new(1);
    let op = CreateTriggerOp::prepare(&create_trigger_args(false), &catalog, &mut batch).unwrap();
    assert_eq!(batch.len(), 1);

    apply_batch(&batch, &mut catalog);

    let trigger = op.output(&catalog);
    assert_eq!(trigger.trigger_name.as_ref(), "my_trigger");
    assert!(!trigger.disabled);
}

#[test]
fn prepare_create_trigger_duplicate() {
    let mut catalog = test_catalog();
    create_db(&mut catalog, "mydb");

    let mut batch = RecordBatch::new(1);
    CreateTriggerOp::prepare(&create_trigger_args(false), &catalog, &mut batch).unwrap();
    apply_batch(&batch, &mut catalog);

    batch = RecordBatch::new(1);
    let result = CreateTriggerOp::prepare(&create_trigger_args(false), &catalog, &mut batch);
    assert!(matches!(result, Err(CatalogError::AlreadyExists)));
}

#[test]
fn prepare_delete_trigger_force() {
    let mut catalog = test_catalog();
    create_db(&mut catalog, "mydb");

    let mut batch = RecordBatch::new(1);
    CreateTriggerOp::prepare(&create_trigger_args(false), &catalog, &mut batch).unwrap();
    apply_batch(&batch, &mut catalog);

    batch = RecordBatch::new(1);
    let op = DeleteTriggerOp::prepare(
        &DeleteTriggerArgs {
            db_name: "mydb".to_string(),
            trigger_name: "my_trigger".to_string(),
            force: true,
        },
        &catalog,
        &mut batch,
    )
    .unwrap();
    assert_eq!(batch.len(), 1);
    apply_batch(&batch, &mut catalog);

    let deleted = op.output(&catalog);
    assert_eq!(deleted.trigger_name.as_ref(), "my_trigger");
}

#[test]
fn prepare_delete_running_trigger_without_force() {
    let mut catalog = test_catalog();
    create_db(&mut catalog, "mydb");

    let mut batch = RecordBatch::new(1);
    CreateTriggerOp::prepare(&create_trigger_args(false), &catalog, &mut batch).unwrap();
    apply_batch(&batch, &mut catalog);

    batch = RecordBatch::new(1);
    let result = DeleteTriggerOp::prepare(
        &DeleteTriggerArgs {
            db_name: "mydb".to_string(),
            trigger_name: "my_trigger".to_string(),
            force: false,
        },
        &catalog,
        &mut batch,
    );
    assert!(matches!(
        result,
        Err(CatalogError::ProcessingEngineTriggerRunning { .. })
    ));
}

#[test]
fn prepare_enable_disable_trigger() {
    let mut catalog = test_catalog();
    create_db(&mut catalog, "mydb");

    // Create enabled trigger
    let mut batch = RecordBatch::new(1);
    CreateTriggerOp::prepare(&create_trigger_args(false), &catalog, &mut batch).unwrap();
    apply_batch(&batch, &mut catalog);

    // Disable it
    batch = RecordBatch::new(1);
    let op = DisableTriggerOp::prepare(
        &DisableTriggerArgs {
            db_name: "mydb".to_string(),
            trigger_name: "my_trigger".to_string(),
        },
        &catalog,
        &mut batch,
    )
    .unwrap();
    apply_batch(&batch, &mut catalog);

    let trigger = op.output(&catalog);
    assert!(trigger.disabled);

    // Enable it
    batch = RecordBatch::new(1);
    let op = EnableTriggerOp::prepare(
        &EnableTriggerArgs {
            db_name: "mydb".to_string(),
            trigger_name: "my_trigger".to_string(),
        },
        &catalog,
        &mut batch,
    )
    .unwrap();
    apply_batch(&batch, &mut catalog);

    let trigger = op.output(&catalog);
    assert!(!trigger.disabled);
}

#[test]
fn prepare_enable_already_enabled() {
    let mut catalog = test_catalog();
    create_db(&mut catalog, "mydb");

    let mut batch = RecordBatch::new(1);
    CreateTriggerOp::prepare(&create_trigger_args(false), &catalog, &mut batch).unwrap();
    apply_batch(&batch, &mut catalog);

    batch = RecordBatch::new(1);
    let result = EnableTriggerOp::prepare(
        &EnableTriggerArgs {
            db_name: "mydb".to_string(),
            trigger_name: "my_trigger".to_string(),
        },
        &catalog,
        &mut batch,
    );
    assert!(matches!(result, Err(CatalogError::TriggerAlreadyEnabled)));
}

#[test]
fn prepare_disable_already_disabled() {
    let mut catalog = test_catalog();
    create_db(&mut catalog, "mydb");

    let mut batch = RecordBatch::new(1);
    CreateTriggerOp::prepare(&create_trigger_args(true), &catalog, &mut batch).unwrap();
    apply_batch(&batch, &mut catalog);

    batch = RecordBatch::new(1);
    let result = DisableTriggerOp::prepare(
        &DisableTriggerArgs {
            db_name: "mydb".to_string(),
            trigger_name: "my_trigger".to_string(),
        },
        &catalog,
        &mut batch,
    );
    assert!(matches!(result, Err(CatalogError::TriggerAlreadyDisabled)));
}
