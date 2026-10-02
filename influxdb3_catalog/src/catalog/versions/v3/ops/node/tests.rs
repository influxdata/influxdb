use std::sync::Arc;

use influxdb3_wal::SnapshotSequenceNumber;
use iox_time::Time;
use uuid::Uuid;

use crate::CatalogError;
use crate::catalog::versions::v3::inner::InnerCatalog;
use crate::catalog::versions::v3::ops::CatalogOp;
use crate::catalog::versions::v3::ops::test_util::{apply_batch, test_catalog};
use crate::catalog::versions::v3::schema::node::{NodeMode, NodeState, RemovalAttestation};
use crate::format::records::{CreateQueryGroup, RegisterNode};
use crate::format::{FeatureLevel, RecordBatch, derive_feature_level, record_ids};

use super::{
    AckStopNodeArgs, AckStopNodeOp, RegisterNodeArgs, RegisterNodeOp, RemoveNodeArgs, RemoveNodeOp,
    RequestStopNodeArgs, RequestStopNodeOp, StopNodeArgs, StopNodeOp, UnregisterNodeArgs,
    UnregisterNodeOp,
};

fn register_args(node_id: &str, instance_id: &str) -> RegisterNodeArgs {
    RegisterNodeArgs {
        node_id: Arc::from(node_id),
        core_count: 4,
        mode: vec![NodeMode::Core],
        process_uuid: Uuid::nil(),
        instance_id: Arc::from(instance_id),
        conn_info: None,
        cli_params: None,
        registered_time: Time::from_timestamp_nanos(1000),
        row_delete_predicate_version: 0,
    }
}

fn stop_args(node_id: &str) -> StopNodeArgs {
    StopNodeArgs {
        node_id: Arc::from(node_id),
        stopped_time: Time::from_timestamp_nanos(2000),
        process_uuid: Uuid::nil(),
    }
}

#[test]
fn prepare_register_node() {
    let catalog = test_catalog();
    let mut batch = RecordBatch::new(1);

    let op =
        RegisterNodeOp::prepare(&register_args("node-a", "inst-1"), &catalog, &mut batch).unwrap();
    assert_eq!(batch.len(), 1);

    let mut catalog = catalog;
    apply_batch(&batch, &mut catalog);

    let node = op.output(&catalog);
    assert_eq!(node.node_id().as_ref(), "node-a");
    assert!(node.is_running());
    assert_eq!(node.core_count(), 4);
}

#[test]
fn prepare_register_node_re_register_stopped() {
    let mut catalog = test_catalog();
    let mut batch = RecordBatch::new(1);

    RegisterNodeOp::prepare(&register_args("node-a", "inst-1"), &catalog, &mut batch).unwrap();
    apply_batch(&batch, &mut catalog);

    batch = RecordBatch::new(1);
    StopNodeOp::prepare(&stop_args("node-a"), &catalog, &mut batch).unwrap();
    apply_batch(&batch, &mut catalog);

    batch = RecordBatch::new(1);
    let op =
        RegisterNodeOp::prepare(&register_args("node-a", "inst-2"), &catalog, &mut batch).unwrap();
    apply_batch(&batch, &mut catalog);

    let node = op.output(&catalog);
    assert!(node.is_running());
    assert_eq!(node.instance_id().as_ref(), "inst-2");
}

#[test]
fn prepare_register_node_re_register_running_same_instance() {
    let mut catalog = test_catalog();
    let mut batch = RecordBatch::new(1);

    RegisterNodeOp::prepare(&register_args("node-a", "inst-1"), &catalog, &mut batch).unwrap();
    apply_batch(&batch, &mut catalog);
    let original_id = catalog
        .nodes
        .get_by_name(&Arc::from("node-a"))
        .unwrap()
        .node_catalog_id();

    batch = RecordBatch::new(1);
    let op =
        RegisterNodeOp::prepare(&register_args("node-a", "inst-1"), &catalog, &mut batch).unwrap();
    apply_batch(&batch, &mut catalog);

    let node = op.output(&catalog);
    assert!(node.is_running());
    assert_eq!(node.instance_id().as_ref(), "inst-1");
    assert_eq!(node.node_catalog_id(), original_id);
}

#[test]
fn prepare_register_node_rejects_running_different_instance() {
    let mut catalog = test_catalog();
    let mut batch = RecordBatch::new(1);

    RegisterNodeOp::prepare(&register_args("node-a", "inst-1"), &catalog, &mut batch).unwrap();
    apply_batch(&batch, &mut catalog);

    batch = RecordBatch::new(1);
    let result = RegisterNodeOp::prepare(&register_args("node-a", "inst-2"), &catalog, &mut batch);
    assert!(matches!(result, Err(CatalogError::InvalidNodeRegistration)));
}

#[test]
fn prepare_stop_node() {
    let mut catalog = test_catalog();
    let mut batch = RecordBatch::new(1);

    RegisterNodeOp::prepare(&register_args("node-a", "inst-1"), &catalog, &mut batch).unwrap();
    apply_batch(&batch, &mut catalog);

    batch = RecordBatch::new(1);
    let op = StopNodeOp::prepare(&stop_args("node-a"), &catalog, &mut batch).unwrap();
    assert_eq!(batch.len(), 1);
    apply_batch(&batch, &mut catalog);

    let node = op.output(&catalog);
    assert!(node.state().is_stopped());
}

#[test]
fn prepare_stop_node_not_found() {
    let catalog = test_catalog();
    let mut batch = RecordBatch::new(1);

    let result = StopNodeOp::prepare(&stop_args("nonexistent"), &catalog, &mut batch);
    assert!(matches!(result, Err(CatalogError::NotFound(_))));
}

#[test]
fn prepare_stop_node_already_stopped() {
    let mut catalog = test_catalog();
    let mut batch = RecordBatch::new(1);

    RegisterNodeOp::prepare(&register_args("node-a", "inst-1"), &catalog, &mut batch).unwrap();
    apply_batch(&batch, &mut catalog);

    batch = RecordBatch::new(1);
    StopNodeOp::prepare(&stop_args("node-a"), &catalog, &mut batch).unwrap();
    apply_batch(&batch, &mut catalog);

    batch = RecordBatch::new(1);
    let result = StopNodeOp::prepare(&stop_args("node-a"), &catalog, &mut batch);
    assert!(matches!(
        result,
        Err(CatalogError::NodeAlreadyStopped { .. })
    ));
}

fn push_register(batch: &mut RecordBatch, id: u32, name: &str, feature_level: FeatureLevel) {
    batch.push(&RegisterNode {
        node_catalog_id: id,
        node_id: name.to_string(),
        instance_id: format!("inst-{name}"),
        registered_time_ns: 1_000,
        core_count: 4,
        mode: vec![],
        process_uuid: [0u8; 16],
        conn_info: None,
        cli_params: None,
        row_delete_predicate_version: 0,
        feature_level,
    });
}

#[test]
fn register_bundles_advance_when_consensus_exceeds_committed() {
    let mut catalog = test_catalog();
    // Lower committed below derived so the registration's consensus
    // computation exceeds it and triggers a bundled advance.
    let derived = derive_feature_level();
    catalog.committed_feature_level = FeatureLevel {
        core: derived.core - 1,
        enterprise: derived.enterprise.saturating_sub(1),
    };

    let mut batch = RecordBatch::new(1);
    RegisterNodeOp::prepare(&register_args("node-a", "inst-1"), &catalog, &mut batch).unwrap();

    let records = batch.as_slice();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].id(), record_ids::REGISTER_NODE);
    assert_eq!(records[1].id(), record_ids::ADVANCE_FEATURE_LEVEL);
}

#[test]
fn register_does_not_bundle_advance_when_already_at_committed() {
    // Default `test_catalog()` commits to the derived level — a fresh
    // registration finds consensus equal to committed, so no advance.
    let catalog = test_catalog();
    let mut batch = RecordBatch::new(1);

    RegisterNodeOp::prepare(&register_args("node-a", "inst-1"), &catalog, &mut batch).unwrap();
    assert_eq!(batch.as_slice().len(), 1);
    assert_eq!(batch.as_slice()[0].id(), record_ids::REGISTER_NODE);
}

#[test]
fn register_replaces_existing_slot_in_consensus() {
    // A stopped node's `feature_level` must not influence consensus
    // when it re-registers — the simulation drops the old slot and
    // replaces it with the registering node's local level. With one
    // legacy stopped node (feature_level = ZERO) and one
    // running node, re-registering the legacy at the local level should
    // trigger an advance because the running set's floor higher.
    let mut catalog = test_catalog();
    let derived = derive_feature_level();

    let mut setup = RecordBatch::new(1);
    push_register(&mut setup, 1, "legacy", FeatureLevel::ZERO);
    push_register(&mut setup, 2, "latest", derived);
    apply_batch(&setup, &mut catalog);

    let mut stop = RecordBatch::new(2);
    StopNodeOp::prepare(&stop_args("legacy"), &catalog, &mut stop).unwrap();
    apply_batch(&stop, &mut catalog);

    // Lower committed so the re-registration would actually advance it.
    catalog.committed_feature_level = FeatureLevel {
        core: derived.core - 1,
        enterprise: derived.enterprise.saturating_sub(1),
    };

    let mut batch = RecordBatch::new(3);
    RegisterNodeOp::prepare(
        &register_args("legacy", "inst-legacy"),
        &catalog,
        &mut batch,
    )
    .unwrap();

    let records = batch.as_slice();
    assert_eq!(
        records.len(),
        2,
        "advance must bundle when re-register replaces the floor slot"
    );
    assert_eq!(records[1].id(), record_ids::ADVANCE_FEATURE_LEVEL);
}

#[test]
fn stop_never_bundles_advance() {
    // Even when stopping this node would lift consensus past the
    // committed level, `StopNodeOp` does not bundle an
    // `AdvanceFeatureLevel` record.
    let mut catalog = test_catalog();
    let derived = derive_feature_level();

    let mut setup = RecordBatch::new(1);
    push_register(&mut setup, 1, "legacy", FeatureLevel::ZERO);
    push_register(&mut setup, 2, "modern", derived);
    apply_batch(&setup, &mut catalog);

    catalog.committed_feature_level = FeatureLevel {
        core: derived.core - 1,
        enterprise: derived.enterprise.saturating_sub(1),
    };

    let mut batch = RecordBatch::new(2);
    StopNodeOp::prepare(&stop_args("legacy"), &catalog, &mut batch).unwrap();

    let records = batch.as_slice();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].id(), record_ids::STOP_NODE);
}

// Registration args for a node that advertises an address, so it can join a
// query group.
fn register_args_with_mode(node_id: &str, instance_id: &str, mode: NodeMode) -> RegisterNodeArgs {
    RegisterNodeArgs {
        node_id: Arc::from(node_id),
        core_count: 4,
        mode: vec![mode],
        process_uuid: Uuid::nil(),
        instance_id: Arc::from(instance_id),
        conn_info: Some(format!("{node_id}:8181")),
        cli_params: None,
        registered_time: Time::from_timestamp_nanos(1000),
        row_delete_predicate_version: 0,
    }
}

fn request_stop_args(node_id: &str) -> RequestStopNodeArgs {
    RequestStopNodeArgs {
        node_id: Arc::from(node_id),
        stopped_time: Time::from_timestamp_nanos(2000),
        process_uuid: Uuid::nil(),
    }
}

fn ack_args(node_id: &str, final_snapshot_sequence: Option<u64>) -> AckStopNodeArgs {
    AckStopNodeArgs {
        node_id: Arc::from(node_id),
        ack_time: Time::from_timestamp_nanos(3000),
        process_uuid: Uuid::nil(),
        final_snapshot_sequence: final_snapshot_sequence.map(SnapshotSequenceNumber::new),
    }
}

fn remove_args(node_id: &str) -> RemoveNodeArgs {
    RemoveNodeArgs {
        node_id: Arc::from(node_id),
        requested_time: Time::from_timestamp_nanos(4000),
        attestation: RemovalAttestation::NotForced,
    }
}

fn unregister_args(node_id: &str) -> UnregisterNodeArgs {
    UnregisterNodeArgs {
        node_id: Arc::from(node_id),
        unregistered_time: Time::from_timestamp_nanos(5000),
    }
}

/// Walk a freshly-created catalog to the `Stopping` state via RegisterNode +
/// RequestStopNode.
fn register_then_request_stop(
    catalog: &mut crate::catalog::versions::v3::inner::InnerCatalog,
    node_id: &str,
) {
    let mut batch = RecordBatch::new(1);
    RegisterNodeOp::prepare(&register_args(node_id, "inst-1"), catalog, &mut batch).unwrap();
    apply_batch(&batch, catalog);

    let mut batch = RecordBatch::new(2);
    RequestStopNodeOp::prepare(&request_stop_args(node_id), catalog, &mut batch).unwrap();
    apply_batch(&batch, catalog);
}

/// Walk a freshly-created catalog through the full Register -> RequestStop ->
/// AckStop sequence so the node ends up in the terminal `Stopped` state.
fn register_request_stop_ack(
    catalog: &mut crate::catalog::versions::v3::inner::InnerCatalog,
    node_id: &str,
) {
    register_then_request_stop(catalog, node_id);
    let mut batch = RecordBatch::new(3);
    AckStopNodeOp::prepare(&ack_args(node_id, Some(7)), catalog, &mut batch).unwrap();
    apply_batch(&batch, catalog);
}

#[test]
fn request_stop_transitions_running_to_stopping() {
    let mut catalog = test_catalog();
    let mut batch = RecordBatch::new(1);
    RegisterNodeOp::prepare(&register_args("node-a", "inst-1"), &catalog, &mut batch).unwrap();
    apply_batch(&batch, &mut catalog);

    let mut batch = RecordBatch::new(2);
    let op =
        RequestStopNodeOp::prepare(&request_stop_args("node-a"), &catalog, &mut batch).unwrap();
    assert_eq!(batch.len(), 1);
    apply_batch(&batch, &mut catalog);

    let node = op.output(&catalog);
    assert!(matches!(node.state(), NodeState::Stopping { .. }));
}

#[test]
fn request_stop_idempotent_when_not_running() {
    let mut catalog = test_catalog();
    register_then_request_stop(&mut catalog, "node-a");

    // Stopping: a second stop request is a no-op, not a conflict.
    let mut batch = RecordBatch::new(3);
    let result = RequestStopNodeOp::prepare(&request_stop_args("node-a"), &catalog, &mut batch);
    assert!(matches!(result, Err(CatalogError::IdempotentNoOp)));
    assert_eq!(batch.len(), 0);

    // Stopped: after the ack lands, retrying the stop is still a no-op.
    let mut batch = RecordBatch::new(3);
    AckStopNodeOp::prepare(&ack_args("node-a", Some(7)), &catalog, &mut batch).unwrap();
    apply_batch(&batch, &mut catalog);
    let mut batch = RecordBatch::new(4);
    let result = RequestStopNodeOp::prepare(&request_stop_args("node-a"), &catalog, &mut batch);
    assert!(matches!(result, Err(CatalogError::IdempotentNoOp)));
    assert_eq!(batch.len(), 0);

    // Removing: the stop already completed; still a no-op.
    let mut batch = RecordBatch::new(4);
    RemoveNodeOp::prepare(&remove_args("node-a"), &catalog, &mut batch).unwrap();
    apply_batch(&batch, &mut catalog);
    let mut batch = RecordBatch::new(5);
    let result = RequestStopNodeOp::prepare(&request_stop_args("node-a"), &catalog, &mut batch);
    assert!(matches!(result, Err(CatalogError::IdempotentNoOp)));
    assert_eq!(batch.len(), 0);
}

#[test]
fn ack_rejected_when_not_stopping() {
    let mut catalog = test_catalog();
    let mut batch = RecordBatch::new(1);
    RegisterNodeOp::prepare(&register_args("node-a", "inst-1"), &catalog, &mut batch).unwrap();
    apply_batch(&batch, &mut catalog);

    let mut batch = RecordBatch::new(2);
    let result = AckStopNodeOp::prepare(&ack_args("node-a", None), &catalog, &mut batch);
    assert!(matches!(result, Err(CatalogError::InvalidStopAck { .. })));
}

#[test]
fn ack_succeeds_after_request_stop() {
    let mut catalog = test_catalog();
    register_then_request_stop(&mut catalog, "node-a");

    let mut batch = RecordBatch::new(3);
    let op = AckStopNodeOp::prepare(&ack_args("node-a", Some(7)), &catalog, &mut batch).unwrap();
    assert_eq!(batch.len(), 1);
    apply_batch(&batch, &mut catalog);

    let node = op.output(&catalog);
    match node.state() {
        NodeState::Stopped {
            ack_time_ns,
            final_snapshot_sequence,
            ..
        } => {
            assert_eq!(ack_time_ns, 3000);
            assert_eq!(
                final_snapshot_sequence,
                Some(SnapshotSequenceNumber::new(7))
            );
        }
        other => panic!("expected Stopped, got {other:?}"),
    }
}

#[test]
fn remove_rejected_when_running() {
    let mut catalog = test_catalog();
    let mut batch = RecordBatch::new(1);
    RegisterNodeOp::prepare(&register_args("node-a", "inst-1"), &catalog, &mut batch).unwrap();
    apply_batch(&batch, &mut catalog);

    let mut batch = RecordBatch::new(2);
    let result = RemoveNodeOp::prepare(&remove_args("node-a"), &catalog, &mut batch);
    assert!(matches!(
        result,
        Err(CatalogError::NodeNotFullyStopped { .. })
    ));
}

#[test]
fn remove_rejected_when_stopping() {
    let mut catalog = test_catalog();
    register_then_request_stop(&mut catalog, "node-a");

    let mut batch = RecordBatch::new(3);
    let result = RemoveNodeOp::prepare(&remove_args("node-a"), &catalog, &mut batch);
    assert!(matches!(
        result,
        Err(CatalogError::NodeNotFullyStopped { .. })
    ));
}

#[test]
fn remove_rejected_when_compact() {
    let mut catalog = test_catalog();

    let mut batch = RecordBatch::new(1);
    RegisterNodeOp::prepare(
        &register_args_with_mode("node-a", "inst-1", NodeMode::Compact),
        &catalog,
        &mut batch,
    )
    .unwrap();
    apply_batch(&batch, &mut catalog);

    let mut batch = RecordBatch::new(2);
    RequestStopNodeOp::prepare(&request_stop_args("node-a"), &catalog, &mut batch).unwrap();
    apply_batch(&batch, &mut catalog);

    let mut batch = RecordBatch::new(3);
    AckStopNodeOp::prepare(&ack_args("node-a", None), &catalog, &mut batch).unwrap();
    apply_batch(&batch, &mut catalog);

    let mut batch = RecordBatch::new(4);
    let result = RemoveNodeOp::prepare(&remove_args("node-a"), &catalog, &mut batch);
    assert!(matches!(
        result,
        Err(CatalogError::NodeModeNotRemovable { .. })
    ));
}

#[test]
fn remove_succeeds_when_stopped() {
    let mut catalog = test_catalog();
    register_request_stop_ack(&mut catalog, "node-a");

    let mut batch = RecordBatch::new(4);
    let op = RemoveNodeOp::prepare(&remove_args("node-a"), &catalog, &mut batch).unwrap();
    assert_eq!(batch.len(), 1);
    apply_batch(&batch, &mut catalog);

    let node = op.output(&catalog);
    match node.state() {
        NodeState::Removing {
            requested_time_ns,
            final_snapshot_sequence,
            ..
        } => {
            assert_eq!(requested_time_ns, 4000);
            assert_eq!(
                final_snapshot_sequence,
                Some(SnapshotSequenceNumber::new(7))
            );
        }
        other => panic!("expected Removing, got {other:?}"),
    }
}

#[test]
fn remove_idempotent_when_removing() {
    let mut catalog = test_catalog();
    register_request_stop_ack(&mut catalog, "node-a");

    let mut batch = RecordBatch::new(4);
    RemoveNodeOp::prepare(&remove_args("node-a"), &catalog, &mut batch).unwrap();
    apply_batch(&batch, &mut catalog);

    let mut batch = RecordBatch::new(5);
    let result = RemoveNodeOp::prepare(&remove_args("node-a"), &catalog, &mut batch);
    assert!(matches!(result, Err(CatalogError::IdempotentNoOp)));
}

#[test]
fn unregister_rejected_unless_removing() {
    let mut catalog = test_catalog();
    register_request_stop_ack(&mut catalog, "node-a");

    let mut batch = RecordBatch::new(4);
    let result = UnregisterNodeOp::prepare(&unregister_args("node-a"), &catalog, &mut batch);
    assert!(matches!(
        result,
        Err(CatalogError::InvalidUnregister { .. })
    ));
}

#[test]
fn remove_node_op_blocked_when_member_of_query_group() {
    // A node that belongs to a query group cannot be removed. Removing it
    // would leave the group referencing a node that no longer exists.
    use crate::catalog::versions::v3::ops::query_group::{
        CreateQueryGroupArgs, CreateQueryGroupOp,
    };
    use std::num::NonZeroUsize;

    let mut catalog = test_catalog();

    // Register as a query node so it satisfies query group membership validation.
    let mut batch = RecordBatch::new(1);
    RegisterNodeOp::prepare(
        &register_args_with_mode("node-a", "inst-1", NodeMode::Query),
        &catalog,
        &mut batch,
    )
    .unwrap();
    apply_batch(&batch, &mut catalog);

    let mut batch = RecordBatch::new(2);
    RequestStopNodeOp::prepare(&request_stop_args("node-a"), &catalog, &mut batch).unwrap();
    apply_batch(&batch, &mut catalog);

    let mut batch = RecordBatch::new(3);
    AckStopNodeOp::prepare(&ack_args("node-a", Some(7)), &catalog, &mut batch).unwrap();
    apply_batch(&batch, &mut catalog);

    let node_id = catalog
        .nodes
        .get_by_name("node-a")
        .unwrap()
        .node_catalog_id();

    let mut batch = RecordBatch::new(4);
    CreateQueryGroupOp::prepare(
        &CreateQueryGroupArgs {
            name: Arc::from("group-1"),
            members: vec![node_id],
            replication_factor: NonZeroUsize::new(1).unwrap(),
        },
        &catalog,
        &mut batch,
    )
    .unwrap();
    apply_batch(&batch, &mut catalog);

    let mut batch = RecordBatch::new(5);
    let result = RemoveNodeOp::prepare(&remove_args("node-a"), &catalog, &mut batch);
    assert!(matches!(result, Err(CatalogError::NodeInQueryGroup { .. })));
}

#[test]
fn unregister_succeeds_when_removing() {
    let mut catalog = test_catalog();
    register_request_stop_ack(&mut catalog, "node-a");

    let mut batch = RecordBatch::new(4);
    RemoveNodeOp::prepare(&remove_args("node-a"), &catalog, &mut batch).unwrap();
    apply_batch(&batch, &mut catalog);

    let mut batch = RecordBatch::new(5);
    UnregisterNodeOp::prepare(&unregister_args("node-a"), &catalog, &mut batch).unwrap();
    assert_eq!(batch.len(), 1);
    apply_batch(&batch, &mut catalog);

    assert!(catalog.nodes.get_by_name("node-a").is_none());
}

// ---------------------------------------------------------------------------
// A query group member may not clear its connection address
// ---------------------------------------------------------------------------

// Registration args for `node_id` in `Query` mode, advertising `conn_info`.
// The instance id is fixed, so re-registering re-uses the same node.
fn register_query_args(node_id: &str, conn_info: Option<&str>) -> RegisterNodeArgs {
    RegisterNodeArgs {
        conn_info: conn_info.map(str::to_string),
        mode: vec![NodeMode::Query],
        ..register_args(node_id, "inst-1")
    }
}

// Register `node_id` as a running query node advertising `conn_info`.
fn register_query_node(catalog: &mut InnerCatalog, node_id: &str, conn_info: Option<&str>) {
    let mut batch = RecordBatch::new(1);
    RegisterNodeOp::prepare(
        &register_query_args(node_id, conn_info),
        catalog,
        &mut batch,
    )
    .expect("register node");
    apply_batch(&batch, catalog);
}

// Put `node_id` in a query group, writing the record straight to the catalog.
// `CreateQueryGroupOp` refuses a member with no address, and one test needs
// exactly the state a catalog written before that check still holds.
fn put_in_query_group(catalog: &mut InnerCatalog, node_id: &str) {
    let member = catalog
        .nodes
        .get_by_name(node_id)
        .unwrap()
        .node_catalog_id();
    let mut batch = RecordBatch::new(2);
    batch.push(&CreateQueryGroup {
        query_group_id: 0,
        query_group_name: "analytics".to_string(),
        members: vec![member.get()],
        replication_factor: 1,
    });
    apply_batch(&batch, catalog);
}

#[test]
fn register_node_op_changes_conn_info_for_a_query_group_member() {
    // A restarted pod comes back at a new address, which must keep working.
    let mut catalog = test_catalog();
    register_query_node(&mut catalog, "node-a", Some("a:8181"));
    put_in_query_group(&mut catalog, "node-a");

    register_query_node(&mut catalog, "node-a", Some("a-moved:8181"));

    let node = catalog.nodes.get_by_name("node-a").unwrap();
    assert_eq!(node.conn_info().unwrap().as_ref(), "a-moved:8181");
}

#[test]
fn register_node_op_clears_conn_info_outside_a_query_group() {
    let mut catalog = test_catalog();
    register_query_node(&mut catalog, "node-a", Some("a:8181"));

    register_query_node(&mut catalog, "node-a", None);

    let node = catalog.nodes.get_by_name("node-a").unwrap();
    assert!(node.conn_info().is_none());
}

#[test]
fn register_node_op_re_registers_a_query_group_member_that_never_advertised() {
    // A catalog migrated from v2 holds no `conn_info`, so a member that never
    // advertised an address must still boot.
    let mut catalog = test_catalog();
    register_query_node(&mut catalog, "node-a", None);
    put_in_query_group(&mut catalog, "node-a");

    let mut batch = RecordBatch::new(3);
    RegisterNodeOp::prepare(&register_query_args("node-a", None), &catalog, &mut batch)
        .expect("a member that never advertised an address must still register");
}

#[test]
fn register_node_op_rejects_clearing_conn_info_for_a_query_group_member() {
    let mut catalog = test_catalog();
    register_query_node(&mut catalog, "node-a", Some("a:8181"));
    put_in_query_group(&mut catalog, "node-a");

    let mut batch = RecordBatch::new(3);
    let Err(err) =
        RegisterNodeOp::prepare(&register_query_args("node-a", None), &catalog, &mut batch)
    else {
        panic!("clearing conn info for a query group member must be refused");
    };
    match err {
        CatalogError::NodeConnInfoRequiredInQueryGroup {
            node_id,
            query_group_name,
            ..
        } => {
            assert_eq!(node_id.as_ref(), "node-a");
            assert_eq!(query_group_name.as_ref(), "analytics");
        }
        other => panic!("expected NodeConnInfoRequiredInQueryGroup, got {other:?}"),
    }
}
