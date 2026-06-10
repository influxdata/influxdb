use std::sync::Arc;

use influxdb3_wal::SnapshotSequenceNumber;
use uuid::Uuid;

use crate::catalog::versions::v3::inner::InnerCatalog;
use crate::catalog::versions::v3::schema::node::NodeState;
use crate::format::records::assert_roundtrip;
use crate::format::records::node::{
    AckStopNode, RegisterNode, RemoveNode, RequestStopNode, StopNode, UnregisterNode,
};
use crate::format::records::types::NodeMode;
use crate::format::{CatalogRecord, FeatureLevel};

/// Helper to create a test catalog.
fn test_catalog() -> InnerCatalog {
    InnerCatalog::new(Arc::from("test"), Uuid::nil())
}

/// Register node id=1 name="node-a", then drive it into the `Stopping` state
/// via `RequestStopNode`. Returns the catalog ready for AckStopNode tests.
fn registered_and_stopping() -> InnerCatalog {
    let mut catalog = test_catalog();
    RegisterNode {
        node_catalog_id: 1,
        node_id: "node-a".to_string(),
        instance_id: "inst-1".to_string(),
        registered_time_ns: 1000,
        core_count: 4,
        mode: vec![NodeMode::Core],
        process_uuid: [0u8; 16],
        conn_info: None,
        cli_params: None,
        row_delete_predicate_version: 0,
        feature_level: FeatureLevel::ZERO,
    }
    .apply(&mut catalog)
    .unwrap();
    RequestStopNode {
        node_catalog_id: 1,
        node_id: "node-a".to_string(),
        stopped_time_ns: 2000,
        process_uuid: [0u8; 16],
    }
    .apply(&mut catalog)
    .unwrap();
    catalog
}

/// Carry the node through to terminal `Stopped` via RequestStopNode + AckStopNode.
fn registered_and_stopped() -> InnerCatalog {
    let mut catalog = registered_and_stopping();
    AckStopNode {
        node_catalog_id: 1,
        node_id: "node-a".to_string(),
        ack_time_ns: 3000,
        process_uuid: [0u8; 16],
        final_snapshot_sequence: Some(7),
    }
    .apply(&mut catalog)
    .unwrap();
    catalog
}

#[test]
fn register_node_round_trip() {
    assert_roundtrip!(
        RegisterNode {
            node_catalog_id: 42,
            node_id: "node-1".to_string(),
            instance_id: "inst-abc".to_string(),
            registered_time_ns: 1234567890,
            core_count: 8,
            mode: vec![NodeMode::Core, NodeMode::Query],
            process_uuid: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            conn_info: Some("localhost:8080".to_string()),
            cli_params: Some("--verbose".to_string()),
            row_delete_predicate_version: 3,
            feature_level: FeatureLevel {
                core: 25,
                enterprise: 3,
            },
        },
        "042a066e6f64652d3108696e73742d61626302d20296490608020601011032547698badcfe010e6c6f63616c686f73743a3830383001092d2d766572626f7365060319000300"
    );
}

#[test]
fn register_node_minimal() {
    assert_roundtrip!(
        RegisterNode {
            node_catalog_id: 1,
            node_id: "n".to_string(),
            instance_id: "i".to_string(),
            registered_time_ns: 0,
            core_count: 1,
            mode: vec![NodeMode::Core],
            process_uuid: [0; 16],
            conn_info: None,
            cli_params: None,
            row_delete_predicate_version: 0,
            feature_level: FeatureLevel::ZERO,
        },
        "0401016e01690600060101000a00000000060000000000"
    );
}

#[test]
fn stop_node_round_trip() {
    assert_roundtrip!(
        StopNode {
            node_catalog_id: 42,
            node_id: "node-1".to_string(),
            stopped_time_ns: 9876543210,
            process_uuid: [16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1],
        },
        "042a066e6f64652d3100ea16b04c020000000101efcdab8967452301"
    );
}

#[test]
fn record_ids() {
    assert_eq!(RegisterNode::ID.raw(), 2);
    assert!(!RegisterNode::FLAGS.is_upgrade_safe());

    assert_eq!(StopNode::ID.raw(), 3);
    assert!(!StopNode::FLAGS.is_upgrade_safe());
}

// ---------------------------------------------------------------------------
// Apply tests
// ---------------------------------------------------------------------------

#[test]
fn apply_register_node() {
    let mut catalog = test_catalog();
    let record = RegisterNode {
        node_catalog_id: 1,
        node_id: "node-a".to_string(),
        instance_id: "inst-1".to_string(),
        registered_time_ns: 1000,
        core_count: 4,
        mode: vec![NodeMode::Core],
        process_uuid: [0u8; 16],
        conn_info: Some("localhost:8080".to_string()),
        cli_params: None,
        row_delete_predicate_version: 7,
        feature_level: FeatureLevel::ZERO,
    };
    record.apply(&mut catalog).unwrap();

    let node = catalog
        .nodes
        .get_by_name("node-a")
        .expect("node should exist");
    assert_eq!(node.node_catalog_id(), influxdb3_id::NodeId::new(1));
    assert!(node.is_running());
    assert_eq!(node.core_count(), 4);
    assert_eq!(node.conn_info().unwrap().as_ref(), "localhost:8080");
    assert_eq!(node.row_delete_predicate_version(), 7);
}

#[test]
fn apply_register_node_re_registration_after_stop() {
    let mut catalog = test_catalog();

    // Register
    RegisterNode {
        node_catalog_id: 1,
        node_id: "node-a".to_string(),
        instance_id: "inst-1".to_string(),
        registered_time_ns: 1000,
        core_count: 4,
        mode: vec![NodeMode::Core],
        process_uuid: [0u8; 16],
        conn_info: None,
        cli_params: None,
        row_delete_predicate_version: 0,
        feature_level: FeatureLevel::ZERO,
    }
    .apply(&mut catalog)
    .unwrap();

    // Stop
    StopNode {
        node_catalog_id: 1,
        node_id: "node-a".to_string(),
        stopped_time_ns: 2000,
        process_uuid: [0u8; 16],
    }
    .apply(&mut catalog)
    .unwrap();

    // Re-register with different instance_id (allowed after stop)
    RegisterNode {
        node_catalog_id: 1,
        node_id: "node-a".to_string(),
        instance_id: "inst-2".to_string(),
        registered_time_ns: 3000,
        core_count: 8,
        mode: vec![NodeMode::All],
        process_uuid: [1u8; 16],
        conn_info: None,
        cli_params: None,
        row_delete_predicate_version: 0,
        feature_level: FeatureLevel::ZERO,
    }
    .apply(&mut catalog)
    .unwrap();

    let node = catalog
        .nodes
        .get_by_id(&influxdb3_id::NodeId::new(1))
        .unwrap();
    assert!(node.is_running());
    assert_eq!(node.core_count(), 8);
    assert_eq!(node.instance_id().as_ref(), "inst-2");
}

#[test]
fn apply_stop_node() {
    let mut catalog = test_catalog();

    RegisterNode {
        node_catalog_id: 1,
        node_id: "node-a".to_string(),
        instance_id: "inst-1".to_string(),
        registered_time_ns: 1000,
        core_count: 4,
        mode: vec![NodeMode::Core],
        process_uuid: [0u8; 16],
        conn_info: None,
        cli_params: None,
        row_delete_predicate_version: 0,
        feature_level: FeatureLevel::ZERO,
    }
    .apply(&mut catalog)
    .unwrap();

    StopNode {
        node_catalog_id: 1,
        node_id: "node-a".to_string(),
        stopped_time_ns: 2000,
        process_uuid: [0u8; 16],
    }
    .apply(&mut catalog)
    .unwrap();

    let node = catalog
        .nodes
        .get_by_id(&influxdb3_id::NodeId::new(1))
        .unwrap();
    assert!(node.state().is_stopped());
}

#[test]
fn request_stop_node_round_trip() {
    assert_roundtrip!(
        RequestStopNode {
            node_catalog_id: 42,
            node_id: "node-1".to_string(),
            stopped_time_ns: 9876543210,
            process_uuid: [16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1],
        },
        "042a066e6f64652d3100ea16b04c020000000101efcdab8967452301"
    );
}

#[test]
fn ack_stop_node_round_trip() {
    assert_roundtrip!(
        AckStopNode {
            node_catalog_id: 42,
            node_id: "node-1".to_string(),
            ack_time_ns: 9876543210,
            process_uuid: [16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1],
            final_snapshot_sequence: Some(99),
        },
        "042a066e6f64652d3100ea16b04c020000000101efcdab8967452301010663"
    );
}

#[test]
fn remove_node_round_trip() {
    assert_roundtrip!(
        RemoveNode {
            node_catalog_id: 42,
            node_id: "node-1".to_string(),
            requested_time_ns: 1234567890,
            process_uuid: [16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1],
        },
        "042a066e6f64652d3102d20296490101efcdab8967452301"
    );
}

#[test]
fn unregister_node_round_trip() {
    assert_roundtrip!(
        UnregisterNode {
            node_catalog_id: 42,
            node_id: "node-1".to_string(),
            unregistered_time_ns: 1234567890,
        },
        "042a066e6f64652d3102d2029649"
    );
}

#[test]
fn new_record_ids() {
    assert_eq!(RequestStopNode::ID.raw(), 43);
    assert!(!RequestStopNode::FLAGS.is_upgrade_safe());
    assert_eq!(AckStopNode::ID.raw(), 44);
    assert!(!AckStopNode::FLAGS.is_upgrade_safe());
    assert_eq!(RemoveNode::ID.raw(), 45);
    assert!(!RemoveNode::FLAGS.is_upgrade_safe());
    assert_eq!(UnregisterNode::ID.raw(), 46);
    assert!(!UnregisterNode::FLAGS.is_upgrade_safe());
}

#[test]
fn request_stop_transitions_running_to_stopping() {
    let mut catalog = test_catalog();
    RegisterNode {
        node_catalog_id: 1,
        node_id: "node-a".to_string(),
        instance_id: "inst-1".to_string(),
        registered_time_ns: 1000,
        core_count: 4,
        mode: vec![NodeMode::Core],
        process_uuid: [0u8; 16],
        conn_info: None,
        cli_params: None,
        row_delete_predicate_version: 0,
        feature_level: FeatureLevel::ZERO,
    }
    .apply(&mut catalog)
    .unwrap();

    RequestStopNode {
        node_catalog_id: 1,
        node_id: "node-a".to_string(),
        stopped_time_ns: 2000,
        process_uuid: [0u8; 16],
    }
    .apply(&mut catalog)
    .unwrap();

    let node = catalog
        .nodes
        .get_by_id(&influxdb3_id::NodeId::new(1))
        .unwrap();
    match node.state() {
        NodeState::Stopping { stopped_time_ns } => assert_eq!(stopped_time_ns, 2000),
        other => panic!("expected Stopping, got {other:?}"),
    }
}

#[test]
fn request_stop_idempotent_when_already_stopping() {
    let mut catalog = registered_and_stopping();
    // A second RequestStopNode must not rewind `stopped_time_ns`.
    RequestStopNode {
        node_catalog_id: 1,
        node_id: "node-a".to_string(),
        stopped_time_ns: 9999,
        process_uuid: [0u8; 16],
    }
    .apply(&mut catalog)
    .unwrap();

    let node = catalog
        .nodes
        .get_by_id(&influxdb3_id::NodeId::new(1))
        .unwrap();
    match node.state() {
        NodeState::Stopping { stopped_time_ns } => assert_eq!(stopped_time_ns, 2000),
        other => panic!("expected Stopping, got {other:?}"),
    }
}

#[test]
fn request_stop_no_op_when_stopped() {
    let mut catalog = registered_and_stopped();
    RequestStopNode {
        node_catalog_id: 1,
        node_id: "node-a".to_string(),
        stopped_time_ns: 9999,
        process_uuid: [0u8; 16],
    }
    .apply(&mut catalog)
    .unwrap();

    let node = catalog
        .nodes
        .get_by_id(&influxdb3_id::NodeId::new(1))
        .unwrap();
    assert!(node.state().is_stopped());
}

#[test]
fn ack_stop_transitions_stopping_to_stopped() {
    let mut catalog = registered_and_stopping();
    AckStopNode {
        node_catalog_id: 1,
        node_id: "node-a".to_string(),
        ack_time_ns: 3000,
        process_uuid: [0u8; 16],
        final_snapshot_sequence: Some(7),
    }
    .apply(&mut catalog)
    .unwrap();

    let node = catalog
        .nodes
        .get_by_id(&influxdb3_id::NodeId::new(1))
        .unwrap();
    match node.state() {
        NodeState::Stopped {
            stopped_time_ns,
            ack_time_ns,
            final_snapshot_sequence,
        } => {
            assert_eq!(stopped_time_ns, 2000);
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
fn ack_stop_idempotent_when_already_stopped() {
    let mut catalog = registered_and_stopped();
    AckStopNode {
        node_catalog_id: 1,
        node_id: "node-a".to_string(),
        ack_time_ns: 9999,
        process_uuid: [0u8; 16],
        final_snapshot_sequence: Some(42),
    }
    .apply(&mut catalog)
    .unwrap();

    let node = catalog
        .nodes
        .get_by_id(&influxdb3_id::NodeId::new(1))
        .unwrap();
    match node.state() {
        NodeState::Stopped {
            stopped_time_ns,
            ack_time_ns,
            final_snapshot_sequence,
        } => {
            assert_eq!(stopped_time_ns, 2000);
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
fn ack_stop_ignored_when_running() {
    let mut catalog = test_catalog();
    RegisterNode {
        node_catalog_id: 1,
        node_id: "node-a".to_string(),
        instance_id: "inst-1".to_string(),
        registered_time_ns: 1000,
        core_count: 4,
        mode: vec![NodeMode::Core],
        process_uuid: [0u8; 16],
        conn_info: None,
        cli_params: None,
        row_delete_predicate_version: 0,
        feature_level: FeatureLevel::ZERO,
    }
    .apply(&mut catalog)
    .unwrap();

    AckStopNode {
        node_catalog_id: 1,
        node_id: "node-a".to_string(),
        ack_time_ns: 3000,
        process_uuid: [0u8; 16],
        final_snapshot_sequence: None,
    }
    .apply(&mut catalog)
    .unwrap();

    let node = catalog
        .nodes
        .get_by_id(&influxdb3_id::NodeId::new(1))
        .unwrap();
    assert!(matches!(node.state(), NodeState::Running { .. }));
}

#[test]
fn remove_node_rejects_running() {
    let mut catalog = test_catalog();
    RegisterNode {
        node_catalog_id: 1,
        node_id: "node-a".to_string(),
        instance_id: "inst-1".to_string(),
        registered_time_ns: 1000,
        core_count: 4,
        mode: vec![NodeMode::Core],
        process_uuid: [0u8; 16],
        conn_info: None,
        cli_params: None,
        row_delete_predicate_version: 0,
        feature_level: FeatureLevel::ZERO,
    }
    .apply(&mut catalog)
    .unwrap();

    let err = RemoveNode {
        node_catalog_id: 1,
        node_id: "node-a".to_string(),
        requested_time_ns: 4000,
        process_uuid: [0u8; 16],
    }
    .apply(&mut catalog)
    .expect_err("RemoveNode should reject Running state");
    assert!(
        err.0.contains("not in Stopped state"),
        "unexpected error: {err}"
    );
}

#[test]
fn remove_node_rejects_stopping() {
    let mut catalog = registered_and_stopping();
    let err = RemoveNode {
        node_catalog_id: 1,
        node_id: "node-a".to_string(),
        requested_time_ns: 4000,
        process_uuid: [0u8; 16],
    }
    .apply(&mut catalog)
    .expect_err("RemoveNode should reject Stopping state");
    assert!(
        err.0.contains("not in Stopped state"),
        "unexpected error: {err}"
    );
}

#[test]
fn remove_node_transitions_stopped_to_removing() {
    let mut catalog = registered_and_stopped();
    RemoveNode {
        node_catalog_id: 1,
        node_id: "node-a".to_string(),
        requested_time_ns: 4000,
        process_uuid: [0u8; 16],
    }
    .apply(&mut catalog)
    .unwrap();

    let node = catalog
        .nodes
        .get_by_id(&influxdb3_id::NodeId::new(1))
        .unwrap();
    match node.state() {
        NodeState::Removing {
            requested_time_ns,
            final_snapshot_sequence,
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
fn remove_node_idempotent_when_removing() {
    let mut catalog = registered_and_stopped();
    RemoveNode {
        node_catalog_id: 1,
        node_id: "node-a".to_string(),
        requested_time_ns: 4000,
        process_uuid: [0u8; 16],
    }
    .apply(&mut catalog)
    .unwrap();
    RemoveNode {
        node_catalog_id: 1,
        node_id: "node-a".to_string(),
        requested_time_ns: 5000,
        process_uuid: [0u8; 16],
    }
    .apply(&mut catalog)
    .unwrap();

    let node = catalog
        .nodes
        .get_by_id(&influxdb3_id::NodeId::new(1))
        .unwrap();
    match node.state() {
        NodeState::Removing {
            requested_time_ns,
            final_snapshot_sequence,
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
fn unregister_node_purges_entry() {
    let mut catalog = registered_and_stopped();
    RemoveNode {
        node_catalog_id: 1,
        node_id: "node-a".to_string(),
        requested_time_ns: 4000,
        process_uuid: [0u8; 16],
    }
    .apply(&mut catalog)
    .unwrap();
    UnregisterNode {
        node_catalog_id: 1,
        node_id: "node-a".to_string(),
        unregistered_time_ns: 5000,
    }
    .apply(&mut catalog)
    .unwrap();

    assert!(
        catalog
            .nodes
            .get_by_id(&influxdb3_id::NodeId::new(1))
            .is_none(),
        "node should be purged from catalog"
    );
    assert!(catalog.nodes.get_by_name("node-a").is_none());
}

#[test]
fn unregister_node_rejects_unless_removing() {
    let mut catalog = registered_and_stopped();
    let err = UnregisterNode {
        node_catalog_id: 1,
        node_id: "node-a".to_string(),
        unregistered_time_ns: 5000,
    }
    .apply(&mut catalog)
    .expect_err("UnregisterNode should reject Stopped state");
    assert!(
        err.0.contains("not in Removing state"),
        "unexpected error: {err}"
    );
}

#[test]
fn unregister_node_idempotent_when_already_gone() {
    let mut catalog = registered_and_stopped();
    RemoveNode {
        node_catalog_id: 1,
        node_id: "node-a".to_string(),
        requested_time_ns: 4000,
        process_uuid: [0u8; 16],
    }
    .apply(&mut catalog)
    .unwrap();
    UnregisterNode {
        node_catalog_id: 1,
        node_id: "node-a".to_string(),
        unregistered_time_ns: 5000,
    }
    .apply(&mut catalog)
    .unwrap();
    UnregisterNode {
        node_catalog_id: 1,
        node_id: "node-a".to_string(),
        unregistered_time_ns: 6000,
    }
    .apply(&mut catalog)
    .unwrap();
}
