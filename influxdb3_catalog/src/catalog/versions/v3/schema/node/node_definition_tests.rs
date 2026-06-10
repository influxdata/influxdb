use super::*;

fn mk(state: NodeState, instance: &str) -> NodeDefinition {
    NodeDefinition {
        node_id: Arc::from("n1"),
        node_catalog_id: NodeId::new(1),
        instance_id: Arc::from(instance),
        mode: vec![NodeMode::Query],
        core_count: 1,
        state,
        conn_info: None,
        cli_params: None,
        row_delete_predicate_version: 0,
        feature_level: FeatureLevel::ZERO,
    }
}

#[test]
fn can_re_register_matrix() {
    let running = NodeState::Running {
        registered_time_ns: 0,
    };
    let stopping = NodeState::Stopping { stopped_time_ns: 0 };
    let stopped = NodeState::Stopped {
        stopped_time_ns: 0,
        ack_time_ns: 1,
        final_snapshot_sequence: None,
    };
    let removing = NodeState::Removing {
        requested_time_ns: 0,
        final_snapshot_sequence: None,
    };

    assert!(mk(running, "i1").can_re_register("i1"));
    assert!(!mk(running, "i1").can_re_register("i2"));
    assert!(mk(stopping, "i1").can_re_register("i1"));
    assert!(!mk(stopping, "i1").can_re_register("i2"));
    assert!(mk(stopped, "i1").can_re_register("i1"));
    assert!(mk(stopped, "i1").can_re_register("i2"));
    assert!(!mk(removing, "i1").can_re_register("i1"));
    assert!(!mk(removing, "i1").can_re_register("i2"));
}
