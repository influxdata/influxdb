use influxdb3_catalog::catalog::versions::v3::schema::node::{NodeState, RemovalAttestation};

/// Pins the four `system.nodes.state` strings rendered by `NodeState::as_str`.
/// The system table calls `node.state().as_str()`, so any drift in these
/// strings would silently change Grafana dashboards and operator runbooks.
#[test]
fn node_state_as_str_covers_all_four_lifecycle_states() {
    let cases = [
        (
            NodeState::Running {
                registered_time_ns: 0,
            },
            "running",
        ),
        (NodeState::Stopping { stopped_time_ns: 0 }, "stopping"),
        (
            NodeState::Stopped {
                stopped_time_ns: 0,
                ack_time_ns: 0,
                final_snapshot_sequence: None,
            },
            "stopped",
        ),
        (
            NodeState::Removing {
                requested_time_ns: 0,
                final_snapshot_sequence: None,
                attestation: RemovalAttestation::NotForced,
            },
            "removing",
        ),
    ];
    for (state, expected) in cases {
        assert_eq!(state.as_str(), expected);
    }
}
