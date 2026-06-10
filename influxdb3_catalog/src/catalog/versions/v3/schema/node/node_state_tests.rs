use super::*;

fn states() -> [NodeState; 4] {
    [
        NodeState::Running {
            registered_time_ns: 10,
        },
        NodeState::Stopping {
            stopped_time_ns: 20,
        },
        NodeState::Stopped {
            stopped_time_ns: 20,
            ack_time_ns: 25,
            final_snapshot_sequence: Some(SnapshotSequenceNumber::new(7)),
        },
        NodeState::Removing {
            requested_time_ns: 30,
            final_snapshot_sequence: Some(SnapshotSequenceNumber::new(7)),
        },
    ]
}

#[test]
fn is_running_only_running() {
    let s = states();
    assert!(s[0].is_running());
    for v in &s[1..] {
        assert!(!v.is_running());
    }
}

#[test]
fn is_stopped_only_stopped() {
    let s = states();
    assert!(!s[0].is_stopped());
    assert!(!s[1].is_stopped());
    assert!(s[2].is_stopped());
    assert!(!s[3].is_stopped());
}

#[test]
fn is_removing_only_removing() {
    let s = states();
    for v in &s[..3] {
        assert!(!v.is_removing());
    }
    assert!(s[3].is_removing());
}

#[test]
fn as_str_renders_four_strings() {
    let s = states();
    assert_eq!(s[0].as_str(), "running");
    assert_eq!(s[1].as_str(), "stopping");
    assert_eq!(s[2].as_str(), "stopped");
    assert_eq!(s[3].as_str(), "removing");
}

#[test]
fn updated_at_ns_picks_latest_timestamp() {
    let s = states();
    assert_eq!(s[0].updated_at_ns(), 10);
    assert_eq!(s[1].updated_at_ns(), 20);
    assert_eq!(s[2].updated_at_ns(), 25);
    assert_eq!(s[3].updated_at_ns(), 30);
}

#[test]
fn final_snapshot_sequence_present_only_on_terminal_states() {
    let s = states();
    assert!(s[0].final_snapshot_sequence().is_none());
    assert!(s[1].final_snapshot_sequence().is_none());
    assert_eq!(
        s[2].final_snapshot_sequence(),
        Some(SnapshotSequenceNumber::new(7))
    );
    assert_eq!(
        s[3].final_snapshot_sequence(),
        Some(SnapshotSequenceNumber::new(7))
    );
}
