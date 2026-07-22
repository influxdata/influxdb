use super::*;

#[test]
fn trigger_work_ids_are_distinct_uuid_v7_values() {
    let first = TriggerWorkId::next();
    let second = TriggerWorkId::next();

    assert_ne!(first, second);
    assert_eq!(first.0.get_version_num(), 7);
    assert_eq!(second.0.get_version_num(), 7);
}
