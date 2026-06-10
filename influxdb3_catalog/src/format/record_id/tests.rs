use super::*;

#[test]
fn core_id_raw_value() {
    let id = RecordId::core(1);
    assert_eq!(id.raw(), 1);
    assert!(!id.is_enterprise());
}

#[test]
fn enterprise_id_raw_value() {
    let id = RecordId::enterprise(1);
    assert_eq!(id.raw(), 0x8001);
    assert!(id.is_enterprise());
}

#[test]
fn core_and_enterprise_do_not_overlap() {
    let core = RecordId::core(1);
    let ent = RecordId::enterprise(1);
    assert_ne!(core.raw(), ent.raw());
}

#[test]
fn from_raw_round_trips() {
    let id = RecordId::enterprise(5);
    let restored = RecordId::from_raw(id.raw());
    assert_eq!(id, restored);
}

#[test]
fn display_formatting() {
    assert_eq!(RecordId::core(3).to_string(), "core(3)");
    assert_eq!(RecordId::enterprise(2).to_string(), "enterprise(2)");
}

#[test]
fn ordering() {
    // Core IDs sort before enterprise IDs
    assert!(RecordId::core(24) < RecordId::enterprise(1));
}

#[test]
fn enterprise_bit_mask() {
    assert!(
        RecordId::enterprise(1)
            .as_enterprise()
            .is_some_and(|i| i == 1)
    )
}

#[test]
fn record_id_kind() {
    assert_eq!(RecordId::core(1).kind(), RecordIdKind::Core(1));
    assert_eq!(RecordId::enterprise(1).kind(), RecordIdKind::Enterprise(1));
}
