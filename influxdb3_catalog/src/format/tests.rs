use super::*;

#[test]
fn op_flags_bits() {
    let flags = RecordFlags::none();
    assert!(!flags.is_upgrade_safe());
    assert_eq!(flags.to_u16(), 0);

    let flags = RecordFlags::upgrade_safe();
    assert!(flags.is_upgrade_safe());
    assert_eq!(flags.to_u16(), 1);

    let flags = RecordFlags::default();
    assert!(!flags.is_upgrade_safe());
}

#[test]
fn op_flags_from_u16() {
    let flags = RecordFlags::from_u16(0x0001);
    assert!(flags.is_upgrade_safe());
    assert_eq!(flags.to_u16(), 0x0001);

    let flags = RecordFlags::from_u16(0x0000);
    assert!(!flags.is_upgrade_safe());
}
