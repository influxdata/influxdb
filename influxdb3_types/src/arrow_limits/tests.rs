use super::*;

#[test]
fn test_constants() {
    assert_eq!(ARROW_VAR_COL_MAX_BYTES, i32::MAX as usize);
    const _: () = {
        assert!(ARROW_VAR_COL_MAX_BYTES > 2_140_000_000);
        assert!(ARROW_VAR_COL_MAX_BYTES < 2_150_000_000);
    };
}
