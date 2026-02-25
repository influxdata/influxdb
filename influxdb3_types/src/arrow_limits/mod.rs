//! Constants for Arrow array size limits.
//!
//! Arrow's `StringBuilder` uses 32-bit offsets, which limits the total variable-length payload to
//! `i32::MAX` bytes.
//!
/// Arrays with total variable-length payload above this limit would panic during build.
pub const ARROW_VAR_COL_MAX_BYTES: usize = i32::MAX as usize;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_constants() {
        assert_eq!(ARROW_VAR_COL_MAX_BYTES, i32::MAX as usize);
        const _: () = {
            assert!(ARROW_VAR_COL_MAX_BYTES > 2_140_000_000);
            assert!(ARROW_VAR_COL_MAX_BYTES < 2_150_000_000);
        };
    }
}
