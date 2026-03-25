//! Constants for Arrow array size limits.
//!
//! Arrow's `StringBuilder` uses 32-bit offsets, which limits the total variable-length payload to
//! `i32::MAX` bytes.
//!
/// Arrays with total variable-length payload above this limit would panic during build.
pub const ARROW_VAR_COL_MAX_BYTES: usize = i32::MAX as usize;

#[cfg(test)]
mod tests;
