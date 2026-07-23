//! Helper types to express disk size.

use std::str::FromStr;

use crate::size_units::{parse_unit_suffix, unit_suffixes};

/// Disk size with optional unit suffix, floored to a multiple of `BLOCK_SIZE`.
///
/// The default `BLOCK_SIZE` of 4096 is a multiple of the logical block sizes
/// in common use (512 and 4096).
///
/// # Parsing
/// This can be parsed from strings in one of the following formats:
///
/// - **absolute (default bytes):** a plain non-negative number specifies the size in bytes,
///   e.g. `1048576`
/// - **with unit suffix** (case-insensitive):
///   - `b` for bytes, e.g. `1048576b`
///   - `kb` for kilobytes, e.g. `1024kb`
///   - `mb` for megabytes, e.g. `100mb`
///   - `gb` for gigabytes, e.g. `2gb`
///   - `tb` for terabytes, e.g. `1tb`
///
/// Whitespace before the suffix is allowed, e.g. `5 gb`.
///
/// Percentages are rejected. A value smaller than `BLOCK_SIZE` is an error.
#[derive(Debug, Clone, Copy)]
pub struct DiskSize<const BLOCK_SIZE: usize = 4096>(usize);

impl<const BLOCK_SIZE: usize> DiskSize<BLOCK_SIZE> {
    /// Express this size in terms of bytes (B), always a multiple of `BLOCK_SIZE`
    pub fn as_num_bytes(&self) -> usize {
        self.0
    }

    /// Construct from a byte count, flooring to a `BLOCK_SIZE` multiple; a
    /// count smaller than `BLOCK_SIZE` is an error.
    ///
    /// A zero `BLOCK_SIZE` is rejected at compile time:
    ///
    /// ```compile_fail
    /// use influxdb3_clap_blocks::disk_size::DiskSize;
    /// let size = DiskSize::<0>::from_bytes(4096);
    /// ```
    pub fn from_bytes(bytes: usize) -> Result<Self, String> {
        const {
            assert!(BLOCK_SIZE > 0, "DiskSize BLOCK_SIZE must be non-zero");
        }
        if bytes < BLOCK_SIZE {
            Err(format!(
                "specified disk size is less than minimum block size of {BLOCK_SIZE} bytes"
            ))
        } else {
            Ok(Self(bytes - bytes % BLOCK_SIZE))
        }
    }
}

impl<const BLOCK_SIZE: usize> FromStr for DiskSize<BLOCK_SIZE> {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim().to_lowercase();
        if s.ends_with('%') {
            return Err(format!(
                "'{}' must be an absolute size: a number of bytes or a value \
                 with a unit suffix ({})",
                s,
                unit_suffixes()
            ));
        }
        match parse_unit_suffix(&s) {
            Some(result) => result.and_then(Self::from_bytes),
            // Bare number = bytes
            None => usize::from_str(&s)
                .map_err(|e| format!("failed to parse '{}' as a disk size in bytes: {}", s, e))
                .and_then(Self::from_bytes),
        }
    }
}

#[cfg(test)]
mod tests;
