//! Shared byte-size units and unit-suffix parsing for size types.

use std::str::FromStr;

pub const KB: usize = 1024;
pub const MB: usize = 1024 * KB;
pub const GB: usize = 1024 * MB;
pub const TB: usize = 1024 * GB;

/// Unit suffixes with their display name and byte multiplier.
/// Order matters for parsing: a suffix must come before its own suffixes
/// ("kb" before "b"), since the first match is stripped.
pub(crate) const UNITS: &[(&str, &str, usize)] = &[
    ("tb", "TB", TB),
    ("gb", "GB", GB),
    ("mb", "MB", MB),
    ("kb", "KB", KB),
    ("b", "bytes (b)", 1),
];

/// The accepted unit suffixes, smallest first, for error messages.
pub(crate) fn unit_suffixes() -> String {
    let mut suffixes: Vec<&str> = UNITS.iter().map(|(suffix, _, _)| *suffix).collect();
    suffixes.reverse();
    suffixes.join(", ")
}

/// Parse a size string with an explicit unit suffix.
///
/// Expects a trimmed, lowercased input. Returns the size in bytes, or `None`
/// if no unit suffix matched. A value that overflows `usize` is an error.
pub(crate) fn parse_unit_suffix(s: &str) -> Option<Result<usize, String>> {
    for (suffix, name, multiplier) in UNITS {
        if let Some(num_str) = s.strip_suffix(suffix) {
            let result = usize::from_str(num_str.trim())
                .map_err(|e| format!("failed to parse '{}' as a size in {}: {}", s, name, e))
                .and_then(|num| {
                    num.checked_mul(*multiplier).ok_or_else(|| {
                        format!("'{}' overflows the maximum representable size in bytes", s)
                    })
                });
            return Some(result);
        }
    }
    None
}
