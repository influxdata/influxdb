//! Helper types to express memory size.

use std::{str::FromStr, sync::OnceLock};

use observability_deps::tracing::info;
use sysinfo::System;

/// Memory size.
///
/// # Parsing
/// This can be parsed from strings in one of the following formats:
///
/// - **absolute:** just use a non-negative number to specify the absolute
///   bytes, e.g. `1024`
/// - **relative:** use percentage between 0 and 100 (both inclusive) to specify
///   a relative amount of the totally available memory size, e.g. `50%`
///
/// # Limits
///
/// Memory limits are read from the following, stopping when a valid value is
/// found:
///
///   - `/sys/fs/cgroup/memory/memory.limit_in_bytes` (cgroup)
///   - `/sys/fs/cgroup/memory.max` (cgroup2)
///   - Platform specific syscall (infallible)
///
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MemorySize(usize);

impl MemorySize {
    /// Number of bytes.
    pub fn bytes(&self) -> usize {
        self.0
    }
}

impl std::fmt::Debug for MemorySize {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::fmt::Display for MemorySize {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for MemorySize {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.strip_suffix('%') {
            Some(s) => {
                let percentage = u64::from_str(s).map_err(|e| e.to_string())?;
                if percentage > 100 {
                    return Err(format!(
                        "relative memory size must be in [0, 100] but is {percentage}"
                    ));
                }
                let total = total_mem_bytes();
                let bytes = (percentage as f64 / 100f64 * total as f64).round() as usize;
                Ok(Self(bytes))
            }
            None => {
                let bytes = usize::from_str(s).map_err(|e| e.to_string())?;
                Ok(Self(bytes))
            }
        }
    }
}

/// Totally available memory size in bytes.
pub fn total_mem_bytes() -> usize {
    // Keep this in a global state so that we only need to inspect the system once during IOx startup.
    static TOTAL_MEM_BYTES: OnceLock<usize> = OnceLock::new();

    *TOTAL_MEM_BYTES.get_or_init(get_memory_limit)
}

/// Resolve the amount of memory available to this process.
///
/// This attempts to find a cgroup limit first, before falling back to the
/// amount of system RAM available.
fn get_memory_limit() -> usize {
    let mut sys = System::new();
    sys.refresh_memory();

    let limit = sys
        .cgroup_limits()
        .map(|v| v.total_memory)
        .unwrap_or_else(|| sys.total_memory()) as usize;

    info!(%limit, "detected process memory available");

    limit
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse() {
        assert_ok("0", 0);
        assert_ok("1", 1);
        assert_ok("1024", 1024);
        assert_ok("0%", 0);

        assert_gt_zero("50%");

        assert_err("-1", "invalid digit found in string");
        assert_err("foo", "invalid digit found in string");
        assert_err("-1%", "invalid digit found in string");
        assert_err(
            "101%",
            "relative memory size must be in [0, 100] but is 101",
        );
    }

    #[track_caller]
    fn assert_ok(s: &'static str, expected: usize) {
        let parsed: MemorySize = s.parse().unwrap();
        assert_eq!(parsed.bytes(), expected);
    }

    #[track_caller]
    fn assert_gt_zero(s: &'static str) {
        let parsed: MemorySize = s.parse().unwrap();
        assert!(parsed.bytes() > 0);
    }

    #[track_caller]
    fn assert_err(s: &'static str, expected: &'static str) {
        let err = MemorySize::from_str(s).unwrap_err();
        assert_eq!(err, expected);
    }
}
