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

/// Similar to [`MemorySize`] but allows specifying absolute sizes in megabytes (MB) instead of
/// bytes (B)
#[derive(Debug, Clone, Copy)]
pub struct MemorySizeMb(usize);

impl MemorySizeMb {
    /// Express this cache size in terms of bytes (B)
    pub fn as_num_bytes(&self) -> usize {
        self.0
    }
}

impl FromStr for MemorySizeMb {
    type Err = String;

    fn from_str(s: &str) -> std::prelude::v1::Result<Self, Self::Err> {
        let s_lower = s.to_lowercase();
        let num_bytes = if s_lower.contains('%') {
            let mem_size = MemorySize::from_str(s).map_err(|e| {
                format!(
                    "failed to parse '{}' as a percentage of available memory: {}",
                    s, e
                )
            })?;
            mem_size.bytes()
        } else if let Some(num_str) = s_lower.strip_suffix("gb") {
            let num = usize::from_str(num_str.trim())
                .map_err(|e| format!("failed to parse '{}' as a memory size in GB: {}", s, e))?;
            num * 1024 * 1024 * 1024
        } else if let Some(num_str) = s_lower.strip_suffix("mb") {
            let num = usize::from_str(num_str.trim())
                .map_err(|e| format!("failed to parse '{}' as a memory size in MB: {}", s, e))?;
            num * 1024 * 1024
        } else if let Some(num_str) = s_lower.strip_suffix("kb") {
            let num = usize::from_str(num_str.trim())
                .map_err(|e| format!("failed to parse '{}' as a memory size in KB: {}", s, e))?;
            num * 1024
        } else if let Some(num_str) = s_lower.strip_suffix('b') {
            usize::from_str(num_str.trim())
                .map_err(|e| format!("failed to parse '{}' as a memory size in bytes: {}", s, e))?
        } else {
            let num_mb = usize::from_str(s.trim())
                .map_err(|e| format!("failed to parse '{}' as a memory size in MB: {}", s, e))?;
            num_mb * 1024 * 1024
        };
        Ok(Self(num_bytes))
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
    fn test_parse_memory_size() {
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

    #[test]
    fn test_parse_memory_size_mb() {
        // Plain number = megabytes
        assert_mb_ok("0", 0);
        assert_mb_ok("1", 1024 * 1024);
        assert_mb_ok("5", 5 * 1024 * 1024);
        assert_mb_ok("100", 100 * 1024 * 1024);

        // With 'mb' suffix (case insensitive)
        assert_mb_ok("5mb", 5 * 1024 * 1024);
        assert_mb_ok("5MB", 5 * 1024 * 1024);
        assert_mb_ok("5Mb", 5 * 1024 * 1024);
        assert_mb_ok("100mb", 100 * 1024 * 1024);

        // With 'kb' suffix
        assert_mb_ok("1kb", 1024);
        assert_mb_ok("512kb", 512 * 1024);
        assert_mb_ok("1024KB", 1024 * 1024);

        // With 'gb' suffix
        assert_mb_ok("1gb", 1024 * 1024 * 1024);
        assert_mb_ok("2GB", 2 * 1024 * 1024 * 1024);

        // With 'b' suffix (raw bytes)
        assert_mb_ok("1024b", 1024);
        assert_mb_ok("1048576b", 1048576);

        // Percentage
        assert_mb_gt_zero("50%");
        assert_mb_ok("0%", 0);

        // With whitespace
        assert_mb_ok(" 5 mb", 5 * 1024 * 1024);
        assert_mb_ok("5 MB", 5 * 1024 * 1024);
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

    #[track_caller]
    fn assert_mb_ok(s: &'static str, expected: usize) {
        let parsed: MemorySizeMb = s.parse().unwrap();
        assert_eq!(parsed.as_num_bytes(), expected, "parsing '{}'", s);
    }

    #[track_caller]
    fn assert_mb_gt_zero(s: &'static str) {
        let parsed: MemorySizeMb = s.parse().unwrap();
        assert!(parsed.as_num_bytes() > 0);
    }
}
