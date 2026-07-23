//! Helper types to express memory size.

use std::{str::FromStr, sync::OnceLock};

use sysinfo::{ProcessRefreshKind, ProcessesToUpdate, System, get_current_pid};

use crate::size_units::{UNITS, parse_unit_suffix, unit_suffixes};

/// Memory size in bytes with optional unit suffix.
///
/// # Parsing
/// This can be parsed from strings in one of the following formats:
///
/// - **absolute (default bytes):** a plain non-negative number specifies the size in bytes,
///   e.g. `1048576`
/// - **with unit suffix:** append a unit suffix (case-insensitive) for explicit sizing:
///   - `b` for bytes, e.g. `1048576b`
///   - `kb` for kilobytes, e.g. `1024kb`
///   - `mb` for megabytes, e.g. `100mb`
///   - `gb` for gigabytes, e.g. `2gb`
///   - `tb` for terabytes, e.g. `1tb`
/// - **relative:** a percentage between 0 and 100 (inclusive) of total available memory, e.g. `50%`
///   (see [`total_mem_bytes`] for how total memory is determined)
///
/// Whitespace before the suffix is allowed, e.g. `5 mb`.
///
/// Unlike [`MemorySizeMb`], bare numbers are interpreted as bytes rather than megabytes.
///
#[derive(Debug, Clone, Copy)]
pub struct MemorySize(usize);

impl MemorySize {
    /// Express this size in terms of bytes (B)
    pub fn as_num_bytes(&self) -> usize {
        self.0
    }
}

impl FromStr for MemorySize {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim().to_lowercase();
        if has_unit_suffix(&s) {
            return parse_memory_with_unit(&s).map(Self);
        }
        // Bare number = bytes
        usize::from_str(&s)
            .map(Self)
            .map_err(|e| format!("failed to parse '{}' as a memory size in bytes: {}", s, e))
    }
}

/// Byte size with optional unit suffix. Does not accept percentages.
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
/// Whitespace before the suffix is allowed, e.g. `5 mb`.
///
/// Unlike [`MemorySize`], percentages are not accepted since this type
/// represents absolute file/data sizes rather than memory allocations.
#[derive(Debug, Clone, Copy)]
pub struct ByteSize(usize);

impl ByteSize {
    pub fn as_num_bytes(&self) -> usize {
        self.0
    }
}

impl FromStr for ByteSize {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim().to_lowercase();
        if has_unit_suffix(&s) {
            return parse_bytes_with_unit(&s).map(Self);
        }
        // Bare number = bytes
        usize::from_str(&s)
            .map(Self)
            .map_err(|e| format!("failed to parse '{}' as a byte size in bytes: {}", s, e))
    }
}

/// Memory size with required unit suffix or percentage.
///
/// # Parsing
/// This can be parsed from strings in one of the following formats:
///
/// - **with unit suffix:** append a unit suffix (case-insensitive) for explicit sizing:
///   - `b` for bytes, e.g. `1048576b`
///   - `kb` for kilobytes, e.g. `1024kb`
///   - `mb` for megabytes, e.g. `100mb`
///   - `gb` for gigabytes, e.g. `2gb`
///   - `tb` for terabytes, e.g. `1tb`
/// - **relative:** a percentage between 0 and 100 (inclusive) of total available memory, e.g. `50%`
///   (see [`total_mem_bytes`] for how total memory is determined)
///
/// Whitespace before the suffix is allowed, e.g. `5 mb`.
///
/// Bare numbers used to mean megabytes and will mean bytes in a future
/// release; to avoid silently changing the meaning of existing
/// configurations, they are rejected with a transitional error in the
/// meantime.
///
/// For new CLI arguments, prefer [`MemorySize`].
///
#[derive(Debug, Clone, Copy, Default)]
pub struct MemorySizeMb(usize);

impl MemorySizeMb {
    /// Express this size in terms of bytes (B)
    pub fn as_num_bytes(&self) -> usize {
        self.0
    }
}

impl FromStr for MemorySizeMb {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim().to_lowercase();
        // If it has a recognized suffix, use strict parsing
        if has_unit_suffix(&s) {
            return parse_memory_with_unit(&s).map(Self);
        }
        // Transitional: bare numbers used to mean megabytes here, but mean
        // bytes for the other size types and will mean bytes here too in a
        // future release. Reject them for now so the meaning of existing
        // configurations never changes silently.
        if usize::from_str(&s).is_ok() {
            return Err(format!(
                "bare number '{s}' is no longer accepted for this value: it \
                 previously meant megabytes and will mean bytes in a future \
                 release; specify an explicit unit suffix (e.g. '{s}mb' for \
                 the previous behavior) or a percentage (e.g. '20%')"
            ));
        }
        Err(format!(
            "failed to parse '{}' as a memory size: expected a number with a \
             unit suffix ({}) or a percentage",
            s,
            unit_suffixes()
        ))
    }
}

/// Check if a string has a recognized unit suffix (%, tb, gb, mb, kb, b).
fn has_unit_suffix(s: &str) -> bool {
    s.ends_with('%') || UNITS.iter().any(|(suffix, _, _)| s.ends_with(suffix))
}

/// Parse a memory size string that has an explicit unit suffix or percentage.
///
/// Expects a trimmed, lowercased input. Returns the size in bytes.
fn parse_memory_with_unit(s: &str) -> Result<usize, String> {
    if let Some(num_str) = s.strip_suffix('%') {
        let percentage = u64::from_str(num_str.trim()).map_err(|e| {
            format!(
                "failed to parse '{}' as a percentage of available memory: {}",
                s, e
            )
        })?;
        if percentage > 100 {
            return Err(format!(
                "relative memory size must be in [0, 100] but is {percentage}"
            ));
        }
        let total = total_mem_bytes();
        return Ok((percentage as f64 / 100f64 * total as f64).round() as usize);
    }

    parse_unit_suffix(s).unwrap_or_else(|| {
        Err(format!(
            "'{}' requires a unit suffix ({}) or percentage (%)",
            s,
            unit_suffixes()
        ))
    })
}

/// Parse a byte size string that has an explicit unit suffix.
///
/// Expects a trimmed, lowercased input. Returns the size in bytes.
/// Unlike [`parse_memory_with_unit`], percentages are not accepted.
fn parse_bytes_with_unit(s: &str) -> Result<usize, String> {
    parse_unit_suffix(s).unwrap_or_else(|| {
        Err(format!(
            "'{}' requires a unit suffix ({})",
            s,
            unit_suffixes()
        ))
    })
}

/// Total available memory in bytes.
///
/// Memory limits are resolved in the following order, stopping at the first valid value:
///
///   - the current process's cgroup memory limit (cgroup v2 `memory.max` or cgroup v1
///     `memory.limit_in_bytes`), discovered via `/proc/self/cgroup` and taking the most
///     restrictive limit up the cgroup hierarchy — this captures per-process limits set
///     with `systemd-run` without namespace isolation
///   - platform-specific syscall for total system RAM (fallback)
pub fn total_mem_bytes() -> usize {
    static TOTAL_MEM_BYTES: OnceLock<usize> = OnceLock::new();
    *TOTAL_MEM_BYTES.get_or_init(get_memory_limit)
}

/// Returns a percentage of total memory, capped at a maximum value.
pub fn percent_of_total_mem_capped(percent: u8, cap_bytes: usize) -> usize {
    let total = total_mem_bytes();
    let amount = (total as f64 * percent as f64 / 100.0) as usize;
    amount.min(cap_bytes)
}

/// Threshold (as a fraction of detected process memory) at which configured
/// memory reservations are considered close enough to the available total to
/// warrant warning the operator.
pub const MEMORY_RESERVATION_WARN_THRESHOLD: f64 = 0.90;

/// Returns true when `reserved` is at or above
/// [`MEMORY_RESERVATION_WARN_THRESHOLD`] of `detected`.
///
/// Returns false when `detected` is zero — without a known total there is no
/// meaningful signal to warn against.
pub fn should_warn_memory_reservations(detected: usize, reserved: usize) -> bool {
    detected > 0 && reserved as f64 / detected as f64 >= MEMORY_RESERVATION_WARN_THRESHOLD
}

fn get_memory_limit() -> usize {
    let mut sys = System::new();
    sys.refresh_memory();

    let cgroup_limit = get_current_pid().ok().and_then(|pid| {
        sys.refresh_processes_specifics(
            ProcessesToUpdate::Some(&[pid]),
            true,
            ProcessRefreshKind::nothing(),
        );
        sys.process(pid).and_then(|p| p.cgroup_limits())
    });

    cgroup_limit
        .map(|v| v.total_memory)
        .unwrap_or_else(|| sys.total_memory()) as usize
}

/// Format a byte count as a human-readable string.
pub fn format_bytes(bytes: usize) -> String {
    const GIB: usize = 1024 * 1024 * 1024;
    const MIB: usize = 1024 * 1024;
    const KIB: usize = 1024;
    if bytes >= GIB {
        format!("{:.2}GiB", bytes as f64 / GIB as f64)
    } else if bytes >= MIB {
        format!("{:.2}MiB", bytes as f64 / MIB as f64)
    } else if bytes >= KIB {
        format!("{:.2}KiB", bytes as f64 / KIB as f64)
    } else {
        format!("{}B", bytes)
    }
}

#[cfg(test)]
mod tests;
