//! Helper types to express memory size.

use std::{str::FromStr, sync::OnceLock};

use sysinfo::{ProcessRefreshKind, ProcessesToUpdate, System, get_current_pid};

use crate::size_units::{MB, UNITS, parse_unit_suffix, unit_suffixes};

/// Memory size in bytes with required unit suffix or percentage.
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
/// Bare numbers are rejected: sizes always carry an explicit unit, because
/// bare numbers have historically meant megabytes for some options and
/// bytes for others.
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
        if usize::from_str(&s).is_ok() {
            return Err(format!(
                "bare number '{s}' is not accepted as a memory size: specify \
                 an explicit unit suffix ({}, e.g. '{s}b' for bytes) or a \
                 percentage of total memory (e.g. '20%')",
                unit_suffixes()
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

/// Byte size with required unit suffix. Does not accept percentages.
///
/// # Parsing
/// This can be parsed from strings with a unit suffix (case-insensitive):
///
/// - `b` for bytes, e.g. `1048576b`
/// - `kb` for kilobytes, e.g. `1024kb`
/// - `mb` for megabytes, e.g. `100mb`
/// - `gb` for gigabytes, e.g. `2gb`
/// - `tb` for terabytes, e.g. `1tb`
///
/// Whitespace before the suffix is allowed, e.g. `5 mb`.
///
/// Bare numbers are rejected: sizes always carry an explicit unit, because
/// bare numbers have historically meant megabytes for some options and
/// bytes for others. Unlike [`MemorySize`], percentages are not accepted
/// since this type represents absolute file/data sizes rather than memory
/// allocations.
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
        if s.ends_with('%') {
            return Err(format!(
                "percentage '{s}' is not supported for this option: it is an \
                 absolute size, not a memory-relative one; specify a unit \
                 suffix ({})",
                unit_suffixes()
            ));
        }
        if has_unit_suffix(&s) {
            return parse_bytes_with_unit(&s).map(Self);
        }
        if usize::from_str(&s).is_ok() {
            return Err(format!(
                "bare number '{s}' is not accepted as a size: specify an \
                 explicit unit suffix ({}, e.g. '{s}b' for bytes)",
                unit_suffixes()
            ));
        }
        Err(format!(
            "failed to parse '{}' as a size: expected a number with a unit \
             suffix ({})",
            s,
            unit_suffixes()
        ))
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
/// Bare numbers used to mean megabytes here; to avoid silently changing
/// the meaning of existing configurations, they are rejected with an error
/// pointing at the explicit forms (as they are for every size type).
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
        // Bare numbers used to mean megabytes here. Sizes now always
        // require an explicit unit, so reject them with an error that
        // points at the spelling preserving the old meaning.
        if usize::from_str(&s).is_ok() {
            return Err(format!(
                "bare number '{s}' is no longer accepted for this value: it \
                 previously meant megabytes; specify an explicit unit suffix \
                 (e.g. '{s}mb' for the previous behavior) or a percentage \
                 (e.g. '20%')"
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

/// Memory size accepted in the pre-3.11 value format: bare numbers mean
/// megabytes; unit suffixes and percentages parse as in [`MemorySizeMb`].
///
/// Only for the legacy cli args so configurations written for 3.10 keep
/// starting. New option names use the strict types above.
#[derive(Debug, Clone, Copy)]
pub struct LegacyMemorySizeMb(usize);

impl LegacyMemorySizeMb {
    /// Express this size in terms of bytes (B)
    pub fn as_num_bytes(&self) -> usize {
        self.0
    }
}

impl From<LegacyMemorySizeMb> for MemorySizeMb {
    fn from(size: LegacyMemorySizeMb) -> Self {
        Self(size.0)
    }
}

impl FromStr for LegacyMemorySizeMb {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim().to_lowercase();
        if has_unit_suffix(&s) {
            return parse_memory_with_unit(&s).map(Self);
        }
        // The pre-3.11 interpretation of a bare number.
        if let Ok(num_mb) = usize::from_str(&s) {
            return num_mb
                .checked_mul(MB)
                .map(Self)
                .ok_or_else(|| format!("'{s}' megabytes overflows the maximum size in bytes"));
        }
        Err(format!(
            "failed to parse '{}' as a memory size: expected a number, \
             optionally with a unit suffix ({}) or a percentage",
            s,
            unit_suffixes()
        ))
    }
}

/// Byte size that still accepts the pre-3.11 bare-number form, which means
/// bytes. Percentages are rejected as in [`ByteSize`].
///
/// `--max-http-request-size` kept its name across the switch to required
/// unit suffixes, so there is no legacy alias to key leniency on; instead
/// bare numbers keep their old meaning and [`Self::is_bare`] lets the caller
/// warn that a unit should be used.
#[derive(Debug, Clone, Copy)]
pub struct LenientByteSize {
    bytes: usize,
    bare: bool,
}

impl LenientByteSize {
    pub fn as_num_bytes(&self) -> usize {
        self.bytes
    }

    /// True when parsed from a bare number without a unit suffix.
    pub fn is_bare(&self) -> bool {
        self.bare
    }
}

impl FromStr for LenientByteSize {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim().to_lowercase();
        if s.ends_with('%') {
            return Err(format!(
                "percentage '{s}' is not supported for this option: it is an \
                 absolute size, not a memory-relative one; specify a unit \
                 suffix ({})",
                unit_suffixes()
            ));
        }
        if has_unit_suffix(&s) {
            return parse_bytes_with_unit(&s).map(|bytes| Self { bytes, bare: false });
        }
        // The pre-3.11 interpretation of a bare number.
        if let Ok(bytes) = usize::from_str(&s) {
            return Ok(Self { bytes, bare: true });
        }
        Err(format!(
            "failed to parse '{}' as a size: expected a number, optionally \
             with a unit suffix ({})",
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
