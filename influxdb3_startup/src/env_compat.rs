//! Backwards compatibility for environment variable names.
//!
//! This module provides aliasing from new `INFLUXDB3_` prefixed environment
//! variables to their legacy unprefixed names, enabling backwards compatibility
//! while encouraging migration to the new naming convention.

use std::collections::HashMap;
use std::env;

use crate::early_logging;

const LOG_TARGET: &str = "influxdb3::env_compat";

/// Mapping of `(new_name, old_name)` for environment variable aliases.
///
/// When the old name is set but the new name is not, the value is
/// copied to the new name and a deprecation warning is emitted.
pub const ENV_ALIASES: &[(&str, &str)] = &[
    // Object Store generic
    (
        "INFLUXDB3_OBJECT_STORE_CONNECTION_LIMIT",
        "OBJECT_STORE_CONNECTION_LIMIT",
    ),
    (
        "INFLUXDB3_OBJECT_STORE_HTTP2_ONLY",
        "OBJECT_STORE_HTTP2_ONLY",
    ),
    (
        "INFLUXDB3_OBJECT_STORE_HTTP2_MAX_FRAME_SIZE",
        "OBJECT_STORE_HTTP2_MAX_FRAME_SIZE",
    ),
    (
        "INFLUXDB3_OBJECT_STORE_REQUEST_TIMEOUT",
        "OBJECT_STORE_REQUEST_TIMEOUT",
    ),
    (
        "INFLUXDB3_OBJECT_STORE_MAX_RETRIES",
        "OBJECT_STORE_MAX_RETRIES",
    ),
    (
        "INFLUXDB3_OBJECT_STORE_RETRY_TIMEOUT",
        "OBJECT_STORE_RETRY_TIMEOUT",
    ),
    (
        "INFLUXDB3_OBJECT_STORE_TLS_ALLOW_INSECURE",
        "OBJECT_STORE_TLS_ALLOW_INSECURE",
    ),
    ("INFLUXDB3_OBJECT_STORE_TLS_CA", "OBJECT_STORE_TLS_CA"),
    // Logging (external crate: trogging)
    ("INFLUXDB3_LOG_FILTER", "LOG_FILTER"),
    ("INFLUXDB3_LOG_DESTINATION", "LOG_DESTINATION"),
    ("INFLUXDB3_LOG_FORMAT", "LOG_FORMAT"),
    // Tracing (external crate: trace_exporters)
    ("INFLUXDB3_TRACES_EXPORTER", "TRACES_EXPORTER"),
    (
        "INFLUXDB3_TRACES_EXPORTER_JAEGER_AGENT_HOST",
        "TRACES_EXPORTER_JAEGER_AGENT_HOST",
    ),
    (
        "INFLUXDB3_TRACES_EXPORTER_JAEGER_AGENT_PORT",
        "TRACES_EXPORTER_JAEGER_AGENT_PORT",
    ),
    (
        "INFLUXDB3_TRACES_EXPORTER_JAEGER_SERVICE_NAME",
        "TRACES_EXPORTER_JAEGER_SERVICE_NAME",
    ),
    (
        "INFLUXDB3_TRACES_EXPORTER_JAEGER_TRACE_CONTEXT_HEADER_NAME",
        "TRACES_EXPORTER_JAEGER_TRACE_CONTEXT_HEADER_NAME",
    ),
    (
        "INFLUXDB3_TRACES_EXPORTER_JAEGER_DEBUG_NAME",
        "TRACES_EXPORTER_JAEGER_DEBUG_NAME",
    ),
    (
        "INFLUXDB3_TRACES_EXPORTER_JAEGER_TAGS",
        "TRACES_EXPORTER_JAEGER_TAGS",
    ),
    (
        "INFLUXDB3_TRACES_JAEGER_MAX_MSGS_PER_SECOND",
        "TRACES_JAEGER_MAX_MSGS_PER_SECOND",
    ),
    // Tokio Console
    ("INFLUXDB3_TOKIO_CONSOLE_ENABLED", "TOKIO_CONSOLE_ENABLED"),
    (
        "INFLUXDB3_TOKIO_CONSOLE_CLIENT_BUFFER_CAPACITY",
        "TOKIO_CONSOLE_CLIENT_BUFFER_CAPACITY",
    ),
    (
        "INFLUXDB3_TOKIO_CONSOLE_EVENT_BUFFER_CAPACITY",
        "TOKIO_CONSOLE_EVENT_BUFFER_CAPACITY",
    ),
    // Renamed to match their CLI flag names (#4237)
    (
        "INFLUXDB3_DISABLE_TELEMETRY_UPLOAD",
        "INFLUXDB3_TELEMETRY_DISABLE_UPLOAD",
    ),
    ("INFLUXDB3_WITHOUT_AUTH", "INFLUXDB3_START_WITHOUT_AUTH"),
    ("INFLUXDB3_NODE_ID", "INFLUXDB3_NODE_IDENTIFIER_PREFIX"),
    (
        "INFLUXDB3_NODE_ID_FROM_ENV",
        "INFLUXDB3_NODE_IDENTIFIER_FROM_ENV",
    ),
    ("INFLUXDB3_DATA_DIR", "INFLUXDB3_DB_DIR"),
    // Parquet cache flags collapsed into engine-agnostic file cache names (#4238)
    (
        "INFLUXDB3_FILE_CACHE_SIZE",
        "INFLUXDB3_PARQUET_MEM_CACHE_SIZE",
    ),
    (
        "INFLUXDB3_FILE_CACHE_RECENCY",
        "INFLUXDB3_PARQUET_MEM_CACHE_QUERY_PATH_DURATION",
    ),
    // Read by the Enterprise serve config and by Core's `debug catalog`,
    // so the alias must serve both binaries.
    ("INFLUXDB3_CLUSTER_ID", "INFLUXDB3_ENTERPRISE_CLUSTER_ID"),
    // Both legacy spellings of the cache-disable switch map directly to the
    // canonical INFLUXDB3_DISABLE_FILE_CACHE (Core and Enterprise share the
    // flag name --disable-file-cache). Transitional spelling first so it
    // wins over the released spelling if both are set.
    (
        "INFLUXDB3_DISABLE_FILE_CACHE",
        "INFLUXDB3_DISABLE_DATA_FILE_CACHE",
    ),
    (
        "INFLUXDB3_DISABLE_FILE_CACHE",
        "INFLUXDB3_DISABLE_PARQUET_MEM_CACHE",
    ),
    // WAL snapshot flags renamed to describe what they count (#4237/#4238)
    (
        "INFLUXDB3_WAL_FILES_PER_SNAPSHOT",
        "INFLUXDB3_WAL_SNAPSHOT_SIZE",
    ),
    (
        "INFLUXDB3_WAL_MAX_BUFFERED_WRITES",
        "INFLUXDB3_WAL_MAX_WRITE_BUFFER_SIZE",
    ),
    (
        "INFLUXDB3_SNAPSHOTTED_WAL_FILES_TO_KEEP",
        "INFLUXDB3_NUM_WAL_FILES_TO_KEEP",
    ),
    // Renamed to match their new flag names (#4237)
    (
        "INFLUXDB3_EXEC_MEM_POOL_SIZE",
        "INFLUXDB3_EXEC_MEM_POOL_BYTES",
    ),
    (
        "INFLUXDB3_QUERY_LOG_MAX_ENTRIES",
        "INFLUXDB3_QUERY_LOG_SIZE",
    ),
    // This one fixes a LISTINER -> LISTENER typo; both binaries read the
    // corrected spelling
    (
        "INFLUXDB3_TCP_LISTENER_FILE_PATH",
        "INFLUXDB3_TCP_LISTINER_FILE_PATH",
    ),
];

/// Copy deprecated environment variable values to their new prefixed names.
///
/// Processes all provided aliases. For each `(new_name, old_name)` pair:
/// - If only the old name is set, copies the value to the new name (with a deprecation warning)
/// - If both are set with different values, uses the new name and copies it to the old name,
///   warning with the names the operator actually set: when the new name's value was itself
///   placed by an earlier pair in this pass (a target with multiple legacy spellings), the
///   warning names that pair's old variable rather than the new name
/// - If both are set with the same value, nothing to reconcile - no warning
/// - If only the new name is set, copies it to the old name so external crates see it
///
/// # Safety
///
/// This function must be called single-threaded during startup, before any
/// threads are spawned
pub fn copy_env_aliases(aliases: &[(&str, &str)]) {
    // Records where a value now under `new_name` came from: the `old_name` of
    // the earlier pair in this pass that placed it. Lets the both-set warning
    // name the variables the operator actually set when one canonical name
    // has multiple legacy spellings (e.g. the cache-disable switch).
    let mut placed_from: HashMap<&str, &str> = HashMap::new();

    for &(new_name, old_name) in aliases {
        let old_value = env::var(old_name);
        let new_value = env::var(new_name);

        match (old_value, new_value) {
            (Ok(old_val), Err(_)) => {
                // Old name is set, new name is not - copy and warn
                early_logging::warn(
                    LOG_TARGET,
                    &format!(
                        "environment variable {old_name} is deprecated, use {new_name} instead"
                    ),
                );
                unsafe {
                    env::set_var(new_name, old_val);
                }
                placed_from.insert(new_name, old_name);
            }
            (Ok(old_val), Ok(new_val)) => {
                // Both are set. Equal values need no reconciliation (and the
                // copy below would be a no-op) - stay quiet.
                if old_val != new_val {
                    let message = match placed_from.get(new_name) {
                        // The value under the new name came from an earlier
                        // legacy spelling - warn with the two variables the
                        // operator actually set, and which one won.
                        Some(prior_old) => format!(
                            "both {prior_old} and {old_name} are set (legacy spellings of \
                             {new_name}); using the value from {prior_old}"
                        ),
                        None => format!(
                            "both {old_name} and {new_name} are set; using {new_name}, as \
                             {old_name} is deprecated"
                        ),
                    };
                    early_logging::warn(LOG_TARGET, &message);
                    // Copy the winning value to the old name so external
                    // crates see it
                    unsafe {
                        env::set_var(old_name, new_val);
                    }
                }
            }
            (Err(_), Ok(new_val)) => {
                // Only new name is set - copy to old name so external crates see it
                unsafe {
                    env::set_var(old_name, new_val);
                }
            }
            (Err(_), Err(_)) => {
                // Neither is set - no action needed
            }
        }
    }
}

/// Environment variable prefixes that are no longer recognized: PachaTree
/// options dropped the `PT_` prefix (#4238), so `INFLUXDB3_PT_*` and
/// `INFLUXDB3_ENTERPRISE_PT_*` no longer map to any option.
pub const DROPPED_PACHA_ENV_PREFIXES: &[&str] = &["INFLUXDB3_PT_", "INFLUXDB3_ENTERPRISE_PT_"];

/// Return the names from `var_names` that start with one of the
/// [`DROPPED_PACHA_ENV_PREFIXES`], sorted.
pub fn find_dropped_pacha_env_vars<I, S>(var_names: I) -> Vec<String>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let mut dropped: Vec<String> = var_names
        .into_iter()
        .map(Into::into)
        .filter(|name| {
            DROPPED_PACHA_ENV_PREFIXES
                .iter()
                .any(|prefix| name.starts_with(prefix))
        })
        .collect();
    dropped.sort();
    dropped
}

/// Warn about any set environment variables that use the dropped
/// `INFLUXDB3_PT_` / `INFLUXDB3_ENTERPRISE_PT_` prefixes.
///
/// The PachaTree flags were renamed without aliases, so a legacy env var
/// would otherwise be silently ignored — which for storage-engine tuning
/// options is worse than warning loudly at startup.
pub fn warn_dropped_pacha_env_vars() {
    for name in find_dropped_pacha_env_vars(env::vars().map(|(name, _value)| name)) {
        early_logging::warn(
            LOG_TARGET,
            &format!(
                "environment variable {name} is no longer recognized: PachaTree \
                 options dropped the PT_ prefix; see the renamed options"
            ),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_find_dropped_pacha_env_vars() {
        let found = find_dropped_pacha_env_vars([
            "INFLUXDB3_PT_SHARD_COUNT",
            "INFLUXDB3_ENTERPRISE_PT_ENABLE_AUTO_DVC",
            "INFLUXDB3_SHARD_COUNT",
            "INFLUXDB3_ENTERPRISE_MODE",
            "PATH",
        ]);
        assert_eq!(
            found,
            vec![
                "INFLUXDB3_ENTERPRISE_PT_ENABLE_AUTO_DVC".to_string(),
                "INFLUXDB3_PT_SHARD_COUNT".to_string(),
            ]
        );
    }

    #[test]
    fn test_find_dropped_pacha_env_vars_empty() {
        assert_eq!(
            find_dropped_pacha_env_vars(["INFLUXDB3_NODE_ID", "HOME"]),
            Vec::<String>::new()
        );
    }

    #[test]
    fn test_copy_env_aliases_multi_spelling_first_pair_wins() {
        // Two legacy spellings of one canonical name: table order decides the
        // winner, and the losing spelling is reconciled to the winning value.
        const NEW: &str = "ECT_MULTI_NEW";
        const OLD_A: &str = "ECT_MULTI_OLD_A";
        const OLD_B: &str = "ECT_MULTI_OLD_B";
        let clear = || {
            for name in [NEW, OLD_A, OLD_B] {
                unsafe { env::remove_var(name) };
            }
        };
        clear();
        unsafe {
            env::set_var(OLD_A, "from-a");
            env::set_var(OLD_B, "from-b");
        }
        copy_env_aliases(&[(NEW, OLD_A), (NEW, OLD_B)]);
        assert_eq!(env::var(NEW).as_deref(), Ok("from-a"));
        assert_eq!(env::var(OLD_B).as_deref(), Ok("from-a"));
        clear();
    }

    #[test]
    fn test_copy_env_aliases_equal_values_reconcile_silently() {
        // Agreeing values are left untouched (and produce no warning).
        const NEW: &str = "ECT_EQ_NEW";
        const OLD: &str = "ECT_EQ_OLD";
        let clear = || {
            for name in [NEW, OLD] {
                unsafe { env::remove_var(name) };
            }
        };
        clear();
        unsafe {
            env::set_var(OLD, "same");
            env::set_var(NEW, "same");
        }
        copy_env_aliases(&[(NEW, OLD)]);
        assert_eq!(env::var(NEW).as_deref(), Ok("same"));
        assert_eq!(env::var(OLD).as_deref(), Ok("same"));
        clear();
    }

    #[test]
    fn test_copy_env_aliases_user_set_new_name_wins() {
        const NEW: &str = "ECT_WIN_NEW";
        const OLD: &str = "ECT_WIN_OLD";
        let clear = || {
            for name in [NEW, OLD] {
                unsafe { env::remove_var(name) };
            }
        };
        clear();
        unsafe {
            env::set_var(OLD, "legacy");
            env::set_var(NEW, "canonical");
        }
        copy_env_aliases(&[(NEW, OLD)]);
        assert_eq!(env::var(NEW).as_deref(), Ok("canonical"));
        assert_eq!(env::var(OLD).as_deref(), Ok("canonical"));
        clear();
    }

    #[test]
    fn test_shared_aliases_count() {
        // 8 object store generic + 3 logging + 8 tracing + 3 tokio console
        // + 5 flag-name renames (#4237) + 4 file cache entries (#4238:
        // size, recency, and two legacy spellings of the disable switch)
        // + 3 WAL snapshot renames (#4237/#4238)
        // + exec-mem-pool-size + query-log-max-entries (#4237)
        // + the LISTINER -> LISTENER typo fix
        // + cluster-id (#4238; read by Core debug catalog) = 38
        assert_eq!(ENV_ALIASES.len(), 38);
    }

    #[test]
    fn test_shared_aliases_all_have_influxdb3_prefix() {
        for (new_name, _old_name) in ENV_ALIASES {
            assert!(
                new_name.starts_with("INFLUXDB3_"),
                "New name {new_name} should start with INFLUXDB3_",
            );
        }
    }

    #[test]
    fn test_shared_aliases_old_names_differ_from_new_names() {
        for (new_name, old_name) in ENV_ALIASES {
            assert_ne!(
                new_name, old_name,
                "Alias pair must map two different names",
            );
        }
    }
}
