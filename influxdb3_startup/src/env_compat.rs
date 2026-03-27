//! Backwards compatibility for environment variable names.
//!
//! This module provides aliasing from new `INFLUXDB3_` prefixed environment
//! variables to their legacy unprefixed names, enabling backwards compatibility
//! while encouraging migration to the new naming convention.

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
        "INFLUXDB3_OBJECT_STORE_CACHE_ENDPOINT",
        "OBJECT_STORE_CACHE_ENDPOINT",
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
];

/// Copy deprecated environment variable values to their new prefixed names.
///
/// Processes all provided aliases. For each `(new_name, old_name)` pair:
/// - If only the old name is set, copies the value to the new name (with a deprecation warning)
/// - If both are set, uses the new name and copies it to the old name (with a warning)
/// - If only the new name is set, copies it to the old name so external crates see it
///
/// # Safety
///
/// This function must be called single-threaded during startup, before any
/// threads are spawned
pub fn copy_env_aliases(aliases: &[(&str, &str)]) {
    for (new_name, old_name) in aliases {
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
            }
            (Ok(_), Ok(new_val)) => {
                // Both are set - use new name, warn about old being ignored
                early_logging::warn(
                    LOG_TARGET,
                    &format!(
                        "both {old_name} and {new_name} are set; using {new_name}, as {old_name} is deprecated"
                    ),
                );
                // Copy new value to old name so external crates see it
                unsafe {
                    env::set_var(old_name, new_val);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shared_aliases_count() {
        // 9 object store generic + 3 logging + 8 tracing = 20
        assert_eq!(ENV_ALIASES.len(), 20);
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
    fn test_shared_aliases_old_names_dont_have_influxdb3_prefix() {
        for (_new_name, old_name) in ENV_ALIASES {
            assert!(
                !old_name.starts_with("INFLUXDB3_"),
                "Old name {old_name} should not start with INFLUXDB3_",
            );
        }
    }
}
