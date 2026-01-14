//! Backwards compatibility for environment variable names.
//!
//! This module provides aliasing from new INFLUXDB3_ prefixed environment
//! variables to their legacy unprefixed names, enabling backwards compatibility
//! while encouraging migration to the new naming convention.

use chrono::Utc;
use std::io::IsTerminal;

/// Print a warning message formatted like the logging system output.
///
/// This is used because these warnings are emitted before the logging system
/// is initialized, but we want consistent-looking output.
fn warn(message: &str) {
    let now = Utc::now().format("%Y-%m-%dT%H:%M:%S%.6fZ");
    if std::io::stderr().is_terminal() {
        // Yellow WARN for TTY output
        eprintln!("{now} \x1b[33m WARN\x1b[0m influxdb3::env_compat: {message}");
    } else {
        eprintln!("{now}  WARN influxdb3::env_compat: {message}");
    }
}

/// Mapping of (new_name, old_name) for environment variable aliases.
///
/// When the old name is set but the new name is not, we copy the value
/// to the new name and emit a deprecation warning.
const ENV_ALIASES: &[(&str, &str)] = &[
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
/// This function should be called BEFORE clap parsing to ensure backwards
/// compatibility. If both old and new names are set, the new name takes
/// precedence and a warning is emitted.
///
/// # Note
///
/// This function uses `eprintln!` for warnings because it runs before the
/// logging system is initialized.
pub(crate) fn copy_deprecated_env_aliases() {
    for (new_name, old_name) in ENV_ALIASES {
        let old_value = std::env::var(old_name);
        let new_value = std::env::var(new_name);

        match (old_value, new_value) {
            (Ok(old_val), Err(_)) => {
                // Old name is set, new name is not - copy and warn
                warn(&format!(
                    "environment variable {old_name} is deprecated, use {new_name} instead"
                ));
                // SAFETY: This is called single-threaded during startup in lib.rs::startup()
                // before any threads are spawned (tokio runtime not yet initialized).
                unsafe {
                    std::env::set_var(new_name, old_val);
                }
            }
            (Ok(_), Ok(new_val)) => {
                // Both are set - use new name, warn about old being ignored
                warn(&format!(
                    "both {old_name} and {new_name} are set; using {new_name}, as {old_name} is deprecated"
                ));
                // Copy new value to old name so external crates see it
                unsafe {
                    std::env::set_var(old_name, new_val);
                }
            }
            (Err(_), Ok(new_val)) => {
                // Only new name is set - copy to old name so external crates see it
                unsafe {
                    std::env::set_var(old_name, new_val);
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
    fn test_env_aliases_count() {
        // Ensure we have all expected aliases:
        // - 9 object store generic vars
        // - 3 logging vars (trogging crate)
        // - 8 tracing vars (trace_exporters crate)
        assert_eq!(ENV_ALIASES.len(), 20);
    }

    #[test]
    fn test_env_aliases_all_have_influxdb3_prefix() {
        for (new_name, _old_name) in ENV_ALIASES {
            assert!(
                new_name.starts_with("INFLUXDB3_"),
                "New name {} should start with INFLUXDB3_",
                new_name
            );
        }
    }

    #[test]
    fn test_env_aliases_old_names_dont_have_influxdb3_prefix() {
        for (_new_name, old_name) in ENV_ALIASES {
            assert!(
                !old_name.starts_with("INFLUXDB3_"),
                "Old name {} should not start with INFLUXDB3_",
                old_name
            );
        }
    }
}
