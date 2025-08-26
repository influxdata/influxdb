//! CLI parameter capture for storing user-provided configuration, filtering out sensitive parameters.

use std::collections::HashMap;

/// List of all known sensitive CLI parameters
const SENSITIVE_PARAMS: &[&str] = &[
    "aws-access-key-id",
    "aws-secret-access-key",
    "aws-session-token",
    "aws-credentials-file",
    "aws-endpoint",
    "aws-allow-http",
    "aws-default-region",
    "aws-skip-signature",
    "azure-endpoint",
    "azure-allow-http",
    "azure-storage-account",
    "azure-storage-access-key",
    "google-service-account",
    "tls-key",
    "tls-cert",
    "without-auth",
];

/// List of all known non-sensitive CLI parameters - only used in test but declared at module level
/// for visibility
#[allow(dead_code)]
const NON_SENSITIVE_PARAMS: &[&str] = &[
    // Core parameters
    "node-id",
    "http-bind",
    "max-http-request-size",
    "object-store",
    "data-dir",
    "verbose",
    "bucket",
    // Memory and performance parameters
    "exec-mem-pool-bytes",
    "num-datafusion-threads",
    "query-file-limit",
    "force-snapshot-mem-threshold",
    // WAL parameters
    "wal-flush-interval",
    "wal-snapshot-size",
    "wal-max-write-buffer-size",
    "wal-replay-fail-on-error",
    "wal-replay-concurrency-limit",
    "snapshotted-wal-files-to-keep",
    // Cache parameters
    "parquet-mem-cache-size",
    "parquet-mem-cache-prune-percentage",
    "parquet-mem-cache-prune-interval",
    "parquet-mem-cache-query-path-duration",
    "disable-parquet-mem-cache",
    "table-index-cache-max-entries",
    "table-index-cache-concurrency-limit",
    // Distinct cache parameters
    "distinct-cache-eviction-interval",
    // Last cache parameters
    "last-cache-eviction-interval",
    // Retention and deletion parameters
    "retention-check-interval",
    "delete-grace-period",
    "hard-delete-default-duration",
    // Generation configuration
    "gen1-duration",
    "gen1-lookback-duration",
    // Logging and tracing parameters
    "log-filter",
    "log-destination",
    "log-format",
    "query-log-size",
    "traces-exporter",
    "traces-exporter-jaeger-agent-host",
    "traces-exporter-jaeger-agent-port",
    "traces-exporter-jaeger-service-name",
    "traces-exporter-jaeger-trace-context-header-name",
    "traces-jaeger-debug-name",
    "traces-jaeger-tags",
    "traces-jaeger-max-msgs-per-second",
    // DataFusion parameters
    "datafusion-config",
    "datafusion-use-cached-parquet-loader",
    "datafusion-max-parquet-fanout",
    "datafusion-runtime-type",
    "datafusion-runtime-thread-priority",
    "datafusion-runtime-thread-keep-alive",
    "datafusion-runtime-disable-lifo-slot",
    "datafusion-runtime-event-interval",
    "datafusion-runtime-global-queue-interval",
    "datafusion-runtime-max-io-events-per-tick",
    "datafusion-runtime-max-blocking-threads",
    // Object store parameters
    "object-store-cache-endpoint",
    "object-store-connection-limit",
    "object-store-http2-only",
    "object-store-http2-max-frame-size",
    "object-store-max-retries",
    "object-store-retry-timeout",
    // Feature flags and modes
    "disable-authz",
    // Telemetry
    "telemetry-endpoint",
    "disable-telemetry-upload",
    // TLS parameters
    "tls-minimum-version",
    // Python integration
    "plugin-dir",
    "virtual-env-location",
    "package-manager",
    // Other parameters
    "tcp-listener-file-path",
];

const REDACTED_STR: &str = "*******";

/// Capture user-provided CLI parameters with sensitive values redacted
///
/// This function takes user-provided parameters extracted from ArgMatches
/// and returns a JSON string where sensitive parameters show as "*******"
/// while non-sensitive parameters show their actual values.
pub(super) fn capture_cli_params(user_params: HashMap<String, String>) -> String {
    let mut params = HashMap::new();

    for (key, value) in user_params {
        if is_sensitive(&key) {
            // Sensitive params show as asterisks
            params.insert(key, REDACTED_STR.to_string());
        } else {
            // Non-sensitive params show actual value
            params.insert(key, value);
        }
    }

    serde_json::to_string(&params).unwrap_or_else(|_| "{}".to_string())
}

/// Check if a parameter name contains sensitive information
fn is_sensitive(param: &str) -> bool {
    // First check against our known sensitive parameters list
    if SENSITIVE_PARAMS.contains(&param) {
        return true;
    }

    // Additional substring matches for parameter patterns (safety net)
    const SENSITIVE_PATTERNS: &[&str] =
        &["password", "secret", "token", "credential", "auth", "key"];

    let param_lower = param.to_lowercase();

    // Check substring matches for extra safety
    for pattern in SENSITIVE_PATTERNS {
        if param_lower.contains(pattern) {
            return true;
        }
    }

    false
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory;
    use hashbrown::HashSet;

    use crate::commands::serve::Config;

    use super::*;

    #[test]
    fn test_sensitive_params_are_redacted() {
        let mut params = HashMap::new();
        for sensitive in SENSITIVE_PARAMS {
            params.insert(sensitive.to_string(), "un-redacted".to_string());
        }
        let result = capture_cli_params(params);
        let parsed = serde_json::from_str::<HashMap<String, String>>(&result).unwrap();
        assert_eq!(
            parsed.len(),
            SENSITIVE_PARAMS.len(),
            "expected there to be {n} parsed entries",
            n = SENSITIVE_PARAMS.len()
        );
        for sensitive in SENSITIVE_PARAMS {
            assert_eq!(
                parsed.get(*sensitive).unwrap(),
                REDACTED_STR,
                "expected {REDACTED_STR} for '{sensitive}' argument"
            );
        }
    }

    /// Extract all argument IDs from a Command recursively
    fn extract_all_arg_ids(cmd: &clap::Command, args: &mut HashSet<String>) {
        for arg in cmd.get_arguments() {
            let id = arg.get_id().as_str();

            // Skip help and version which are always present
            if id == "help" || id == "version" || id == "help-all" {
                continue;
            }

            // Get the display name (long form or short form or id)
            let display_name = if let Some(long) = arg.get_long() {
                long.to_string()
            } else if let Some(short) = arg.get_short() {
                format!("{}", short)
            } else {
                id.to_string()
            };

            args.insert(display_name);
        }

        // Recursively process subcommands
        for subcmd in cmd.get_subcommands() {
            if subcmd.get_name() != "help" {
                extract_all_arg_ids(subcmd, args);
            }
        }
    }

    #[test]
    fn test_all_config_params_categorized() {
        // Use the module-level constants - no need to redefine them here
        // Get all arguments from the Config command
        let cmd = Config::command();
        let mut discovered_args = HashSet::new();
        extract_all_arg_ids(
            cmd.get_subcommands()
                .find(|c| c.get_name() == "serve")
                .unwrap_or(&cmd),
            &mut discovered_args,
        );

        // If there are no serve subcommand, check the root
        if discovered_args.is_empty() {
            extract_all_arg_ids(&cmd, &mut discovered_args);
        }

        let mut uncategorized = Vec::new();

        for arg in &discovered_args {
            let is_in_non_sensitive_list = NON_SENSITIVE_PARAMS.contains(&arg.as_str());
            let is_in_sensitive_list = SENSITIVE_PARAMS.contains(&arg.as_str());

            if !is_in_non_sensitive_list && !is_in_sensitive_list {
                // Check if it might be caught by substring matching in is_sensitive function
                if !is_sensitive(arg) {
                    uncategorized.push(arg.clone());
                }
            }
        }

        if !uncategorized.is_empty() {
            panic!(
                "The following CLI parameters are not categorized as either sensitive or \
                non-sensitive:\n{}\n\n\
                Please add them to either NON_SENSITIVE_PARAMS or SENSITIVE_PARAMS constants \
                at the module level.",
                uncategorized.join("\n")
            );
        }

        let mut needlessly_categorized = Vec::new();

        for arg in NON_SENSITIVE_PARAMS.iter().chain(SENSITIVE_PARAMS) {
            let is_discovered = discovered_args.contains(*arg);
            if !is_discovered {
                needlessly_categorized.push(arg.to_owned());
            }
        }

        if !needlessly_categorized.is_empty() {
            panic!(
                "The following CLI parameters were set as either sensitive or non-sensitive \
                but were not discovered in the actual command:\n{}\n\n\
                Please remove them from the NON_SENSITIVE_PARAMS or SENSITIVE_PARAMS constants.",
                needlessly_categorized.join("\n")
            );
        }
    }
}
