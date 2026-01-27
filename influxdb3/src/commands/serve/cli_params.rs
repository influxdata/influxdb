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
    "node-id-from-env",
    "cluster-id",
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
    "checkpoint-interval",
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
    "object-store-request-timeout",
    "object-store-retry-timeout",
    "object-store-tls-allow-insecure",
    "object-store-tls-ca",
    // Feature flags and modes
    "disable-authz",
    // Telemetry
    "telemetry-endpoint",
    "disable-telemetry-upload",
    "serve-invocation-method",
    // TLS parameters
    "tls-minimum-version",
    // Python integration
    "plugin-dir",
    "virtual-env-location",
    "package-manager",
    "plugin-repo",
    // Other parameters
    "tcp-listener-file-path",
    // Tokio console parameters
    "tokio-console-enabled",
    "tokio-console-event-buffer-capacity",
    "tokio-console-client-buffer-capacity",
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

    /// Extract all public (non-hidden) argument long names from a Command recursively.
    /// Returns a HashSet of option names (without the leading "--").
    fn extract_public_cli_options(cmd: &clap::Command, args: &mut HashSet<String>) {
        for arg in cmd.get_arguments() {
            // Skip help and version which are always present
            let id = arg.get_id().as_str();
            if id == "help" || id == "version" || id == "help-all" {
                continue;
            }

            // Skip hidden arguments - these are internal/test-only and shouldn't
            // be in the config file
            if arg.is_hide_set() {
                continue;
            }

            // Only include arguments that have a long form (these are the ones
            // that can be configured via TOML config)
            if let Some(long) = arg.get_long() {
                args.insert(long.to_string());
            }
        }

        // Recursively process subcommands
        for subcmd in cmd.get_subcommands() {
            if subcmd.get_name() != "help" {
                extract_public_cli_options(subcmd, args);
            }
        }
    }

    /// Parse the core config file and extract all documented option names.
    /// Options are in the format: #option-name="value" or #option-name=value
    fn extract_config_file_options(config_content: &str) -> HashSet<String> {
        let mut options = HashSet::new();

        for line in config_content.lines() {
            let line = line.trim();

            // Skip empty lines and comment-only lines (lines starting with # followed
            // by space or that don't contain '=')
            if line.is_empty() || !line.contains('=') {
                continue;
            }

            // Match lines like: #option-name="value" or option-name="value"
            // The '#' prefix indicates a commented-out (default) option
            let line = line.strip_prefix('#').unwrap_or(line);

            // Extract the option name (everything before the '=')
            if let Some(eq_pos) = line.find('=') {
                let option_name = line[..eq_pos].trim();

                // Only include valid option names (kebab-case identifiers)
                if !option_name.is_empty()
                    && option_name
                        .chars()
                        .all(|c| c.is_ascii_alphanumeric() || c == '-')
                {
                    options.insert(option_name.to_string());
                }
            }
        }

        options
    }

    /// Options (long form) that are intentionally excluded from the config file.
    /// These are either:
    /// - Deprecated and will be removed
    /// - Have special handling that doesn't fit the config file model
    /// - Are handled by the launcher itself (not passed to influxdb3)
    /// - CLI-only debugging/interactive flags
    const CONFIG_EXCLUDED_OPTIONS: &[&str] = &[
        // Verbose flag - typically used only on command line for debugging
        "verbose",
        // Enterprise-only option that errors when used with Core OSS
        "cluster-id",
    ];

    /// Options that exist in the config file but not in CLI.
    /// These are typically options that are:
    /// - Renamed in CLI (old name kept in config for backwards compatibility)
    /// - Translated by the launcher to a different CLI option
    /// - Other runtime options not part of serve subcommand's Config
    const CLI_EXCLUDED_OPTIONS: &[&str] = &[
        // Renamed to num-datafusion-threads in CLI
        "datafusion-num-threads",
        // Top-level IO runtime options - these are valid CLI options but
        // defined using the tokio_rt_config! macro outside of serve::Config.
        // The test only introspects serve::Config, so these appear as
        // "missing" from CLI.
        "io-runtime-type",
        "io-runtime-disable-lifo-slot",
        "io-runtime-event-interval",
        "io-runtime-global-queue-interval",
        "io-runtime-max-blocking-threads",
        "io-runtime-max-io-events-per-tick",
        "io-runtime-thread-keep-alive",
        "io-runtime-thread-priority",
        "num-io-threads",
    ];

    #[test]
    fn test_config_file_cli_option_drift() {
        use crate::commands::serve::Config;

        // Path to the core config file (relative to workspace root)
        let config_path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../.circleci/packages/influxdb3/fs/usr/share/influxdb3/influxdb3-core.conf"
        );

        // Read the config file
        let config_content = std::fs::read_to_string(config_path).unwrap_or_else(|e| {
            panic!(
                "Failed to read config file at {}: {}\n\
                 This test verifies that CLI options match the config file template.",
                config_path, e
            )
        });

        // Extract options from CLI (using clap introspection)
        let cmd = Config::command();
        let mut cli_options = HashSet::new();
        extract_public_cli_options(&cmd, &mut cli_options);

        // Extract options from config file
        let config_options = extract_config_file_options(&config_content);

        // Find options in CLI but not in config file
        let mut missing_from_config: Vec<_> = cli_options
            .iter()
            .filter(|opt| {
                !config_options.contains(*opt) && !CONFIG_EXCLUDED_OPTIONS.contains(&opt.as_str())
            })
            .cloned()
            .collect();
        missing_from_config.sort();

        // Find options in config file but not in CLI
        let mut missing_from_cli: Vec<_> = config_options
            .iter()
            .filter(|opt| {
                !cli_options.contains(*opt) && !CLI_EXCLUDED_OPTIONS.contains(&opt.as_str())
            })
            .cloned()
            .collect();
        missing_from_cli.sort();

        // Build error message if there's drift
        let mut error_msg = String::new();

        if !missing_from_config.is_empty() {
            error_msg.push_str(&format!(
                "\n\nCLI options missing from config file ({}):\n",
                config_path
            ));
            for opt in &missing_from_config {
                error_msg.push_str(&format!("  - {}\n", opt));
            }
            error_msg.push_str("\nTo fix: Add these options to the config file template, or add them to\nCONFIG_EXCLUDED_OPTIONS if they should be excluded. Until influxdb3 supports\nTOML configuration natively, when adding to the config file template, you\nmust also consider that the launcher maps options to the corresponding\nenvironment variable for the option by using this pattern:\n`INFLUXDB3_ + key.replace('-', '_').upper()`. If the new option doesn't\nfollow this pattern (ie, all 'INFLUXDB3_ENTERPRISE_...' env vars), update\nTOML_KEY_ENVVAR in `influxdb3-launcher` to include your new mapping.");
        }

        if !missing_from_cli.is_empty() {
            error_msg.push_str(&format!(
                "\n\nConfig file options not found in CLI ({}):\n",
                config_path
            ));
            for opt in &missing_from_cli {
                error_msg.push_str(&format!("  - {}\n", opt));
            }
            error_msg.push_str("\nTo fix: Remove these options from the config file, or add them to\nCLI_EXCLUDED_OPTIONS if they're handled specially.");
        }

        if !error_msg.is_empty() {
            panic!("Config file and CLI options are out of sync!{}", error_msg);
        }
    }
}
