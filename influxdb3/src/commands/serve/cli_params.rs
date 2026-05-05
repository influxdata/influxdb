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
    "max-concurrent-queries",
    "force-snapshot-mem-threshold",
    // WAL parameters
    "wal-flush-interval",
    "wal-snapshot-size",
    "wal-max-write-buffer-size",
    "wal-replay-fail-on-error",
    "wal-replay-concurrency-limit",
    "snapshot-concurrency-limit",
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
    "disable-log-filter-noise-reduction",
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
    "restrict-plugin-triggers-to",
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
mod tests;
