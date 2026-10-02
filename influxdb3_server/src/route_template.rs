//! Maps raw request paths to a bounded set of route-template strings for use
//! as `/metrics` label values. Every path that is not a known route maps to
//! `"other"`, so the set of `path` label values is bounded by construction
//! and never contains client-chosen strings (gh#4487).

use crate::all_paths;

/// Label value for any path that does not match a known route.
pub(crate) const OTHER: &str = "other";

/// Map a raw request path to its route template.
///
/// Routes with client-chosen or variable trailing segments map to a static
/// `:param`-style template. Keep this in sync with `perform_routing` in
/// `http.rs`; an unmapped new route is not a correctness bug -- it just
/// shows up as `"other"` until added here.
pub(crate) fn route_template(path: &str) -> &'static str {
    match path {
        all_paths::API_LEGACY_WRITE => return "/write",
        all_paths::API_V2_WRITE => return "/api/v2/write",
        all_paths::API_V3_WRITE => return "/api/v3/write_lp",
        all_paths::API_V3_QUERY_SQL => return "/api/v3/query_sql",
        all_paths::API_V3_QUERY_INFLUXQL => return "/api/v3/query_influxql",
        all_paths::API_V1_QUERY => return "/query",
        all_paths::API_V3_HEALTH => return "/health",
        all_paths::API_V1_HEALTH => return "/api/v1/health",
        all_paths::API_V3_READY => return "/ready",
        all_paths::API_METRICS => return "/metrics",
        all_paths::API_PING => return "/ping",
        all_paths::API_DEBUG_PPROF_HEAP => return "/debug/pprof/heap",
        all_paths::API_V3_CONFIGURE_DISTINCT_CACHE => return "/api/v3/configure/distinct_cache",
        all_paths::API_V3_CONFIGURE_LAST_CACHE => return "/api/v3/configure/last_cache",
        all_paths::API_V3_CONFIGURE_PROCESSING_ENGINE_DISABLE => {
            return "/api/v3/configure/processing_engine_trigger/disable";
        }
        all_paths::API_V3_CONFIGURE_PROCESSING_ENGINE_ENABLE => {
            return "/api/v3/configure/processing_engine_trigger/enable";
        }
        all_paths::API_V3_CONFIGURE_PROCESSING_ENGINE_TRIGGER => {
            return "/api/v3/configure/processing_engine_trigger";
        }
        all_paths::API_V3_CONFIGURE_PLUGIN_INSTALL_PACKAGES => {
            return "/api/v3/configure/plugin_environment/install_packages";
        }
        all_paths::API_V3_CONFIGURE_PLUGIN_INSTALL_REQUIREMENTS => {
            return "/api/v3/configure/plugin_environment/install_requirements";
        }
        all_paths::API_V3_CONFIGURE_DATABASE => return "/api/v3/configure/database",
        all_paths::API_V3_CONFIGURE_TABLE => return "/api/v3/configure/table",
        all_paths::API_V3_CONFIGURE_DATABASE_RETENTION_PERIOD => {
            return "/api/v3/configure/database/retention_period";
        }
        all_paths::API_V3_CONFIGURE_TOKEN => return "/api/v3/configure/token",
        all_paths::API_V3_CONFIGURE_ADMIN_TOKEN => return "/api/v3/configure/token/admin",
        all_paths::API_V3_CONFIGURE_ADMIN_TOKEN_REGENERATE => {
            return "/api/v3/configure/token/admin/regenerate";
        }
        all_paths::API_V3_CONFIGURE_NAMED_ADMIN_TOKEN => {
            return "/api/v3/configure/token/named_admin";
        }
        all_paths::API_V3_TEST_WAL_ROUTE => return "/api/v3/plugin_test/wal",
        all_paths::API_V3_TEST_PLUGIN_ROUTE => return "/api/v3/plugin_test/schedule",
        all_paths::API_V3_TEST_TELEMETRY_SNAPSHOT => return "/api/v3/test/:action",
        all_paths::API_V3_PLUGINS_FILES => return "/api/v3/plugins/files",
        all_paths::API_V3_PLUGINS_DIRECTORY => return "/api/v3/plugins/directory",
        _ => {}
    }

    if path.starts_with(all_paths::API_V3_ENGINE) {
        return "/api/v3/engine/:path";
    }

    OTHER
}

#[cfg(test)]
mod tests {
    use super::*;

    const KNOWN_ROUTES: [&str; 8] = [
        "/write",
        "/api/v2/write",
        "/api/v3/write_lp",
        "/api/v3/query_sql",
        "/health",
        "/metrics",
        "/ping",
        "/api/v3/plugins/files",
    ];

    const PARAMETERIZED_PATHS: [&str; 2] = [
        "/api/v3/engine/my_secret_trigger_path",
        "/api/v3/test/telemetry_snapshot",
    ];

    #[test]
    fn known_routes_map_to_themselves() {
        for route in KNOWN_ROUTES {
            assert_eq!(route_template(route), route);
        }
    }

    #[test]
    fn parameterized_routes_map_to_templates() {
        assert_eq!(
            route_template("/api/v3/engine/my_secret_trigger_path"),
            "/api/v3/engine/:path"
        );
        assert_eq!(
            route_template("/api/v3/test/telemetry_snapshot"),
            "/api/v3/test/:action"
        );
    }

    #[test]
    fn unknown_paths_map_to_other() {
        // Dashboards key on this exact literal; changing it is a breaking
        // change to the metric contract.
        assert_eq!(OTHER, "other");

        for path in [
            "/",
            "/wp-admin/setup.php",
            "/api/v3/doesnotexist",
            "/api/v3/test/doesnotexist",
            "/../../etc/passwd",
            "/api/v3/engine",
        ] {
            assert_eq!(route_template(path), OTHER, "path: {path}");
        }
    }

    #[test]
    fn templates_start_with_slash() {
        for path in KNOWN_ROUTES.iter().chain(PARAMETERIZED_PATHS.iter()) {
            let template = route_template(path);
            assert_ne!(template, OTHER, "path: {path}");
            assert!(template.starts_with('/'), "template: {template}");
        }
    }
}
