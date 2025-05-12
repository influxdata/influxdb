pub(crate) const API_LEGACY_WRITE: &str = "/write";
pub(crate) const API_V2_WRITE: &str = "/api/v2/write";
pub(crate) const API_V3_WRITE: &str = "/api/v3/write_lp";
pub(crate) const API_V3_QUERY_SQL: &str = "/api/v3/query_sql";
pub(crate) const API_V3_QUERY_INFLUXQL: &str = "/api/v3/query_influxql";
pub(crate) const API_V1_QUERY: &str = "/query";
pub const API_V3_HEALTH: &str = "/health";
pub const API_V1_HEALTH: &str = "/api/v1/health";
pub(crate) const API_V3_ENGINE: &str = "/api/v3/engine/";
pub(crate) const API_V3_CONFIGURE_DISTINCT_CACHE: &str = "/api/v3/configure/distinct_cache";
pub(crate) const API_V3_CONFIGURE_LAST_CACHE: &str = "/api/v3/configure/last_cache";
pub(crate) const API_V3_CONFIGURE_PROCESSING_ENGINE_DISABLE: &str =
    "/api/v3/configure/processing_engine_trigger/disable";
pub(crate) const API_V3_CONFIGURE_PROCESSING_ENGINE_ENABLE: &str =
    "/api/v3/configure/processing_engine_trigger/enable";
pub(crate) const API_V3_CONFIGURE_PROCESSING_ENGINE_TRIGGER: &str =
    "/api/v3/configure/processing_engine_trigger";
pub(crate) const API_V3_CONFIGURE_PLUGIN_INSTALL_PACKAGES: &str =
    "/api/v3/configure/plugin_environment/install_packages";
pub(crate) const API_V3_CONFIGURE_PLUGIN_INSTALL_REQUIREMENTS: &str =
    "/api/v3/configure/plugin_environment/install_requirements";
pub(crate) const API_V3_CONFIGURE_DATABASE: &str = "/api/v3/configure/database";
pub(crate) const API_V3_CONFIGURE_TABLE: &str = "/api/v3/configure/table";
pub const API_METRICS: &str = "/metrics";
pub const API_PING: &str = "/ping";
pub(crate) const API_V3_CONFIGURE_TOKEN: &str = "/api/v3/configure/token";
pub(crate) const API_V3_CONFIGURE_ADMIN_TOKEN: &str = "/api/v3/configure/token/admin";
pub(crate) const API_V3_CONFIGURE_ADMIN_TOKEN_REGENERATE: &str =
    "/api/v3/configure/token/admin/regenerate";
pub(crate) const API_V3_TEST_WAL_ROUTE: &str = "/api/v3/plugin_test/wal";
pub(crate) const API_V3_TEST_PLUGIN_ROUTE: &str = "/api/v3/plugin_test/schedule";
