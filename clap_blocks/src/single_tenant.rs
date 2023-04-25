//! CLI config for request authorization.

/// Env var providing authz address
pub const CONFIG_AUTHZ_ENV_NAME: &str = "INFLUXDB_IOX_AUTHZ_ADDR";
/// CLI flag for authz address
pub const CONFIG_AUTHZ_FLAG: &str = "authz-addr";

/// Env var for single tenancy deployments
pub const CONFIG_CST_ENV_NAME: &str = "INFLUXDB_IOX_SINGLE_TENANCY";
/// CLI flag for single tenancy deployments
pub const CONFIG_CST_FLAG: &str = "single-tenancy";
