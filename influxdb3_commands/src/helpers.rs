// common fruitilities for cli

use std::{str::FromStr, sync::OnceLock};

use influxdb3_server::all_paths;
use observability_deps::tracing::trace;

const DISABLED_AUTHZ_TOO_MANY_VALUES_ERR: &str = "--disable-authz cannot take more than 6 items";
const DISABLED_AUTHZ_INVALID_VALUE_ERR: &str = "invalid value passed in for --disable-authz, allowed values are health, ping, metrics, ready, pprof, and node_stop";

// The enterprise node-stop endpoint. The canonical constant lives in the enterprise server crate
// (`all_enterprise_paths::API_V3_ENTERPRISE_CONFIGURE_NODE_STOP`), which this OSS-shared crate
// cannot depend on, so the path is mirrored here as a literal. The enterprise auth integration
// test exercises this endpoint end to end and will fail loudly if the two ever drift.
const API_V3_ENTERPRISE_CONFIGURE_NODE_STOP: &str = "/api/v3/enterprise/configure/node/stop";

/// The resource names `--disable-authz` accepts. `health`, `metrics`, `ping` and
/// `ready` are also system resources in the permission model; `pprof` and
/// `node_stop` are not, and are only ever routes.
const DISABLED_AUTHZ_RESOURCES: [&str; 6] =
    ["health", "ping", "metrics", "ready", "pprof", "node_stop"];

static AUTHZ_DISABLED_RESOURCES: OnceLock<Vec<&'static str>> = OnceLock::new();

/// The resource names given to `--disable-authz`, as opposed to the routes they
/// map to. Callers that reason about resources rather than paths use this, so
/// the resource-to-route mapping stays in this file alone.
static AUTHZ_DISABLED_RESOURCE_NAMES: OnceLock<Vec<&'static str>> = OnceLock::new();

// This custom type is used to parse `--disable-authz health,ping,metrics`, it wasn't straight
// forward to setup num_args and also collect the values into a list with a value_delimiter set.
// Even if it's possible with a bit of clap-fu, still requires mapping those resource names to
// endpoints. This custom type pulls both parsing and mapping logic together, easier to setup
// and test
#[derive(Debug, Clone, Copy)]
pub struct DisableAuthzList;

impl Default for DisableAuthzList {
    fn default() -> Self {
        AUTHZ_DISABLED_RESOURCES.get_or_init(Vec::new);
        AUTHZ_DISABLED_RESOURCE_NAMES.get_or_init(Vec::new);
        Self {}
    }
}

impl DisableAuthzList {
    pub fn get_mapped_endpoints(&self) -> &'static [&'static str] {
        let all_paths_without_authz = AUTHZ_DISABLED_RESOURCES
            .get()
            .expect("disabled resource paths to have been loaded");
        trace!(paths_without_authz = ?all_paths_without_authz, "paths setup without authz");
        all_paths_without_authz
    }

    pub fn get_exempt_resources(&self) -> &'static [&'static str] {
        AUTHZ_DISABLED_RESOURCE_NAMES
            .get()
            .expect("disabled resource names to have been loaded")
    }
}

impl FromStr for DisableAuthzList {
    type Err = &'static str;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let requested: Vec<&str> = s.split(',').map(str::trim).collect();
        if requested.len() > DISABLED_AUTHZ_RESOURCES.len() {
            return Err(DISABLED_AUTHZ_TOO_MANY_VALUES_ERR);
        }

        let resources: Vec<&'static str> = requested
            .iter()
            .map(|r| {
                DISABLED_AUTHZ_RESOURCES
                    .iter()
                    .find(|allowed| *allowed == r)
                    .copied()
                    .ok_or(DISABLED_AUTHZ_INVALID_VALUE_ERR)
            })
            .collect::<std::result::Result<_, _>>()?;

        let resources_static = resources
            .iter()
            .flat_map(|path| match *path {
                "health" => [all_paths::API_V3_HEALTH, all_paths::API_V1_HEALTH].as_slice(),
                "ping" => &[all_paths::API_PING],
                "metrics" => &[all_paths::API_METRICS],
                "ready" => &[all_paths::API_V3_READY],
                "pprof" => &[all_paths::API_DEBUG_PPROF_HEAP],
                "node_stop" => &[API_V3_ENTERPRISE_CONFIGURE_NODE_STOP],
                _ => &[],
            })
            .copied()
            .collect();

        AUTHZ_DISABLED_RESOURCES.get_or_init(|| resources_static);
        AUTHZ_DISABLED_RESOURCE_NAMES.get_or_init(|| resources);

        Ok(Self)
    }
}

#[cfg(test)]
mod tests;
